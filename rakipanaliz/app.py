# app.py
import os, sys, subprocess, glob
from pathlib import Path
import pandas as pd
import streamlit as st
import altair as alt

st.set_page_config(page_title="Rakip Analizi – Sosyal Metrikler", layout="wide")
st.title("📊 Rakip Analizi – Sosyal Metrikler")

# ---------- Yol/konumlar ----------
ROOT = Path(__file__).resolve().parent
MASTER_CSV = ROOT / "social_stats_master.csv"
DAILY_REPORTS_PATTERN = str(ROOT / "social_stats_*.csv")
SCRAPER_SCRIPT = ROOT / "social_pw.py"

PLATFORM_ICONS = {
    "Facebook": "https://cdn.jsdelivr.net/gh/simple-icons/simple-icons/icons/facebook.svg",
    "Instagram": "https://cdn.jsdelivr.net/gh/simple-icons/simple-icons/icons/instagram.svg",
    "TikTok": "https://cdn.jsdelivr.net/gh/simple-icons/simple-icons/icons/tiktok.svg",
}

@st.cache_data
def load_csv(p: Path) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(p)
        if "Collected_At" in df.columns:
            df["Collected_At"] = pd.to_datetime(df["Collected_At"], errors="coerce")
        return df
    except Exception as e:
        st.error(f"CSV okunamadı: {p}\nHata: {e}")
        return None

def find_daily_reports() -> list[str]:
    return sorted(glob.glob(DAILY_REPORTS_PATTERN))

def combine_daily_to_master() -> bool:
    files = find_daily_reports()
    if not files:
        return False
    frames = [load_csv(Path(f)) for f in files]
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return False
    df = pd.concat(frames, ignore_index=True)
    df.drop_duplicates(subset=["Platform", "Account", "Collected_At"], keep="last", inplace=True)
    df.sort_values("Collected_At", inplace=True)
    df.to_csv(MASTER_CSV, index=False)
    return True

def run_scraper_and_refresh_master() -> bool:
    st.info("Yeni veri için toplayıcı çalıştırılıyor…")
    cmd = [sys.executable, str(SCRAPER_SCRIPT)]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    with st.expander("Komut çıktısı (log)"):
        st.code(proc.stdout or "(stdout boş)")
        st.code(proc.stderr or "(stderr boş)")
    if proc.returncode != 0:
        st.error(f"Toplayıcı başarısız (exit {proc.returncode}).")
        return False
    st.success("Toplayıcı tamam!")
    if MASTER_CSV.exists() or combine_daily_to_master():
        st.success("Master CSV güncellendi.")
        st.cache_data.clear()       # cache’i sıfırla
        return True
    st.warning("Master CSV bulunamadı/oluşturulamadı.")
    return False

# ---------- Veri yükle ----------
df = None
if MASTER_CSV.exists():
    df = load_csv(MASTER_CSV)
elif combine_daily_to_master():
    df = load_csv(MASTER_CSV)

# üst butonlar
cbtn1, cbtn2 = st.columns([1,1])
with cbtn1:
    if st.button("🔴 Veri Güncelle", type="primary"):
        if run_scraper_and_refresh_master():
            df = load_csv(MASTER_CSV)
with cbtn2:
    if st.button("↻ Yeniden Yükle"):
        st.cache_data.clear()
        df = load_csv(MASTER_CSV)

if df is None or df.empty:
    st.warning("Henüz veri yok. ‘Veri Güncelle’ ile başlatabilirsin.")
    st.stop()

st.caption(f"Kullanılan dosya: **{MASTER_CSV.name}**  |  Konum: `{MASTER_CSV.parent}`")

# ---------- Yardımcılar ----------
def platform_section_header(name: str):
    """Başlık + sağda büyük platform görseli."""
    colL, colR = st.columns([3,1], vertical_alignment="center")
    with colL:
        st.header(name)
    with colR:
        st.image(PLATFORM_ICONS.get(name, ""), width=110, caption=name)

def latest_two_snapshots(df_platform: pd.DataFrame) -> pd.DataFrame:
    """
    Her Account için en güncel iki satırı bırakır (yoksa tek satır),
    ardından pivotlamaya uygun hale getirir.
    """
    if df_platform.empty:
        return df_platform.copy()

    # en güncel iki kayıt
    df_sorted = df_platform.sort_values(["Account", "Collected_At"])
    last2 = (
        df_sorted
        .groupby("Account", as_index=False, group_keys=False)
        .apply(lambda g: g.tail(2))
    )
    return last2

def compute_deltas(df_platform: pd.DataFrame) -> pd.DataFrame:
    """
    Son iki güncelleme arasındaki farklar:
    Followers, Avg_Likes_Last5, Avg_Comments_Last5
    """
    last2 = latest_two_snapshots(df_platform)
    # her hesap için son (t=-1) ve önceki (t=-2) ayır
    def add_rank(g):
        g = g.sort_values("Collected_At")
        g["__rank"] = range(len(g))  # 0..n-1
        return g

    ranked = last2.groupby("Account", as_index=False, group_keys=False).apply(add_rank)
    # son kayıt index’ini bulalım
    idx_last = ranked.groupby("Account")["__rank"].transform("max") == ranked["__rank"]
    cur = ranked[idx_last].drop(columns="__rank").rename(
        columns={
            "Followers": "Followers_cur",
            "Avg_Likes_Last5": "Likes_cur",
            "Avg_Comments_Last5": "Comments_cur",
        }
    )
    prev = ranked[~idx_last].drop(columns="__rank").rename(
        columns={
            "Followers": "Followers_prev",
            "Avg_Likes_Last5": "Likes_prev",
            "Avg_Comments_Last5": "Comments_prev",
        }
    )
    # merge (aynı Account, en güncel kayıtla eşleşmeyen olabilir => left join)
    merged = pd.merge(
        cur[["Platform","Account","Collected_At","Followers_cur","Likes_cur","Comments_cur"]],
        prev[["Account","Followers_prev","Likes_prev","Comments_prev"]],
        on="Account", how="left"
    )
    # farklar
    merged["Delta_Followers"] = merged["Followers_cur"] - merged["Followers_prev"]
    merged["Delta_Likes"] = merged["Likes_cur"] - merged["Likes_prev"]
    merged["Delta_Comments"] = merged["Comments_cur"] - merged["Comments_prev"]
    return merged

def delta_bar_chart(df_delta: pd.DataFrame, field: str, title: str):
    base = alt.Chart(df_delta).encode(
        x=alt.X("Account:N", sort="-y", title="Hesap"),
        y=alt.Y(f"{field}:Q", title=title),
        tooltip=["Account", field, "Followers_cur", "Collected_At"],
    )
    # koşullu renk
    color = alt.condition(
        alt.datum[field] > 0, alt.value("#16a34a"),  # yeşil
        alt.value("#dc2626")                         # kırmızı
    )
    return base.mark_bar().encode(color=color)

# ---------- Navigasyon ----------
st.sidebar.title("Menü")
page = st.sidebar.radio("Sayfa", ["Ana Sayfa", "Facebook", "Instagram", "TikTok"])

# ---------- ANA SAYFA ----------
if page == "Ana Sayfa":
    st.subheader("Özet")
    c1, c2, c3 = st.columns(3)
    c1.metric("Toplam Kayıt", f"{len(df):,}")
    c2.metric("Platform Sayısı", f"{df['Platform'].nunique():,}")
    c3.metric("Hesap Sayısı", f"{df['Account'].nunique():,}")

    st.markdown(
        "Sol menüden bir platformu seçerek detaylara inebilir, **Veri Güncelle** ile en güncel verileri çekebilirsin."
    )

# ---------- FACEBOOK ----------
elif page == "Facebook":
    platform_section_header("Facebook")
    pf = df[df["Platform"] == "Facebook"].copy()
    accounts = sorted(pf["Account"].dropna().unique())
    sel = st.multiselect("Hesaplar", accounts, default=accounts)
    pf = pf[pf["Account"].isin(sel)]

    k1, k2 = st.columns(2)
    k1.metric("Ortalama Takipçi", f"{pf['Followers'].dropna().mean():,.0f}" if not pf.empty else "-")
    # Facebook’ta like/yorum yok: ikinci KPI’ı bilgilendirici bırakalım
    k2.metric("Metrik", "Takipçi odaklı analiz")

    st.divider()
    st.subheader("Son Güncellemeye Göre Değişim")
    deltas = compute_deltas(pf)
    if deltas.empty:
        st.info("Fark grafiği için en az iki güncelleme gerekli.")
    else:
        chart = delta_bar_chart(deltas, "Delta_Followers", "Takipçi Farkı (son güncelleme)")
        st.altair_chart(chart, use_container_width=True)
        st.dataframe(
            deltas[["Account","Followers_cur","Delta_Followers","Collected_At"]]
            .sort_values("Delta_Followers", ascending=False),
            use_container_width=True
        )

    st.subheader("Kayıtlar")
    st.dataframe(
        pf[["Collected_At","Account","Followers","Handle_or_URL"]]
        .sort_values("Collected_At", ascending=False),
        use_container_width=True
    )

# ---------- INSTAGRAM ----------
elif page == "Instagram":
    platform_section_header("Instagram")
    pf = df[df["Platform"] == "Instagram"].copy()
    accounts = sorted(pf["Account"].dropna().unique())
    sel = st.multiselect("Hesaplar", accounts, default=accounts)
    pf = pf[pf["Account"].isin(sel)]

    k1, k2, k3 = st.columns(3)
    k1.metric("Ortalama Takipçi", f"{pf['Followers'].dropna().mean():,.0f}" if not pf.empty else "-")
    k2.metric("Ort. Beğeni (Son 5)", f"{pf['Avg_Likes_Last5'].dropna().mean():,.0f}" if not pf.empty else "-")
    k3.metric("Ort. Yorum (Son 5)", f"{pf['Avg_Comments_Last5'].dropna().mean():,.0f}" if not pf.empty else "-")

    st.divider()
    st.subheader("Son Güncellemeye Göre Değişim")
    deltas = compute_deltas(pf)
    if deltas.empty:
        st.info("Fark grafiği için en az iki güncelleme gerekli.")
    else:
        cA, cB = st.columns(2)
        with cA:
            st.caption("Takipçi Farkı")
            st.altair_chart(delta_bar_chart(deltas, "Delta_Followers", "Takipçi Farkı"), use_container_width=True)
        with cB:
            st.caption("Beğeni (Son 5) Farkı")
            st.altair_chart(delta_bar_chart(deltas, "Delta_Likes", "Beğeni Farkı"), use_container_width=True)

        st.dataframe(
            deltas[["Account","Followers_cur","Delta_Followers","Delta_Likes","Delta_Comments","Collected_At"]]
            .sort_values("Delta_Followers", ascending=False),
            use_container_width=True
        )

    st.subheader("Kayıtlar")
    st.dataframe(
        pf[["Collected_At","Account","Followers","Avg_Likes_Last5","Avg_Comments_Last5","Posts_Total","Handle_or_URL"]]
        .sort_values("Collected_At", ascending=False),
        use_container_width=True
    )

# ---------- TIKTOK ----------
elif page == "TikTok":
    platform_section_header("TikTok")
    pf = df[df["Platform"] == "TikTok"].copy()
    accounts = sorted(pf["Account"].dropna().unique())
    sel = st.multiselect("Hesaplar", accounts, default=accounts)
    pf = pf[pf["Account"].isin(sel)]

    k1, k2 = st.columns(2)
    k1.metric("Ortalama Takipçi", f"{pf['Followers'].dropna().mean():,.0f}" if not pf.empty else "-")
    # Likes_total bilgisi Extra’da geliyorsa göster
    if "Extra" in pf.columns:
        try:
            likes_vals = (
                pf["Extra"].fillna("")
                .str.extract(r"likes_total=(\d+)")
                .astype("float")
                .dropna()[0]
            )
            k2.metric("Ort. Toplam Beğeni (Extra)", f"{likes_vals.mean():,.0f}" if not likes_vals.empty else "-")
        except Exception:
            k2.metric("Metrik", "Takipçi odaklı analiz")
    else:
        k2.metric("Metrik", "Takipçi odaklı analiz")

    st.divider()
    st.subheader("Son Güncellemeye Göre Değişim")
    deltas = compute_deltas(pf)
    if deltas.empty:
        st.info("Fark grafiği için en az iki güncelleme gerekli.")
    else:
        st.altair_chart(delta_bar_chart(deltas, "Delta_Followers", "Takipçi Farkı (son güncelleme)"),
                        use_container_width=True)
        st.dataframe(
            deltas[["Account","Followers_cur","Delta_Followers","Collected_At"]]
            .sort_values("Delta_Followers", ascending=False),
            use_container_width=True
        )

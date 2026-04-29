import asyncio, re, json, csv
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, urlparse
from playwright.async_api import async_playwright

# ================== Settings ==================
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

FB_STATE = Path("fb_state.json")
IG_STATE = Path("ig_state.json")
FB_MAP   = Path("fb_pages.json")    # label -> full URL (mbasic or www)
IG_MAP   = Path("ig_handles.json")  # label -> handle

# ---- IG defaults ----
DEFAULT_IG_MAP = {
  "BBC_archive": "bbc_archive",
  "DiasporaTürk": "diasporaturk",
  "Tarih Dergi": "tarihdergi",
  "Derin Tarih": "derintarih",
  "Atlas Tarih Dergisi": "atlastarihdergisi",
  "Arsivden.sahne": "arsivden.sahne",
  "32 Gün Arşivi": "32gunarsivi",
  "Pathe Films": "pathefilms",
  "BBC Türkçe": "bbcturkce",
  "Diasporaturk": "diasporaturk",
  "BBC_archive (old)": "BBC_archive",
  "Pathefilms": "pathefilms",
  "Atlastarihdergisi": "atlastarihdergisi",
  "Arsiv.unutmaz": "arsiv.unutmaz"
}
instagram_inputs = [
    "BBC Türkçe","BBC_archive","DiasporaTürk","Tarih Dergi","Derin Tarih",
    "Atlas Tarih Dergisi","Arsivden.sahne","32 Gün Arşivi","Pathe Films","Arsiv.unutmaz"
]

# ---- FB defaults (mbasic) ----
DEFAULT_FB_MAP = {
  "BBC Archive": "https://mbasic.facebook.com/BBCArchive/",
  "BBC Türkçe": "https://mbasic.facebook.com/bbcnewsturkceservisi/",
  "32.Gün Arşivi": "https://mbasic.facebook.com/32gunarsivi/",
  "Tarih Arşivi": "https://mbasic.facebook.com/tariharsivi/",
  "Derin Tarih": "https://mbasic.facebook.com/DerinTarih/",
  "Atlas Tarih": "https://mbasic.facebook.com/atlastarihdergi/",
  "Nostalji Kahvesi": "https://mbasic.facebook.com/pages/Nostalji-Kahvesi/36954382601"
}
facebook_queries = list(DEFAULT_FB_MAP.keys())

# TikTok users
tt_users = [
    "rotasizseyyah","natgeo","dmaxtr","animalplanet","natgeowild",
    "bbcearth","discovery","tac_turkiye",
]

# ================== Utils ==================
def load_json(p: Path):
    if p.exists():
        try: return json.loads(p.read_text(encoding="utf-8"))
        except: return {}
    return {}

def save_json(p: Path, data: dict):
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

async def pause(msg="Devam etmek için ENTER'a bas..."):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: input(msg))

async def visit(page, url: str, wait_ms: int = 1000):
    try:
        await page.goto(url, wait_until="load", timeout=90000)
        await page.wait_for_timeout(wait_ms)
    except Exception as e:
        print("[visit] Hata:", url, e)

def first_profile_segment(ig_url: str) -> str | None:
    try:
        u = urlparse(ig_url)
        segs = [s for s in u.path.split("/") if s]
        if not segs: return None
        if segs[0] in {"reels","p","tags","explore","stories"}: return None
        return segs[0]
    except: return None

def parse_compact_number(text: str) -> int | None:
    if not text: return None
    t = text.strip().replace("\xa0"," ").lower()
    for word,mult in [("milyar",1_000_000_000),("mn",1_000_000),("milyon",1_000_000),("bin",1_000)]:
        if word in t:
            nums = re.findall(r"\d[\d.,]*", t)
            if not nums: return None
            val = float(nums[-1].replace(".", "").replace(",", "."))
            return int(round(val*mult))
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(mn|b)\b", t, re.IGNORECASE)
    if m:
        val = float(m.group(1).replace(",", "."))
        return int(round(val*(1_000_000 if m.group(2).lower()=="mn" else 1_000)))
    m = re.search(r"(\d+(?:[.,]\d+)?)(\s*[KMB])\b", text, re.IGNORECASE)
    if m:
        val = float(m.group(1).replace(",", "."))
        mult = {"K":1_000,"M":1_000_000,"B":1_000_000_000}[m.group(2).strip().upper()]
        return int(round(val*mult))
    nums = re.findall(r"\d[\d.,]*", t)
    if nums:
        raw = nums[-1]
        if raw.count(",")==1 and raw.count(".")==0: raw = raw.replace(",", ".")
        if raw.count(".")>=1 and raw.count(",")==0 and len(raw.split(".")[-1])==3: raw = raw.replace(".", "")
        try: return int(float(raw.replace(",", ".")))
        except: return None
    return None

# ================== Instagram ==================
async def ensure_instagram_login(pw, headless=False):
    if IG_STATE.exists():
        return await pw.chromium.launch(headless=headless)
    browser = await pw.chromium.launch(headless=headless)
    context = await browser.new_context(user_agent=UA, locale="en-US", viewport={"width":1280,"height":800})
    page = await context.new_page()
    print("\n[IG] Instagram login page is opening...")
    await visit(page, "https://www.instagram.com/accounts/login/", 1200)
    print("[IG] Please LOGIN in the browser window. When you see your feed/profile, press ENTER here.")
    await pause()
    try:
        await context.storage_state(path=str(IG_STATE))
        print("[IG] Saved login state -> ig_state.json\n")
    except Exception as e:
        print("[IG] Could not save storage_state:", e)
    await browser.close()
    return await pw.chromium.launch(headless=headless)

async def ig_resolve_handle(page, label: str, ig_map: dict):
    print(f"[IG] Handle missing for ‘{label}’. Open the PROFILE page (instagram.com/<handle>/) then press ENTER.")
    await visit(page, "https://www.instagram.com/", 1400)
    await pause()
    handle = first_profile_segment(page.url)
    if handle:
        ig_map[label] = handle; save_json(IG_MAP, ig_map)
        print(f"   [IG] Resolved: {label} -> {handle}")
        return handle
    print("   [IG] Could not resolve. Skipped."); return None

async def _ig_api_profile(context, handle: str):
    url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={handle}"
    headers = {
        "User-Agent": UA, "X-IG-App-ID": "936619743392459",
        "Accept": "application/json", "Referer": f"https://www.instagram.com/{handle}/"
    }
    resp = await context.request.get(url, headers=headers, timeout=30000)
    if not resp.ok: return None
    return await resp.json()

async def _ig_api_last_posts(context, user_id: str, count: int = 5):
    url = f"https://www.instagram.com/api/v1/feed/user/{user_id}/?count={count}"
    headers = {"User-Agent": UA, "X-IG-App-ID": "936619743392459", "Accept": "application/json"}
    resp = await context.request.get(url, headers=headers, timeout=30000)
    if not resp.ok: return None
    return await resp.json()

async def ig_fetch_stats(context, page, handle: str):
    out = {"followers": None, "posts_total": None, "recent_posts": [], "avg_likes": None, "avg_comments": None}

    user_json = None
    try:
        user_json = await _ig_api_profile(context, handle)
    except Exception as e:
        print("[IG] profile API error:", handle, e)

    user = (((user_json or {}).get("data") or {}).get("user") or {})
    if user:
        out["followers"]   = (user.get("edge_followed_by") or {}).get("count")
        media = user.get("edge_owner_to_timeline_media") or {}
        out["posts_total"] = media.get("count")
        user_id = user.get("id")
    else:
        user_id = None

    if user_id:
        try:
            feed = await _ig_api_last_posts(context, user_id, 5)
            items = (feed or {}).get("items") or []
            for it in items[:5]:
                code = it.get("code") or it.get("shortcode")
                likes = it.get("like_count")
                comments = it.get("comment_count") or (it.get("comments") or {}).get("count")
                out["recent_posts"].append({"shortcode": code, "likes": likes, "comments": comments})
        except Exception as e:
            print("[IG] feed API error:", handle, e)

    if not out["recent_posts"]:
        try:
            await visit(page, f"https://www.instagram.com/{handle}/", 1500)
            items = page.locator('article a[href^="/p/"], article a[href^="/reel/"]')
            count = await items.count()
            for i in range(min(5, count)):
                a = items.nth(i)
                await a.scroll_into_view_if_needed()
                await a.hover()
                await page.wait_for_timeout(800)
                numbers = []
                aria = await a.get_attribute("aria-label")
                if aria:
                    for t in re.findall(r"[\d\.,KkMmBb]+(?:\s*(?:mn|b))?", aria):
                        v = parse_compact_number(t)
                        if isinstance(v, int): numbers.append(v)
                span_texts = await a.locator("span").all_inner_texts()
                for t in span_texts:
                    v = parse_compact_number(t)
                    if isinstance(v, int): numbers.append(v)
                nums = [n for n in numbers if isinstance(n, int)]
                nums.sort(reverse=True)
                href = await a.get_attribute("href")
                sc = None
                if href:
                    m = re.search(r"/(p|reel)/([^/?#]+)", href)
                    if m: sc = m.group(2)
                likes = nums[0] if len(nums) >= 1 else None
                comments = nums[1] if len(nums) >= 2 else None
                out["recent_posts"].append({"shortcode": sc, "likes": likes, "comments": comments})
        except Exception as e:
            print("[IG] UI hover fallback error:", handle, e)

    like_vals = [p["likes"] for p in out["recent_posts"] if isinstance(p.get("likes"), int)]
    comm_vals = [p["comments"] for p in out["recent_posts"] if isinstance(p.get("comments"), int)]
    if like_vals: out["avg_likes"] = sum(like_vals)/len(like_vals)
    if comm_vals: out["avg_comments"] = sum(comm_vals)/len(comm_vals)
    return out

# ================== Facebook ==================
def _to_mbasic(url: str) -> str:
    if url.startswith("https://www.facebook.com/"):
        return url.replace("https://www.facebook.com/","https://mbasic.facebook.com/",1)
    if url.startswith("https://m.facebook.com/"):
        return url.replace("https://m.facebook.com/","https://mbasic.facebook.com/",1)
    if "mbasic.facebook.com" not in url and "facebook.com" in url:
        return url.replace("facebook.com","mbasic.facebook.com",1)
    return url

def _to_www_about_en(url: str) -> str:
    u = url.replace("https://mbasic.facebook.com/", "https://www.facebook.com/")
    if "?" in u: u = u.split("?", 1)[0]
    if not u.rstrip("/").endswith("/about"):
        u = u.rstrip("/") + "/about"
    sep = "&" if "?" in u else "?"
    return f"{u}{sep}locale=en_US"

def _extract_fb_counts(text: str) -> int | None:
    pats = [
        r"([\d\.\,\sKkMmBb]+)\s*(followers|people\s+follow\s+this)",
        r"([\d\.\,\sKkMmBb]+)\s*(takipçi|bu\s+sayfayı\s+takip\s+ediyor)",
        r"([\d\.\,\sKkMmBb]+)\s*(beğeni|kişinin\s+hoşuna\s+gidiyor|likes)"
    ]
    vals = []
    for pat in pats:
        for num,_ in re.findall(pat, text, flags=re.IGNORECASE):
            v = parse_compact_number(num)
            if v is not None: vals.append(v)
    return max(vals) if vals else None

async def ensure_facebook_login(pw, headless=False):
    if FB_STATE.exists():
        return await pw.chromium.launch(headless=headless)
    browser = await pw.chromium.launch(headless=headless)
    context = await browser.new_context(user_agent=UA, viewport={"width":1280,"height":800})
    page = await context.new_page()
    print("\n[FB] Facebook login page is opening...")
    await visit(page, "https://mbasic.facebook.com/login.php", 1000)
    print("[FB] Please LOGIN in the browser window, then press ENTER here.")
    await pause()
    try:
        await context.storage_state(path=str(FB_STATE))
        print("[FB] Saved login state -> fb_state.json\n")
    except Exception as e:
        print("[FB] Could not save storage_state:", e)
    await browser.close()
    return await pw.chromium.launch(headless=headless)

async def fb_resolve_page(page, label: str, fb_map: dict):
    print(f"[FB] Opening search for ‘{label}’. Go to the correct page → press ENTER.")
    q = quote(label)
    await visit(page, f"https://mbasic.facebook.com/search/top/?q={q}", 1500)
    await pause()
    full = page.url
    if "/login" in full or "/search/" in full or "/home.php" in full or "/story.php" in full:
        print("   [FB] Invalid URL. Skipped.")
        return None
    full = _to_mbasic(full)
    fb_map[label] = full; save_json(FB_MAP, fb_map)
    print(f"   [FB] Resolved: {label} -> {full}"); return full

async def fb_fetch_followers(page_ctx, full_url: str):
    full_url = _to_mbasic(full_url)
    async def _one_try(url: str, wait="domcontentloaded", extra_wait_ms=1000):
        p = await page_ctx.context.new_page()
        try:
            await p.goto(url, wait_until=wait, timeout=90000)
            await p.wait_for_timeout(extra_wait_ms)
            return await p.inner_text("body")
        finally:
            await p.close()
    body_all = ""
    try: body_all += "\n" + (await _one_try(full_url, extra_wait_ms=800))
    except Exception as e: print("[FB] mbasic main err:", full_url, e)
    try:
        info_url = full_url + ("&v=info" if "?" in full_url else "?v=info")
        body_all += "\n" + (await _one_try(info_url, extra_wait_ms=800))
    except Exception as e: print("[FB] mbasic info err:", info_url, e)
    try:
        about_url = _to_www_about_en(full_url)
        body_all += "\n" + (await _one_try(about_url, extra_wait_ms=1600))
    except Exception as e: print("[FB] www/about err:", about_url, e)
    if not body_all.strip(): return None
    return _extract_fb_counts(body_all)

# ================== TikTok ==================
async def tt_fetch_followers(context, page, username: str):
    api_url = f"https://www.tiktok.com/api/user/detail/?uniqueId={username}"
    headers = {"User-Agent": UA, "Accept": "application/json, text/plain, */*",
               "Referer": f"https://www.tiktok.com/@{username}"}
    try:
        resp = await context.request.get(api_url, headers=headers, timeout=25000)
        if resp.ok:
            try:
                data = await resp.json()
                stats = (data.get("userInfo") or {}).get("stats") or {}
                fol = stats.get("followerCount"); likes = stats.get("heartCount")
                if fol is not None or likes is not None: return fol, likes
            except Exception: pass
    except Exception: pass
    try:
        await page.goto(f"https://www.tiktok.com/@{username}", wait_until="load", timeout=90000)
        await page.wait_for_timeout(2200)
        fol_txt = await page.locator('[data-e2e="followers-count"]').inner_text()
        like_txt = await page.locator('[data-e2e="likes-count"]').inner_text()
        return parse_compact_number(fol_txt), parse_compact_number(like_txt)
    except Exception: pass
    try:
        sels = ["script#__UNIVERSAL_DATA_FOR_REHYDRATION__","script#SIGI_STATE","script#__NEXT_DATA__"]
        for sel in sels:
            if await page.locator(sel).count():
                raw = await page.locator(sel).first.inner_text()
                data = json.loads(raw); txt = json.dumps(data)
                m1 = re.search(r'"followerCount"\s*:\s*(\d+)', txt)
                m2 = re.search(r'"heartCount"\s*:\s*(\d+)', txt)
                fol = int(m1.group(1)) if m1 else None
                likes = int(m2.group(1)) if m2 else None
                if fol is not None or likes is not None: return fol, likes
    except Exception as e:
        print("[TT] Fallback err:", username, e)
    return None, None

# ================== Runner (CSV dahil) ==================
async def main():
    results = []  # CSV'ye yazılacak satırlar

    ig_map = {**DEFAULT_IG_MAP, **load_json(IG_MAP)}; save_json(IG_MAP, ig_map)
    fb_map_loaded = load_json(FB_MAP)
    fb_map = {**DEFAULT_FB_MAP, **fb_map_loaded}
    fb_map = {k: _to_mbasic(v) for k,v in fb_map.items()}; save_json(FB_MAP, fb_map)

    async with async_playwright() as pw:
        # IG & TikTok
        ig_browser = await ensure_instagram_login(pw, headless=False)
        if IG_STATE.exists():
            ig_context = await ig_browser.new_context(user_agent=UA, locale="en-US",
                             viewport={"width":1280,"height":800}, storage_state=str(IG_STATE))
        else:
            ig_context = await ig_browser.new_context(user_agent=UA, locale="en-US",
                             viewport={"width":1280,"height":800})
        page = await ig_context.new_page()

        # Instagram
        for label in instagram_inputs:
            handle = ig_map.get(label) or await ig_resolve_handle(page, label, ig_map)
            if not handle: continue
            await visit(page, f"https://www.instagram.com/{handle}/?hl=en", 500)
            stats = await ig_fetch_stats(ig_context, page, handle)

            print("IG >", f"{label} ({handle})",
                  "followers:", stats["followers"],
                  "posts_total:", stats["posts_total"],
                  "| last5_avg_likes:", (round(stats["avg_likes"]) if stats["avg_likes"] is not None else None),
                  "last5_avg_comments:", (round(stats["avg_comments"]) if stats["avg_comments"] is not None else None))
            for p in stats["recent_posts"]:
                print("   -", p.get("shortcode"), "likes:", p.get("likes"), "comments:", p.get("comments"))

            results.append({
                "Platform": "Instagram",
                "Account": label,
                "Handle_or_URL": handle,
                "Followers": stats["followers"],
                "Posts_Total": stats["posts_total"],
                "Avg_Likes_Last5": round(stats["avg_likes"]) if stats["avg_likes"] is not None else None,
                "Avg_Comments_Last5": round(stats["avg_comments"]) if stats["avg_comments"] is not None else None,
                "Recent_Posts_JSON": json.dumps(stats["recent_posts"], ensure_ascii=False),
                "Extra": "",
                "Collected_At": datetime.now().isoformat(timespec="seconds"),
            })

        # TikTok
        for u in tt_users:
            fol, likes = await tt_fetch_followers(ig_context, page, u)
            print("TT >", u, "followers:", fol, "likes:", likes)
            results.append({
                "Platform": "TikTok",
                "Account": u,
                "Handle_or_URL": u,
                "Followers": fol,
                "Posts_Total": None,
                "Avg_Likes_Last5": None,
                "Avg_Comments_Last5": None,
                "Recent_Posts_JSON": "",
                "Extra": f"likes_total={likes}" if likes is not None else "",
                "Collected_At": datetime.now().isoformat(timespec="seconds"),
            })
        await ig_browser.close()

        # Facebook
        fb_browser = await ensure_facebook_login(pw, headless=False)
        if FB_STATE.exists():
            fb_context = await fb_browser.new_context(user_agent=UA, viewport={"width":1280,"height":800},
                                                      storage_state=str(FB_STATE))
        else:
            fb_context = await fb_browser.new_context(user_agent=UA, viewport={"width":1280,"height":800})
        fb_page = await fb_context.new_page()
        for label in facebook_queries:
            full = fb_map.get(label) or await fb_resolve_page(fb_page, label, fb_map)
            if full:
                count = await fb_fetch_followers(fb_page, full)
                print("FB >", f"{label} ({full})", count)
                results.append({
                    "Platform": "Facebook",
                    "Account": label,
                    "Handle_or_URL": full,
                    "Followers": count,
                    "Posts_Total": None,
                    "Avg_Likes_Last5": None,
                    "Avg_Comments_Last5": None,
                    "Recent_Posts_JSON": "",
                    "Extra": "",
                    "Collected_At": datetime.now().isoformat(timespec="seconds"),
                })
        await fb_browser.close()

    # --- CSV’ye yaz ---
    if results:
        filename = f"social_stats_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        keys = ["Platform","Account","Handle_or_URL","Followers","Posts_Total",
                "Avg_Likes_Last5","Avg_Comments_Last5","Recent_Posts_JSON","Extra","Collected_At"]
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(results)
        print(f"✅ Veriler CSV'ye kaydedildi: {Path(filename).resolve()}")

if __name__ == "__main__":
    asyncio.run(main())

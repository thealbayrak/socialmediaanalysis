import pandas as pd
import time
import sys
import os
import asyncio
import random
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ==============================
#  OTOMATİK INSTAGRAM GİRİŞ BİLGİLERİ
# LÜTFEN KENDİ BİLGİLERİNİZİ GİRİN
# ==============================
USERNAME = "sezaralim" 
PASSWORD = "Enes2003**"
# ==============================

TARGETS_FILE = "targets.txt"  
OUT_CSV = "followers_usernames_pw.csv"
OUT_XLSX = "followers_usernames_pw.xlsx"

# İnsancıl davranış için rastgele bekleme fonksiyonu
def get_random_wait():
    """1.5 ile 3.5 saniye arasında rastgele bir bekleme süresi döndürür."""
    return random.uniform(1.5, 3.5)

SCROLL_WAIT_SECONDS = 6.0 # Yeni verinin yüklenmesini bekleme süresi
MAX_FOLLOWERS_PER_TARGET = None 

# ==============================
# YARDIMCI VE DAVRANIŞ FONKSİYONLARI
# ==============================

def load_targets(path):
    if not os.path.exists(path):
        print(f"Hata: Hedef dosyası bulunamadı: {path}")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip().lstrip("@") for line in f if line.strip()]

async def humanize_explore(page):
    """Giriş sonrası Keşfet sayfasına gider ve rastgele aşağı kaydırır."""
    print("  👤 Kullanıcı davranışı: Keşfet sayfasına gidiliyor...")
    
    await page.goto("https://www.instagram.com/explore/", timeout=60000)
    await asyncio.sleep(get_random_wait() * 2)
    
    scroll_count = random.randint(3, 5) 
    for i in range(scroll_count):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        print(f"  👤 Kaydırma {i + 1}/{scroll_count} yapıldı.")
        await asyncio.sleep(get_random_wait())

    print("  👤 Keşfet ziyareti tamamlandı.")

# ==============================
# ANA ÇEKİM FONKSİYONU
# ==============================

async def scrape_followers(page, target, rows):
    print(f"\n==> Hedef hesap: **{target}**")
    
    # 1. Hedef profile git
    await page.goto(f"https://www.instagram.com/{target}/", timeout=60000)
    await asyncio.sleep(get_random_wait() * 2)

    # 2. Takipçi Penceresini MANUEL Açma
    
    print("\n========================================================")
    print("📢 LÜTFEN ŞİMDİ TARAYICIDAKİ TAKİPÇİ SAYISINA MANUEL TIKLAYIN...")
    print("📢 15 saniye içinde tıklamanız bekleniyor...")
    print("========================================================\n")
    
    # Koda devam etmeden önce 15 saniye bekle
    await asyncio.sleep(15) 
    
    # 3. Pencereyi doğrulama ve kaydırma elementini bulma
    
    # YENİ SEÇİCİ (EN GÜVENİLİR VERSİYON): style özelliğinde "overflow: hidden auto" olan div'i arar.
    follower_list_selector = 'div[role="dialog"] div[style*="overflow: hidden auto"]'
    
    try:
        follower_list = page.locator(follower_list_selector).nth(0)
        await follower_list.wait_for(state="visible", timeout=10000)
        print("  ✅ Takipçi penceresi algılandı, veri çekimi başlıyor...")
        
    except PlaywrightTimeoutError:
         print("  ❌ HATA: Pencere algılanmadı. Manuel tıklama yapılmadı veya seçici çalışmıyor.")
         return
    except Exception as e:
        print(f"  ❌ Kritik Hata: Pencere seçici hatası. Hata: {e}")
        return

    # 4. Kaydırma ve Çekme Döngüsü (Agresif Kaydırma Fix'i)
    last_scroll_height = -1
    count = 0
    
    while True:
        # Kullanıcı adı elementlerini çek
        username_elements = await follower_list.locator('a[role="link"][tabindex="0"]').all()
        current_usernames = set()
        
        for element in username_elements:
            username = await element.get_attribute("href")
            if username and username.startswith('/'):
                username = username.strip('/').split('/')[0]
                current_usernames.add(username)
                
        # Toplananları ana listeye ekle
        for username in current_usernames:
            if not any(row['follower_username'] == username for row in rows if row['target_profile'] == target):
                if MAX_FOLLOWERS_PER_TARGET and count >= MAX_FOLLOWERS_PER_TARGET:
                    break
                        
                rows.append({
                    "target_profile": target,
                    "follower_username": username
                })
                count += 1
        
        # Limite ulaşıldıysa döngüyü kes
        if MAX_FOLLOWERS_PER_TARGET and count >= MAX_FOLLOWERS_PER_TARGET:
            print(f"  -> Maksimum limit olan {MAX_FOLLOWERS_PER_TARGET} takipçiye ulaşıldı.")
            break
            
        # Agresif Kaydırma FIX'i: Kaydırma komutunu 3 kez tekrarlayarak zorla
        for _ in range(3):
            await follower_list.evaluate("el => el.scrollTo(0, el.scrollHeight)")
            await asyncio.sleep(0.5) # Kısa bir bekleme
        
        # Yeni verinin yüklenmesi için insancıl bekleme
        await asyncio.sleep(SCROLL_WAIT_SECONDS)
        
        # Kaydırma yüksekliğini kontrol et
        current_scroll_height = await follower_list.evaluate("el => el.scrollHeight")
        
        if current_scroll_height == last_scroll_height:
            print("  Listenin sonuna ulaşıldı veya yeni veri yüklenmedi.")
            break
        
        last_scroll_height = current_scroll_height
        print(f"  -> Toplanan: {count} - Kaydırma devam ediyor...")

    print(f"  -> Toplam çekilen takipçi sayısı: **{count}**")
    
# ==============================
# ANA ÇALIŞTIRMA FONKSİYONU
# ==============================

async def main():
    print("Instagram Follower Scraper (Playwright - Final Attempt)\n")
    
    targets = load_targets(TARGETS_FILE)
    rows = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False) 
        page = await browser.new_page()

        # Giriş yapma
        print("Instagram'a giriş yapılıyor...")
        
        # Giriş denemesi fonksiyonu
        async def try_login(p, page):
            await page.goto("https://www.instagram.com/accounts/login/", timeout=60000)
            await page.wait_for_selector('input[name="username"]', timeout=7000) 
            await page.fill('input[name="username"]', USERNAME)
            await page.fill('input[name="password"]', PASSWORD)
            await page.click('button[type="submit"]')
            await asyncio.sleep(5) 

        # Giriş döngüsü (One-Tap Sorununu Çözmek İçin)
        login_successful = False
        
        try:
            await try_login(p, page)
            login_successful = True
        except PlaywrightTimeoutError:
            print("⚠️ İlk deneme başarısız (One-Tap/Zaman Aşımı), normal giriş zorlanıyor...")
            await asyncio.sleep(3) 
            try:
                 await try_login(p, page)
                 login_successful = True
            except Exception as e:
                print(f"❌ Kritik Hata: Giriş başarısız (İkinci Deneme). Hata: {e}")
                await browser.close()
                sys.exit(1)
        except Exception as e:
             print(f"❌ Kritik Hata: Giriş başarısız. Hata: {e}")
             await browser.close()
             sys.exit(1)


        # Eğer giriş başarılı ise devam et
        if login_successful:
            # Pop-up yönetimi
            try:
                await page.click("text=Şimdi Değil", timeout=5000)
                await asyncio.sleep(get_random_wait())
            except:
                pass 
                
            print("✅ Başarıyla giriş yapıldı ve ilk pop-up yönetildi.\n")
        

        # İnsancıl davranış: Keşfet ziyareti
        await humanize_explore(page)
        
        # Takipçileri çekme döngüsü
        for target in targets:
            await scrape_followers(page, target, rows)
            
        await browser.close()

    # Verileri kaydetme
    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig") 
        df.to_excel(OUT_XLSX, index=False)
        print(f"\nİşlem tamamlandı!\nKaydedilen dosyalar:")
        print(f"- **{OUT_CSV}**")
        print(f"- **{OUT_XLSX}**")
        print(f"Toplam satır: **{len(df)}**")
    else:
        print("\nHiç veri alınamadı.")

if __name__ == "__main__":
    asyncio.run(main())
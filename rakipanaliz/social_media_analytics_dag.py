"""
Sosyal Medya Analizi DAG'ı
Bu DAG Instagram, Facebook ve TikTok hesaplarından veri toplar ve analiz eder.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import asyncio
import json
import yaml
from pathlib import Path
import pandas as pd
from typing import Dict, List, Any

# Default arguments
default_args = {
    'owner': 'rakip-analiz-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG tanımı
dag = DAG(
    'sosyal_medya_analizi',
    default_args=default_args,
    description='Instagram, Facebook ve TikTok hesaplarından veri toplama ve analiz',
    schedule_interval='@daily',  # Her gün çalışır
    catchup=False,
    tags=['sosyal-medya', 'analiz', 'veri-toplama'],
)

# Konfigürasyon dosyalarının yolları
CONFIG_DIR = Path('/opt/airflow/dags/configs')
DATA_DIR = Path('/opt/airflow/dags/data')
REPORTS_DIR = DATA_DIR / 'reports'

def load_social_accounts(**context):
    """Sosyal medya hesaplarını YAML dosyasından yükle"""
    try:
        with open(CONFIG_DIR / 'social_accounts.yml', 'r', encoding='utf-8') as f:
            accounts = yaml.safe_load(f)
        
        # XCom'a kaydet
        context['task_instance'].xcom_push(key='social_accounts', value=accounts)
        print(f"Sosyal medya hesapları yüklendi: {accounts}")
        return accounts
    except Exception as e:
        print(f"Hesapları yüklerken hata: {e}")
        raise

def run_instagram_analysis(**context):
    """Instagram hesaplarını analiz et"""
    import sys
    import os
    
    # Proje dizinini Python path'ine ekle
    project_dir = '/opt/airflow/dags'
    if project_dir not in sys.path:
        sys.path.append(project_dir)
    
    try:
        # Sosyal medya hesaplarını XCom'dan al
        accounts = context['task_instance'].xcom_pull(key='social_accounts', task_ids='load_accounts')
        instagram_accounts = accounts.get('instagram', [])
        
        # Instagram analizi için subprocess kullan (async fonksiyonlar için)
        import subprocess
        
        # Instagram verilerini topla
        cmd = f"cd {project_dir} && python -c \"" \
              f"import asyncio; " \
              f"from social_pw import *; " \
              f"async def ig_only(): " \
              f"    async with async_playwright() as pw: " \
              f"        browser = await ensure_instagram_login(pw, headless=True); " \
              f"        context = await browser.new_context(user_agent=UA, storage_state='ig_state.json'); " \
              f"        page = await context.new_page(); " \
              f"        results = []; " \
              f"        for handle in {instagram_accounts}: " \
              f"            stats = await ig_fetch_stats(context, page, handle); " \
              f"            results.append({{'handle': handle, 'stats': stats}}); " \
              f"        await browser.close(); " \
              f"        return results; " \
              f"results = asyncio.run(ig_only()); " \
              f"import json; " \
              f"with open('{DATA_DIR}/instagram_data.json', 'w') as f: json.dump(results, f, ensure_ascii=False, indent=2)" \
              f"\""
        
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Instagram analizi hatası: {result.stderr}")
            raise Exception(f"Instagram analizi başarısız: {result.stderr}")
        
        print("Instagram analizi tamamlandı")
        return "instagram_success"
        
    except Exception as e:
        print(f"Instagram analizi sırasında hata: {e}")
        raise

def run_facebook_analysis(**context):
    """Facebook sayfalarını analiz et"""
    import sys
    import subprocess
    
    project_dir = '/opt/airflow/dags'
    if project_dir not in sys.path:
        sys.path.append(project_dir)
    
    try:
        # Sosyal medya hesaplarını XCom'dan al
        accounts = context['task_instance'].xcom_pull(key='social_accounts', task_ids='load_accounts')
        facebook_accounts = accounts.get('facebook', [])
        
        # Facebook analizi için subprocess kullan
        cmd = f"cd {project_dir} && python -c \"" \
              f"import asyncio; " \
              f"from social_pw import *; " \
              f"async def fb_only(): " \
              f"    async with async_playwright() as pw: " \
              f"        browser = await ensure_facebook_login(pw, headless=True); " \
              f"        context = await browser.new_context(user_agent=UA, storage_state='fb_state.json'); " \
              f"        page = await context.new_page(); " \
              f"        results = []; " \
              f"        fb_map = load_json(Path('fb_pages.json')); " \
              f"        for account in {facebook_accounts}: " \
              f"            url = fb_map.get(account); " \
              f"            if url: " \
              f"                followers = await fb_fetch_followers(page, url); " \
              f"                results.append({{'account': account, 'url': url, 'followers': followers}}); " \
              f"        await browser.close(); " \
              f"        return results; " \
              f"results = asyncio.run(fb_only()); " \
              f"import json; " \
              f"with open('{DATA_DIR}/facebook_data.json', 'w') as f: json.dump(results, f, ensure_ascii=False, indent=2)" \
              f"\""
        
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Facebook analizi hatası: {result.stderr}")
            raise Exception(f"Facebook analizi başarısız: {result.stderr}")
        
        print("Facebook analizi tamamlandı")
        return "facebook_success"
        
    except Exception as e:
        print(f"Facebook analizi sırasında hata: {e}")
        raise

def run_tiktok_analysis(**context):
    """TikTok hesaplarını analiz et"""
    import sys
    import subprocess
    
    project_dir = '/opt/airflow/dags'
    if project_dir not in sys.path:
        sys.path.append(project_dir)
    
    try:
        # Sosyal medya hesaplarını XCom'dan al
        accounts = context['task_instance'].xcom_pull(key='social_accounts', task_ids='load_accounts')
        tiktok_accounts = accounts.get('tiktok', [])
        
        # TikTok analizi için subprocess kullan
        cmd = f"cd {project_dir} && python -c \"" \
              f"import asyncio; " \
              f"from social_pw import *; " \
              f"async def tt_only(): " \
              f"    async with async_playwright() as pw: " \
              f"        browser = await pw.chromium.launch(headless=True); " \
              f"        context = await browser.new_context(user_agent=UA); " \
              f"        page = await context.new_page(); " \
              f"        results = []; " \
              f"        for username in {tiktok_accounts}: " \
              f"            followers, likes = await tt_fetch_followers(context, page, username); " \
              f"            results.append({{'username': username, 'followers': followers, 'likes': likes}}); " \
              f"        await browser.close(); " \
              f"        return results; " \
              f"results = asyncio.run(tt_only()); " \
              f"import json; " \
              f"with open('{DATA_DIR}/tiktok_data.json', 'w') as f: json.dump(results, f, ensure_ascii=False, indent=2)" \
              f"\""
        
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"TikTok analizi hatası: {result.stderr}")
            raise Exception(f"TikTok analizi başarısız: {result.stderr}")
        
        print("TikTok analizi tamamlandı")
        return "tiktok_success"
        
    except Exception as e:
        print(f"TikTok analizi sırasında hata: {e}")
        raise

def process_and_report(**context):
    """Toplanan verileri işle ve rapor oluştur"""
    try:
        # Veri dosyalarını yükle
        instagram_data = []
        facebook_data = []
        tiktok_data = []
        
        # Instagram verilerini yükle
        ig_file = DATA_DIR / 'instagram_data.json'
        if ig_file.exists():
            with open(ig_file, 'r', encoding='utf-8') as f:
                instagram_data = json.load(f)
        
        # Facebook verilerini yükle
        fb_file = DATA_DIR / 'facebook_data.json'
        if fb_file.exists():
            with open(fb_file, 'r', encoding='utf-8') as f:
                facebook_data = json.load(f)
        
        # TikTok verilerini yükle
        tt_file = DATA_DIR / 'tiktok_data.json'
        if tt_file.exists():
            with open(tt_file, 'r', encoding='utf-8') as f:
                tiktok_data = json.load(f)
        
        # Rapor oluştur
        report = {
            'tarih': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'instagram': {
                'toplam_hesap': len(instagram_data),
                'hesaplar': instagram_data
            },
            'facebook': {
                'toplam_sayfa': len(facebook_data),
                'sayfalar': facebook_data
            },
            'tiktok': {
                'toplam_hesap': len(tiktok_data),
                'hesaplar': tiktok_data
            }
        }
        
        # Raporu kaydet
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        report_file = REPORTS_DIR / f"sosyal_medya_raporu_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        # Özet rapor oluştur
        summary = {
            'tarih': report['tarih'],
            'instagram_toplam_takipci': sum([
                acc['stats'].get('followers', 0) or 0 
                for acc in instagram_data 
                if acc.get('stats', {}).get('followers')
            ]),
            'facebook_toplam_takipci': sum([
                acc.get('followers', 0) or 0 
                for acc in facebook_data 
                if acc.get('followers')
            ]),
            'tiktok_toplam_takipci': sum([
                acc.get('followers', 0) or 0 
                for acc in tiktok_data 
                if acc.get('followers')
            ])
        }
        
        summary_file = REPORTS_DIR / f"ozet_rapor_{datetime.now().strftime('%Y%m%d')}.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"Rapor oluşturuldu: {report_file}")
        print(f"Özet rapor: {summary_file}")
        print(f"Instagram: {summary['instagram_toplam_takipci']} takipçi")
        print(f"Facebook: {summary['facebook_toplam_takipci']} takipçi") 
        print(f"TikTok: {summary['tiktok_toplam_takipci']} takipçi")
        
        return summary
        
    except Exception as e:
        print(f"Rapor oluşturma hatası: {e}")
        raise

# Task'ları tanımla
load_accounts_task = PythonOperator(
    task_id='load_accounts',
    python_callable=load_social_accounts,
    dag=dag,
)

instagram_task = PythonOperator(
    task_id='instagram_analysis',
    python_callable=run_instagram_analysis,
    dag=dag,
)

facebook_task = PythonOperator(
    task_id='facebook_analysis', 
    python_callable=run_facebook_analysis,
    dag=dag,
)

tiktok_task = PythonOperator(
    task_id='tiktok_analysis',
    python_callable=run_tiktok_analysis,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_and_report',
    python_callable=process_and_report,
    dag=dag,
)

# Dizinleri oluştur
create_dirs_task = BashOperator(
    task_id='create_directories',
    bash_command=f'mkdir -p {DATA_DIR} {REPORTS_DIR} {DATA_DIR}/posts {DATA_DIR}/snapshots',
    dag=dag,
)

# Task bağımlılıklarını ayarla
create_dirs_task >> load_accounts_task

# Sosyal medya analizleri paralel çalışır
load_accounts_task >> [instagram_task, facebook_task, tiktok_task]

# Tüm analizler bittikten sonra rapor oluştur
[instagram_task, facebook_task, tiktok_task] >> process_task

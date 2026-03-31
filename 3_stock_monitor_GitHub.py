# ============================================================
# 專案：Python股票週K布林RSI+Gmail推播自動通知
# 版本：(由AI每次改版時自動填寫)
# 更新日期：(由AI每次改版時依照對話視窗提供的日期,並經由使用者確認後為準)（新增期貨5分K模式）
# 適用：台股1845支 + 美股38支 + 虛擬幣3支 + 三合一追蹤【安聯月配息基金】
# 通知方式：Gmail發信到 shchyu61@gmail.com
# 重要：本程式原則上只適用週K，可視情況用於日K以下
# =========================
# 快取設定（週K決定要不要看，日K決定準不準，5分K決定何時動手）
USE_CACHE = True
WEEKLY_REFRESH_HOUR = 8   # 每天早上更新一次週K
five_min_cache = {}  # ✅ 新增 5 分鐘 K 線快取容器
DELISTING_CHECK_DAYS = 3  # 每3天檢查一次下市風險（3天兼顧效能與即時性，可改1~7）
DELISTING_FILE = '2_delisting_cache.json'  # 下市風險本地快取檔
# ============================================================
# 【１．設定區】

# 驗證篩選模式：
#   False  = 正式模式（週K三層嚴格過濾）
#   True   = 測試模式
#   '5mk'  = 期貨5分K模式（週一15:01~週三13:30）
# ✅ 兩路控制（任一皆可）：
#   本機筆電：直接改下方預設值 'False'
#   GitHub Actions：futures-scan Job 自動帶入 TEST_MODE='5mk'，不需改程式碼
import os as _os_tm
_tm_raw = _os_tm.environ.get('TEST_MODE', 'False')
if   _tm_raw == 'False': TEST_MODE = False
elif _tm_raw == 'True':  TEST_MODE = True
else:                    TEST_MODE = _tm_raw   # '5mk' 保持字串

# ── 【期貨5分K專屬設定】──────────────────────────────────────────
FUTURES_5MK_TARGETS  = ['^TWII']   # 台指期替代標的
FUTURES_5MK_INTERVAL = 300         # 每5分鐘掃描一次
FUTURES_5MK_OWNER    = 'shchyu61@gmail.com'  # 5分K模式專屬帳號

# Gmail設定
# ✅ 💻【本機】執行：直接填入帳號密碼
# ✅ ☁️【雲端】GitHub Actions執行：自動從 GitHub Secrets 讀取，不需填寫
import os as _os
GMAIL_ACCOUNT  = _os.environ.get("GMAIL_ACCOUNT",  "shchyu61@gmail.com") # 您Gmail（寄件人）
GMAIL_PASSWORD = _os.environ.get("GMAIL_PASSWORD", "")  # ☁️【雲端】從Secrets讀取；或💻【本機】填入密碼格式："xxxx xxxx xxxx xxxx"（密碼可刪。實戰要補上。）。
NOTIFY_EMAIL   = "shchyu61@gmail.com"       # 收通知的信箱（可與寄件人同一個）

# 台股持有股票，要去此章節最下面的第51行自己輸入。

# 美股掃描清單（道瓊30 + DRIP精選 + 持有股票）
US_STOCKS = [
    # 道瓊工業30支
    'AAPL', 'AMGN', 'AXP', 'BA', 'CAT',
    'CRM', 'CSCO', 'CVX', 'DIS', 'DOW',
    'GS', 'HD', 'HON', 'IBM', 'INTC',
    'JNJ', 'JPM', 'KO', 'MCD', 'MMM',
    'MRK', 'MSFT', 'NKE', 'PG', 'TRV',
    'UNH', 'V', 'VZ', 'WMT', 'AMZN', 'NVDA', 'AVGO', # 補上強勢領導股: 亞馬遜, 輝達, 博通
    # DRIP精選（去除與道瓊重複後）
    'T', 'XOM', 'PEP', 'CL', 'ABT', 'GE',
    # 持有（O、PFE；KO已在道瓊內）
    'O', 'PFE'
]

# 虛擬幣清單
CRYPTO_LIST = [
    'BTC-USD',   # 比特幣
    'ETH-USD',   # 以太幣
    'DOGE-USD'   # 狗狗幣
]

# 持有清單（賣出/平倉只掃這些）
HOLDINGS_US     = ['KO', 'O', 'PFE']
HOLDINGS_CRYPTO = ['BTC-USD', 'ETH-USD', 'DOGE-USD']
HOLDINGS_TW     = ['2330', '3037','3147','6188']  # 有台股持有時填入，例如：['2330', '2317']

# ============================================================
# 【１-1．Firebase 期貨狀態寫入（方案B：Python寫，網頁讀）】
# 每次期貨5分K掃描結束後，把狀態寫入 Firebase
# 網頁登入後讀同一路徑，不需要 Token 在前端，F12 看不到敏感資訊
# Firestore 路徑：artifacts/kj-wealth-manager/public/futures_status
# ============================================================
FIREBASE_PROJECT_ID = 'kj-wealth-manager'
FIREBASE_CRED_ENV   = 'FIREBASE_SERVICE_KEY'

def write_futures_status_to_firebase(status: str, scan_time: str, signal_count: int):
    cred_json = _os_tm.environ.get(FIREBASE_CRED_ENV, '')
    if not cred_json:
        print('  ℹ️ FIREBASE_SERVICE_KEY 未設定，跳過Firebase狀態寫入')
        return
    try:
        import json as _json, urllib.request as _req, urllib.parse as _up
        import base64 as _b64, time as _time
        cred = _json.loads(cred_json)
        token_url = 'https://oauth2.googleapis.com/token'
        header  = _b64.urlsafe_b64encode(_json.dumps({'alg':'RS256','typ':'JWT'}).encode()).rstrip(b'=')
        now_ts  = int(_time.time())
        payload = _b64.urlsafe_b64encode(_json.dumps({
            'iss': cred['client_email'], 'sub': cred['client_email'],
            'aud': 'https://oauth2.googleapis.com/token',
            'iat': now_ts, 'exp': now_ts + 3600,
            'scope': 'https://www.googleapis.com/auth/datastore'
        }).encode()).rstrip(b'=')
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.backends import default_backend
        private_key = serialization.load_pem_private_key(
            cred['private_key'].encode(), password=None, backend=default_backend())
        sig_input = header + b'.' + payload
        signature = private_key.sign(sig_input, padding.PKCS1v15(), hashes.SHA256())
        jwt_token = sig_input + b'.' + _b64.urlsafe_b64encode(signature).rstrip(b'=')
        body = _up.urlencode({'grant_type':'urn:ietf:params:oauth:grant-type:jwt-bearer',
                              'assertion':jwt_token.decode()}).encode()
        r = _req.urlopen(_req.Request(token_url, data=body,
                         headers={'Content-Type':'application/x-www-form-urlencoded'}))
        access_token = _json.loads(r.read())['access_token']
        fs_url = (f'https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}'
                  f'/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}/public/futures_status')
        doc_body = _json.dumps({'fields': {
            'status':       {'stringValue': status},
            'scan_time':    {'stringValue': scan_time},
            'signal_count': {'integerValue': str(signal_count)},
            'updated_at':   {'stringValue': scan_time},
        }}).encode()
        patch_url = fs_url + '?updateMask.fieldPaths=status&updateMask.fieldPaths=scan_time&updateMask.fieldPaths=signal_count&updateMask.fieldPaths=updated_at'
        req2 = _req.Request(patch_url, data=doc_body, method='PATCH',
                            headers={'Authorization': f'Bearer {access_token}',
                                     'Content-Type': 'application/json'})
        _req.urlopen(req2)
        print(f'  ✅ Firebase期貨狀態已寫入：{status} / {scan_time} / 訊號{signal_count}支')
    except Exception as e:
        print(f'  ⚠️ Firebase狀態寫入失敗（不影響主流程）：{e}')

# ============================================================
# ============================================================
# 【２．策略參數設定區】← 所有策略的可調數值都在這裡，不需往下翻
# ============================================================

# ── 【２-1】買進策略：截圖3、4條件（條件A + 條件B）──────────────
# 條件A：近N根任一最低價 <= 布林下緣 AND RSI上升 AND MACD柱放大
# 條件B：近N根Low均<布林中軌 AND 近N根High均<布林上軌
#         AND 前N根MACD柱持續縮小 AND 當根MACD柱放大
#
# ✅【K棒數量等比換算】─ 依K棒換算文件，切換週期時根數同步換算
#   週K = 3根（基準，3根≈3週）
#   日K = 15根（3週×5天=15根日K）
#   5分K = 54根（含夜盤的近期K棒，主力夜盤為主戰場）
BUY_LOOKBACK_BARS    = 3      # 週K回看根數（條件A/B共用）
BUY_LOOKBACK_DAILY   = 15     # 日K回看根數（3週×5天，等比換算）
BUY_LOOKBACK_5MK     = 54     # 5分K回看根數（近54根5分K棒，含夜盤）
# ── 【掃描週期模式】切換此處決定scan_stock用哪個週期把關 ──────────
# 'weekly' = 週K三道關卡（第一道週K/第二道日K eLeader/第三道5分K）
# 'daily'  = 日K三道關卡（第一道週K/第二道日K 15根/第三道5分K）
SCAN_MODE = 'weekly'   # 切換：'weekly' / 'daily'
BUY_RSI_MIN          = 35     # 買進RSI最低門檻（條件A，RSI需 > 此值才視為上升有效）
BUY_BOLL_TOLERANCE   = 1.02   # 布林下緣容忍度（1.02=允許價格在下緣上方2%內仍觸發）

# ── 【２-2】買進策略：eLeader 25個複合條件 ────────────────────────
# （eLeader條件邏輯在第8章，勿修改第8章程式碼，只改這裡的參數）
ELEADER_RSI_MAX      = 78     # eLeader基底條件：前根RSI需 < 此值才允許進場

# ── 【２-3】賣出策略：截圖1、2條件 ──────────────────────────────
# 最高價 >= 布林上緣 AND RSI下降 AND MACD柱縮小
SELL_BOLL_TOLERANCE  = 1.00   # 布林上緣容忍度（1.00=嚴格貼上緣；0.97=上緣下方3%即觸發）

# ── 【２-4】停損策略（預留，目前未啟用）─────────────────────────
# 未來可在此新增停損條件，例如：
# STOP_LOSS_PCT = 0.08   # 跌破買入價8%強制停損
# ENABLE_STOP_LOSS = False
ENABLE_STOP_LOSS     = False   # 停損開關（False=未啟用）

# ── 【２-5】大盤過濾策略 ─────────────────────────────────────────
# 台股大盤(^TWII)或道瓊(^DJI) RSI+MACD雙雙下彎時，是否發出警告
ENABLE_INDEX_FILTER  = True    # True=啟用大盤過濾警告 / False=忽略大盤只看個股
# 大盤過濾條件：RSI下彎 AND MACD柱下彎 → 發出警告（目前僅警告，不強制停止掃描）
# 若需強制停止進場，請將 main_task 中大盤判斷區的 print 改為 return

# ── 【２-6】全額交割股預警策略 ───────────────────────────────────
ENABLE_CASH_DELIVERY_CHECK = True   # True=啟用全額交割預警 / False=關閉
CASH_DELIVERY_CACHE_HOURS  = 24     # 全額交割清單快取時間（小時，建議24）

# ============================================================
# 【３．套件引用】
# ============================================================
import yfinance as yf
import pandas as pd
import pandas_ta as ta
import smtplib
import warnings
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import time
import pytz
import json
import os

weekly_cache = {} # 3.0版用到的快取,週資料不常變，存起來對
daily_cache  = {} # 3.0版用到的快取,日資料一天變一次，存起來也對。

warnings.filterwarnings('ignore')
import logging
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ============================================================
# 【３-1．下市風險預警模組（最高優先級）】
# 每 DELISTING_CHECK_DAYS 天檢查一次，結果存入本地 JSON 快取
# 持有股：❌❌終極警報 / 未持有股：⚠️勿碰預警（可提醒親友）
# ============================================================
def get_delisting_risk(ticker):
    """
    檢查個股是否有下市風險。
    回傳 (is_at_risk: bool, msg: str)
    每 DELISTING_CHECK_DAYS 天才重新向 Yahoo Finance 查詢一次，其餘時間讀本地快取。
    """
    now = datetime.now()

    # 1. 讀取本地快取
    delisting_cache = {}
    if os.path.exists(DELISTING_FILE):
        try:
            with open(DELISTING_FILE, 'r', encoding='utf-8') as f:
                delisting_cache = json.load(f)
        except:
            delisting_cache = {}

    # 2. 若快取未過期，直接回傳（不重複查詢）
    if ticker in delisting_cache:
        entry = delisting_cache[ticker]
        try:
            check_date = datetime.strptime(entry['date'], '%Y-%m-%d')
            if (now - check_date).days < DELISTING_CHECK_DAYS:
                return entry['is_at_risk'], entry['msg']
        except:
            pass

    # 3. 執行實際下市檢查
    is_at_risk = False
    msg = ""
    try:
        info = yf.Ticker(ticker).info

        # ── 【第4階段】官方公告下市日期（最明確，提前1~2週）──────
        delist_date = info.get('delistingDate')
        if delist_date:
            is_at_risk = True
            # 計算距離下市天數
            try:
                from datetime import date as _date
                d = _date.fromisoformat(str(delist_date))
                days_left = (d - _date.today()).days
                if days_left >= 0:
                    msg = f"⚠️【第4階段警報】官方公告下市日期：{delist_date}（距今 {days_left} 天），請儘速處理！"
                else:
                    msg = f"❌【已過下市日期】官方公告下市日期：{delist_date}，股票已停止交易！"
            except:
                msg = f"⚠️【第4階段警報】官方公告下市日期：{delist_date}，請儘速確認！"

        # ── 【第5階段】查無即時報價（停牌或已下市）────────────────
        elif info.get('regularMarketPrice') is None and info.get('currentPrice') is None:
            df_test = yf.download(ticker, period='5d', interval='1d', progress=False)
            if df_test is None or (hasattr(df_test, 'empty') and df_test.empty):
                is_at_risk = True
                msg = "❌【第5階段警報】近5日查無交易資料，疑似已停牌或下市，請立即確認！"

        # ── 市值異常歸零（財務崩潰早期警訊）────────────────────────
        elif info.get('marketCap') is not None and info.get('marketCap') == 0:
            is_at_risk = True
            msg = "⚠️ 市值歸零，財務狀況極度異常，建議立即確認！"

    except Exception as e:
        is_at_risk = True
        msg = f"無法獲取股票資訊（{e}），疑似下市或代碼變更"


    # 4. 寫入本地快取（下次同一支股票在期限內不再重查）
    delisting_cache[ticker] = {
        'is_at_risk': is_at_risk,
        'msg': msg,
        'date': now.strftime('%Y-%m-%d')
    }
    try:
        with open(DELISTING_FILE, 'w', encoding='utf-8') as f:
            json.dump(delisting_cache, f, ensure_ascii=False, indent=2)
    except:
        pass

    return is_at_risk, msg

# ============================================================
# 【３-2．全額交割股預警模組】（引用第2-6章設定）
# 來源1：TWSE OpenAPI（上市）→ 來源2：TPEX OpenAPI（上櫃）→ 來源3：備援
# ============================================================
import threading as _threading
_cash_delivery_cache = {'codes': set(), 'ts': None}
_cash_delivery_lock  = _threading.Lock()

def get_cash_delivery_set():
    """
    取得全額交割股票代碼 Set（上市+上櫃）。
    快取 CASH_DELIVERY_CACHE_HOURS 小時，避免重複抓取。
    回傳 set of str（純代碼，如 '1234'）
    """
    if not ENABLE_CASH_DELIVERY_CHECK:
        return set()

    from datetime import datetime as _dt
    now = _dt.now()
    with _cash_delivery_lock:
        # 快取未過期則直接回傳
        if _cash_delivery_cache['ts'] is not None:
            elapsed = (now - _cash_delivery_cache['ts']).total_seconds() / 3600
            if elapsed < CASH_DELIVERY_CACHE_HOURS:
                return _cash_delivery_cache['codes']

    codes = set()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'application/json'
    }

    # ── 來源1：TWSE OpenAPI（上市全額交割）────────────────────
    try:
        import requests as _req
        r = _req.get(
            'https://openapi.twse.com.tw/v1/company/cashPaymentStocks',
            headers=headers, timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            for item in data:
                code = item.get('公司代號') or item.get('Code') or item.get('code') or ''
                if code:
                    codes.add(str(code).strip())
            print(f'  ✅ TWSE全額交割上市：{len(codes)} 支')
    except Exception as e:
        print(f'  ⚠️ TWSE全額交割抓取失敗：{e}')

    # ── 來源2：TPEX OpenAPI（上櫃全額交割）────────────────────
    try:
        import requests as _req
        r2 = _req.get(
            'https://www.tpex.org.tw/openapi/v1/tpex_cash_payment_stocks',
            headers=headers, timeout=10
        )
        if r2.status_code == 200:
            data2 = r2.json()
            before = len(codes)
            for item in data2:
                code = item.get('公司代號') or item.get('Code') or item.get('code') or ''
                if code:
                    codes.add(str(code).strip())
            print(f'  ✅ TPEX全額交割上櫃：{len(codes)-before} 支')
    except Exception as e:
        print(f'  ⚠️ TPEX全額交割抓取失敗：{e}')

    # ── 來源3：備援 TWSE HTML 表格（若前兩個都失敗且 codes 仍空）──
    if not codes:
        try:
            import requests as _req
            from bs4 import BeautifulSoup as _BS
            r3 = _req.get(
                'https://www.twse.com.tw/zh/page/trading/exchange/TWTB4U.html',
                headers=headers, timeout=10
            )
            soup = _BS(r3.text, 'html.parser')
            for td in soup.select('table td:first-child'):
                code = td.get_text(strip=True)
                if code.isdigit() and len(code) == 4:
                    codes.add(code)
            print(f'  ✅ 備援HTML全額交割：{len(codes)} 支')
        except Exception as e:
            print(f'  ⚠️ 備援HTML全額交割抓取失敗：{e}')

    with _cash_delivery_lock:
        _cash_delivery_cache['codes'] = codes
        _cash_delivery_cache['ts']    = now

    print(f'  📋 全額交割股共 {len(codes)} 支（含上市+上櫃）')
    return codes

# ============================================================
# 【４．交易時段與數據抓取核心】
# ============================================================
def get_active_markets():
    """判斷現在哪些市場在交易中（台灣時間）"""
    tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(tz)
    weekday = now.weekday()  # 0=週一 4=週五 5=週六 6=週日
    hour = now.hour
    minute = now.minute
    time_val = hour * 60 + minute  # 換算成分鐘

    active = []

    # 週一到週五才有台股和美股
    if weekday <= 4:
        # 台股：09:00～13:30
        if 9*60 <= time_val <= 13*60+30:
            active.append('TW')

    is_us_time = False
    # 週一～週五 晚上
    if weekday <= 4 and time_val >= 21*60+30:
        is_us_time = True
    # 週二～週六 凌晨
    elif 1 <= weekday <= 5 and time_val <= 4*60:
        is_us_time = True

    if is_us_time:
        active.append('US')      # 美股
        active.append('CRYPTO')  # 虛擬幣

    # 期貨5分K時段：週一13:00~週三11:30（深夜01:00~05:00不觸發，人在睡覺）
    is_futures_time = False
    # 週一 13:00 以後
    if weekday == 0 and time_val >= 13*60:
        is_futures_time = True
    # 週二（排除深夜01:00~05:00）
    elif weekday == 1 and (time_val < 1*60 or time_val >= 5*60):
        is_futures_time = True
    # 週三 11:30 以前（日盤結束）
    elif weekday == 2 and time_val <= 11*60+30:
        is_futures_time = True
    if is_futures_time:
        active.append('FUTURES')
    return active

def get_stock_data(ticker, period='2y', interval='1wk', cache=None):
    """
    【核心函數】負責抓取 Yahoo Finance 資料並處理時區。
    修復 scan_stock() 呼叫時找不到此函數的問題。
    """
    cache_key = f"{ticker}_{interval}"
    
    # 1. 檢查快取
    if USE_CACHE and cache is not None and cache_key in cache:
        return cache[cache_key]
    
    try:
        # 2. 下載資料
        stock = yf.Ticker(ticker)
        df = stock.history(period=period, interval=interval)
        
        if df.empty:
            return None
            
        # 3. 統一處理時區為台北時間 (確保與主程式對齊)
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC').tz_convert('Asia/Taipei')
        else:
            df.index = df.index.tz_convert('Asia/Taipei')
            
        # 4. 存入快取
        if USE_CACHE and cache is not None:
            cache[cache_key] = df
            
        return df
    except Exception as e:
        print(f"  ❌ 抓取 {ticker} ({interval}) 失敗: {e}")
        return None

def get_data_period(market):
    """根據市場取得適當的資料期間"""
    return '1y'

# ============================================================
# 【５．Gmail通知函數】
# ============================================================
def send_gmail(subject, body):
    try:
        msg = MIMEMultipart()
        msg['From']    = GMAIL_ACCOUNT
        msg['To']      = NOTIFY_EMAIL
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain', 'utf-8'))

        pwd = GMAIL_PASSWORD.replace(" ", "")  # 移除空格
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(GMAIL_ACCOUNT, pwd)
            server.sendmail(GMAIL_ACCOUNT, NOTIFY_EMAIL, msg.as_string())
        print(f"  ✅ Gmail已發送：{subject}")
        return True
    except Exception as e:
        print(f"  ❌ Gmail發送失敗：{e}")
        return False
# ============================================================
# 【６．通知紀錄讀寫函數】
# ============================================================
def load_notified():
    if os.path.exists('4_notified_today.json'):
        try:
            with open('4_notified_today.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            print("⚠️ 通知紀錄檔損壞，已重置")
            return {}
    return {}

def save_notified(data):
    with open('4_notified_today.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
# ============================================================
# 【７．技術指標計算】
# ============================================================
def calc_indicators(df):
    if df is None or len(df) < 50:
        return None
    try:
        # 處理MultiIndex欄位
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        c = df['Close'].squeeze()
        h = df['High'].squeeze()
        l = df['Low'].squeeze()
        o = df['Open'].squeeze()
        v = df['Volume'].squeeze()

        # 均線
        df['ma_c_2']  = c.rolling(2).mean()
        df['ma_c_20'] = c.rolling(20).mean()
        df['ma_o_2']  = o.rolling(2).mean()
        df['ma_h_2']  = h.rolling(2).mean()
        df['ma_l_2']  = l.rolling(2).mean()
        df['ma_v_5']  = v.rolling(5).mean()

        # 布林通道（週期20，標準差2）
        std20            = c.rolling(20).std()
        df['boll_top20'] = df['ma_c_20'] + 2 * std20
        df['boll_bot20'] = df['ma_c_20'] - 2 * std20

        # 短期布林通道（週期2，標準差2）
        std2             = c.rolling(2).std()
        df['boll_top2']  = df['ma_c_2'] + 2 * std2
        df['boll_bot2']  = df['ma_c_2'] - 2 * std2

        # RSI(14) 與 EMA(RSI,9)
        df['rsi14']    = ta.rsi(c, length=14)
        df['ema_rsi9'] = df['rsi14'].ewm(span=9, adjust=False).mean()

        # MACD（收盤價。週K：快12 慢26 訊號9。日K：快8 慢21 訊號5）
        macd_df = ta.macd(c, fast=12, slow=26, signal=9)
        df['macd_line']   = macd_df.iloc[:, 0]
        df['macd_signal'] = macd_df.iloc[:, 2]
        df['macd_hist']   = macd_df.iloc[:, 1]
        df['ema_macd9']   = df['macd_line'].ewm(span=9, adjust=False).mean()

        # MACD（開盤價，可留可不留）
        macd_o_df = ta.macd(o, fast=12, slow=26, signal=9)
        df['macd_open_line'] = macd_o_df.iloc[:, 0]

        return df
    except Exception as e:
        print(f"❌ calc_indicators 錯誤 → {e}")
        return None

# ============================================================
# 【７-1．三指數融合（SPY / QQQ / HYG三合一追蹤【安聯月配息基金】）】
# ============================================================
def build_fund_proxy_df(df_spy, df_qqq, df_hyg):
    if any(d is None or d.empty for d in [df_spy, df_qqq, df_hyg]):
        return None
    try:
        # 處理 MultiIndex 並對齊時間軸
        for d in [df_spy, df_qqq, df_hyg]:
            if isinstance(d.columns, pd.MultiIndex): d.columns = d.columns.get_level_values(0)
        
        common_idx = df_spy.index.intersection(df_qqq.index).intersection(df_hyg.index)
        df_s, df_q, df_h = df_spy.loc[common_idx], df_qqq.loc[common_idx], df_hyg.loc[common_idx]

        df = df_s.copy()
        w_spy, w_qqq, w_hyg = 0.5, 0.3, 0.2

        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = (df_s[col].squeeze() * w_spy + 
                       df_q[col].squeeze() * w_qqq + 
                       df_h[col].squeeze() * w_hyg)
        df['Volume'] = df_s['Volume'].squeeze()
        return df
    except Exception as e:
        print(f"❌ build_fund_proxy_df 錯誤：{e}")
        return None
# ============================================================
# 【８．第一道門檻：買進前提條件執行】（引用第2-1章策略參數）
# 條件A：近N根任一最低價<=布林下緣 AND RSI上升 AND MACD柱放大
# 條件B：近N根Low均<布林中軌 AND 近N根High均<布林上軌
#         AND 前N根MACD柱持續縮小 AND 當根MACD柱放大
# 成交量完全不看（截圖是期貨口數）
# 🔥 適用於「週K篩選」、「5分K篩選」、「5分K實戰」（條件A or 條件B，任一成立即觸發）
# ⚙️  可調參數請至【２-1章策略參數設定區】修改，勿直接改這裡
# ============================================================
def check_buy_precondition(df):
    try:
        l   = df['Low']
        h   = df['High']
        rsi = df['rsi14']
        bb  = df['boll_bot20']   # 布林下軌
        bt  = df['boll_top20']   # 布林上軌
        bm  = df['ma_c_20']      # 布林中軌
        mh  = df['macd_hist']
        n   = BUY_LOOKBACK_BARS  # 引用第2-1章：回看根數

        # ── 條件A（截圖3、4）──────────────────────────────────
        # 1. 近N根(含當根)任一最低價 <= 布林下緣
        price_near_lower = (l.iloc[-n:] <= bb.iloc[-n:] * BUY_BOLL_TOLERANCE).any()
        # 2. 當根 RSI 上升
        rsi_rising       = float(rsi.iloc[-1]) > float(rsi.iloc[-2])
        # 3. 當根 MACD 柱上升
        macd_rising      = float(mh.iloc[-1])  > float(mh.iloc[-2])
        cond_A = price_near_lower and rsi_rising and macd_rising

        # ── 條件B（截圖3、4補充）──────────────────────────────
        # 1. 近N根最低價均低於布林中軌
        low_below_mid  = (l.iloc[-n:] < bm.iloc[-n:]).all()
        # 2. 近N根最高價均低於布林上軌
        high_below_top = (h.iloc[-n:] < bt.iloc[-n:]).all()
        # 3. 前N根~前1根MACD柱持續縮小（需至少N+1根資料）
        if len(mh) >= n + 1:
            macd_shrink = all(float(mh.iloc[-n-1+j]) > float(mh.iloc[-n+j]) for j in range(n-1))
        else:
            macd_shrink = False
        # 4. 當根MACD柱放大（當根 > 前1根）
        macd_expand    = float(mh.iloc[-1]) > float(mh.iloc[-2])
        cond_B = low_below_mid and high_below_top and macd_shrink and macd_expand

        return cond_A or cond_B
    except Exception as e:
        # print(f"Precondition Error: {e}") # 偵錯用
        return False

# ============================================================
# 【９．第二道門檻：eLeader 25個複合條件執行】（引用第2-2章策略參數）
# ⚙️  可調參數請至【２-2章策略參數設定區】修改，勿直接改這裡
# ============================================================
def check_buy_eleader(df_w, df_d=None, df_5m=None, name=None):
    """
    100% 保留 25 個複合條件邏輯，並加入多週期共振判定。
    """
    # 兼容性：如果沒傳日K，就用週K當主判斷
    df = df_d if df_d is not None else df_w
    
    try:
        if df is None or len(df) < 5: return None
        
        # --- 原始 25 個複合條件邏輯 (一字不漏) ---
        o = df['Open']; h = df['High']; l = df['Low']; c = df['Close']; v = df['Volume']
        ma_c2 = df['ma_c_2']; ma_c20 = df['ma_c_20']
        ma_o2 = df['ma_o_2']; ma_h2 = df['ma_h_2']
        ma_l2 = df['ma_l_2']; ma_v5 = df['ma_v_5']
        bt20 = df['boll_top20']; bb20 = df['boll_bot20']
        bt2 = df['boll_top2']; bb2 = df['boll_bot2']
        rsi = df['rsi14']; ersi = df['ema_rsi9']
        macd = df['macd_line']; msig = df['macd_signal']
        mosc = df['macd_hist']; emacd = df['ema_macd9']
        macd_o = df['macd_open_line']

        base = ((rsi.shift(1) < ELEADER_RSI_MAX) & (c > ma_l2) & (h > o) & (c > l))  # 引用第2-2章

        Cond01 = ((h.shift(1) > bb20.shift(1)) & (bt2.shift(1) < bt20.shift(1)) & (bb2 > bb20))
        Cond02 = (mosc > mosc.shift(1))
        Cond03 = (((ma_c20 > ma_c20.shift(1)) & (bt20 > bt20.shift(1)) & (rsi > rsi.shift(1))) |
                  ((ma_c20 < ma_c20.shift(1)) & ((bb20 < bb20.shift(1)) | ((ma_o2 < ma_o2.shift(1)) & (o > ma_o2))) &
                   (rsi.shift(2) < rsi.shift(3)) & (rsi.shift(1) > rsi.shift(2)) & (rsi > rsi.shift(1)) & (ersi.shift(1) > ersi.shift(2))))
        Cond04 = (((c.shift(1) > ma_c2.shift(1)) | (c.shift(1) > ma_c20.shift(1))) & (o < ma_c20.shift(1)))

        Cond11 = ((bt20.shift(1)<bt20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(rsi.shift(1)<ersi.shift(1))&(rsi>ersi)&(emacd.shift(1)>emacd.shift(2))&(ma_o2.shift(1)<ma_c20.shift(1))&(ma_o2.shift(1)<ma_o2.shift(2))&(ma_o2>ma_o2.shift(1))&(o.shift(1)>ma_o2.shift(1))&(l<ma_o2)&(c>ma_o2))
        Cond12 = ((bt20.shift(1)>bt20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(rsi.shift(1)<ersi.shift(1))&(rsi>ersi)&(emacd.shift(1)<emacd.shift(2))&(ma_o2.shift(1)<ma_c20.shift(1))&(ma_o2.shift(1)<ma_o2.shift(2))&(ma_o2>ma_o2.shift(1))&(o.shift(1)>ma_o2.shift(1))&(c>ma_o2))
        Cond21 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bt20<bt20.shift(1))&(bb20.shift(1)<bb20.shift(2))&(bb20<bb20.shift(1))&(rsi.shift(1)>rsi.shift(2))&(mosc>mosc.shift(1))&(ma_o2.shift(1)<ma_o2.shift(2))&(o.shift(1)>ma_o2.shift(1)))
        Cond22 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(bb20.shift(1)<bb20.shift(2))&(ersi.shift(1)>rsi.shift(1))&(emacd>macd)&(ma_o2.shift(1)<ma_c20.shift(1))&(ma_o2.shift(1)<ma_o2.shift(2))&(o.shift(1)>ma_o2.shift(1))&(ma_o2>ma_o2.shift(1))&(ma_l2>ma_l2.shift(1))&(c>ma_l2))
        Cond23 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(bt20>bt20.shift(1))&(macd.shift(1)<msig.shift(1))&(macd>macd.shift(1)))
        Cond24 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bt20.shift(1)>bt20.shift(2))&(bb20.shift(1)<bb20.shift(2))&(ma_o2<bb20)&(rsi.shift(1)>rsi.shift(2))&(mosc>mosc.shift(1))&(ma_o2<ma_o2.shift(1))&(o.shift(1)<ma_o2.shift(1))&(o>ma_o2))
        Cond25 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bt20.shift(1)>bt20.shift(2))&(bt20>bt20.shift(1))&(bb20.shift(1)<bb20.shift(2))&(c.shift(1)<bb20.shift(1))&(o<bb20.shift(1))&(mosc.shift(1)<mosc.shift(2))&(mosc>mosc.shift(1))&(c>ma_o2))
        Cond26 = ((ma_c20.shift(1)<ma_c20.shift(2))&(h.shift(1)<ma_c20.shift(1))&(bt20.shift(1)<bt20.shift(2))&(bb20>bb20.shift(1))&(rsi.shift(1)>ersi.shift(1))&(ersi.shift(1)>ersi.shift(2))&(macd.shift(1)>macd.shift(2))&(ma_o2>ma_o2.shift(1))&(c>ma_o2)&(c>o))
        Cond27 = ((ma_c20.shift(1)<ma_c20.shift(2))&(o.shift(1)<bb20.shift(1))&(macd_o<0)&(rsi.shift(1)<rsi.shift(2))&(ersi.shift(2)<35)&(ma_o2.shift(1)<bb20.shift(1))&(ma_o2<ma_o2.shift(1))&(c>l.shift(1)))
        Cond28 = ((ma_c20.shift(1)<ma_c20.shift(2))&(o.shift(1)<ma_c20.shift(1))&(c.shift(1)>ma_c20.shift(1))&(bt20.shift(1)<bt20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(bb20>bb20.shift(1))&(rsi.shift(1)>rsi.shift(2))&(ersi.shift(1)>ersi.shift(2))&(ma_o2.shift(1)<ma_c20.shift(1))&(ma_o2.shift(1)<ma_o2.shift(2))&(ma_o2>ma_o2.shift(1))&(o.shift(1)<ma_o2.shift(1))&(c>ma_o2)&(c>l))
        Cond41 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bb20>bb20.shift(1))&(bt20<bt20.shift(1))&(ersi.shift(1)>rsi.shift(1))&(emacd.shift(1)>macd.shift(1))&(o.shift(1)<ma_c20.shift(1))&(h.shift(1)>ma_c20.shift(1))&(ma_o2.shift(1)<ma_c20.shift(1))&(ma_o2<ma_o2.shift(1))&(o<ma_o2)&(c>ma_o2)&(l>=l.shift(1)))
        Cond42 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(o<bb20.shift(1))&(o.shift(1)>ma_c20.shift(1))&(ma_o2.shift(1)>l.shift(2))&(ma_o2<ma_o2.shift(1))&(c>ma_o2))
        Cond43 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(bt20>bt20.shift(1))&(macd.shift(1)>msig.shift(1))&(macd.shift(1)<macd.shift(2))&(mosc.shift(1)<mosc.shift(2))&(mosc>mosc.shift(1))&(ma_o2.shift(1)<ma_o2.shift(2))&(ma_o2>ma_o2.shift(1))&(c>o))
        Cond44 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(emacd.shift(1)>emacd.shift(2))&(o<ma_c20.shift(1))&(ma_o2>ma_c20.shift(1))&(ma_o2.shift(1)>ma_o2.shift(2))&(h<ma_o2)&(ma_l2.shift(1)>=ma_l2.shift(2))&(l<ma_l2)&(c>ma_l2))
        Cond45 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(emacd.shift(1)>macd.shift(1))&(ersi.shift(1)>rsi.shift(1))&(o.shift(1)<ma_o2.shift(1))&(rsi.shift(1)>rsi.shift(2))&(o>ma_o2)&(c.shift(1)>ma_o2.shift(1))&(ma_o2<ma_o2.shift(1))&(ma_l2.shift(1)<bb20.shift(1))&(c>o))
        Cond46 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bt20.shift(1)>bt20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(h.shift(1)>bt20.shift(1))&(c>bt20)&(c.shift(1)>ma_o2.shift(1))&(ma_o2>ma_o2.shift(1))&(o<ma_o2))
        Cond47 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bt20.shift(1)>bt20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(macd.shift(1)>msig.shift(1))&(macd.shift(1)<macd.shift(2))&(macd>macd.shift(1))&(ma_o2.shift(1)>ma_c20.shift(1))&(ma_o2<ma_o2.shift(1))&(c>ma_o2.shift(1)))
        Cond48 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bt20.shift(2)<bt20.shift(3))&(bt20.shift(1)<bt20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(rsi>rsi.shift(1))&(emacd.shift(1)>emacd.shift(2))&(ma_o2.shift(1)>ma_o2.shift(2))&(ma_o2>ma_o2.shift(1))&(c>ma_o2)&(c>o.shift(1)))
        Cond49 = ((ma_c20.shift(1)>ma_c20.shift(2))&(h.shift(2)<bt20.shift(2))&(bt20.shift(1)>bt20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(ersi>ersi.shift(1))&(macd.shift(1)>macd.shift(2))&(o.shift(1)>ma_o2.shift(1))&(o<ma_o2)&(c>ma_o2))
        Cond50 = ((ma_c20.shift(2)>ma_c20.shift(3))&(rsi>rsi.shift(1))&(ersi.shift(1)<ersi.shift(2))&(rsi<ersi)&(emacd.shift(1)<macd.shift(1))&(bt20.shift(2)>bt20.shift(3))&(bb20.shift(2)>bb20.shift(3))&(ma_o2.shift(1)<ma_o2.shift(2))&(o.shift(1)<ma_o2.shift(1))&(c>ma_o2))
        Cond61 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(ma_o2>ma_o2.shift(1))&(c.shift(2)<o.shift(2))&(c.shift(1)>o.shift(1))&(bb20.shift(1)>bb20.shift(2))&(bt2.shift(1)>bt2.shift(2))&(bb2.shift(1)<bb2.shift(2))&(ma_o2.shift(1)<ma_c20.shift(2))&(ma_o2.shift(1)<ma_o2.shift(2))&(ma_v5>ma_v5.shift(1))&(v>ma_v5)&(h>o))
        Cond62 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(ma_o2>ma_o2.shift(1))&(c.shift(1)<o.shift(1))&(o.shift(1)>ma_c20.shift(1))&(bb20.shift(1)<bb20.shift(2))&(ma_h2.shift(1)>ma_h2.shift(2))&(ma_l2.shift(1)>ma_l2.shift(2))&(c.shift(1)>=ma_c2.shift(1))&(l<o)&(c>o))
        Cond63 = ((ma_c20>ma_c20.shift(1))&(bb20>bb20.shift(1))&(rsi>rsi.shift(1))&(ersi.shift(1)<ersi.shift(2))&(ersi>ersi.shift(1))&(c>ma_o2.shift(1)))
        Cond66 = ((c.shift(1)>o.shift(1))&(ma_c20.shift(2)>ma_c20.shift(3))&(ma_c20.shift(1)>ma_c20.shift(2))&(l.shift(1)>ma_c20.shift(1))&(bt20.shift(1)>bt20.shift(2))&(bt2.shift(1)>bt20.shift(1))&(bb20<bb20.shift(1))&(o.shift(1)<bt20.shift(1))&(o<bt20)&(macd>msig)&(o.shift(1)<ma_o2.shift(1))&(c.shift(1)>ma_o2.shift(1))&(c.shift(1)>=ma_l2.shift(1))&(ma_l2>ma_l2.shift(1))&(c>o))
        Cond71 = ((c.shift(1)>o.shift(1))&(c.shift(2)<o.shift(2))&(bb2.shift(2)>bb20.shift(2))&(ma_l2.shift(2)>bb20.shift(2))&(bb20>bb20.shift(1))&(o.shift(1)<ma_c20.shift(1))&(((ma_o2>ma_o2.shift(1))&(h.shift(1)<ma_h2.shift(1))&(c>ma_c2))|((ma_o2<ma_o2.shift(1))&(l<ma_o2)&(c>ma_o2)))&(o>ma_o2)&(ma_l2>=ma_l2.shift(1))&(c.shift(1)>=ma_c2.shift(1)))

        group_A = (Cond01 & Cond02 & Cond03 & Cond04)
        group_B = (Cond11|Cond12|Cond21|Cond22|Cond23|Cond24|Cond25|Cond26|Cond27|Cond28|Cond41|Cond42|Cond43|Cond44|Cond45|Cond46|Cond47|Cond48|Cond49|Cond50|Cond61|Cond62|Cond63|Cond66|Cond71)

        is_buy_candidate = bool((base & (group_A | group_B)).iloc[-1])

        if is_buy_candidate:
            # 如果有 5分K 資料，進行精確進場判定
            if df_5m is not None and len(df_5m) >= 2:
                # 【防閃退檢查】確保 5分K 的 RSI 欄位存在且不是空值(NaN)
                if 'rsi14' not in df_5m.columns:
                    df_5m['rsi14'] = ta.rsi(df_5m['Close'].squeeze(), length=14)
                
                if pd.isna(df_5m['rsi14'].iloc[-1]) or pd.isna(df_5m['rsi14'].iloc[-2]):
                    return None # 資料不足，不貿然發動
                
                last_5m_rsi = float(df_5m['rsi14'].iloc[-1])
                prev_5m_rsi = float(df_5m['rsi14'].iloc[-2])
                
                # 改為「轉折向上」才進場，並回傳 5分K 的 RSI 給 Email 顯示
                if last_5m_rsi > prev_5m_rsi:
                    return ('BUY', c.iloc[-1], prev_5m_rsi, last_5m_rsi)
                else:
                    return None
            
            # 若沒有 5分K (單純預篩階段)，直接回傳日/週K的數值
            return ('BUY', c.iloc[-1], bb20.iloc[-1], rsi.iloc[-1])
        return None

    except Exception as e:
        if name: print(f"❌ check_buy_eleader 錯誤({name}): {e}")
        return None

# ============================================================
# 【１０．賣出/平倉條件執行】（引用第2-3章策略參數）
# 最高價 >= 布林上緣 AND RSI下降 AND MACD柱縮小
# ⚙️  可調參數請至【２-3章策略參數設定區】修改，勿直接改這裡
# ============================================================
def check_sell_condition(df):
    try:
        h   = df['High']
        rsi = df['rsi14']
        bt  = df['boll_top20']
        mh  = df['macd_hist']

        price_near_upper = float(h.iloc[-1])   >= float(bt.iloc[-1]) * SELL_BOLL_TOLERANCE  # 引用第2-3章
        rsi_falling      = float(rsi.iloc[-1]) <  float(rsi.iloc[-2])
        macd_falling     = float(mh.iloc[-1])  <  float(mh.iloc[-2])

        return price_near_upper and rsi_falling and macd_falling
    except:
        return False

# ============================================================
# 【１０-２．做空策略：三道關卡（買進策略完全鏡像）】
# 第一道：週K觸碰上軌（mirror of 觸碰下軌）
# 第二道：日K eLeader 25條件全部反向
# 第三道：5分K RSI↓ AND MACD柱↓
# ============================================================
def check_short_precondition(df):
    """做空第一道：週K位階高檔（買進策略完全鏡像）"""
    try:
        l   = df['Low'];  h   = df['High']
        rsi = df['rsi14']
        bb  = df['boll_bot20']; bt  = df['boll_top20']; bm  = df['ma_c_20']
        mh  = df['macd_hist']
        n   = BUY_LOOKBACK_BARS

        # 鏡像條件A：近N根任一最高價 >= 布林上軌 AND RSI↓ AND MACD柱↓
        price_near_upper = (h.iloc[-n:] >= bt.iloc[-n:] * SELL_BOLL_TOLERANCE).any()
        rsi_falling      = float(rsi.iloc[-1]) < float(rsi.iloc[-2])
        macd_falling     = float(mh.iloc[-1])  < float(mh.iloc[-2])
        cond_A = price_near_upper and rsi_falling and macd_falling

        # 鏡像條件B：近N根最高均>布林中軌 AND 最低均>布林下軌 AND 前N根MACD柱持續放大 AND 當根縮小
        high_above_mid = (h.iloc[-n:] > bm.iloc[-n:]).all()
        low_above_bot  = (l.iloc[-n:] > bb.iloc[-n:]).all()
        if len(mh) >= n + 1:
            macd_expand = all(float(mh.iloc[-n-1+j]) < float(mh.iloc[-n+j]) for j in range(n-1))
        else:
            macd_expand = False
        macd_shrink = float(mh.iloc[-1]) < float(mh.iloc[-2])
        cond_B = high_above_mid and low_above_bot and macd_expand and macd_shrink

        return cond_A or cond_B
    except:
        return False

def check_short_eleader(df_d):
    """做空第二道：日K eLeader 25條件全部反向（多頭訊號→空頭訊號）"""
    try:
        if df_d is None or len(df_d) < 5: return None
        df = df_d

        o=df['Open']; h=df['High']; l=df['Low']; c=df['Close']; v=df['Volume']
        ma_c2=df['ma_c_2']; ma_c20=df['ma_c_20']
        ma_o2=df['ma_o_2']; ma_h2=df['ma_h_2']
        ma_l2=df['ma_l_2']; ma_v5=df['ma_v_5']
        bt20=df['boll_top20']; bb20=df['boll_bot20']
        bt2=df['boll_top2']; bb2=df['boll_bot2']
        rsi=df['rsi14']; ersi=df['ema_rsi9']
        macd=df['macd_line']; msig=df['macd_signal']
        mosc=df['macd_hist']; emacd=df['ema_macd9']
        macd_o=df['macd_open_line']

        # 做空基底條件（eLeader買進base的完全鏡像）
        # 原: RSI前根<RSI_MAX AND 收>ma_l2 AND High>Open AND Close>Low
        # 反: RSI前根>RSI_MAX AND 收<ma_h2 AND Low<Open AND Close<High
        short_base = ((rsi.shift(1) > (100 - ELEADER_RSI_MAX)) & (c < ma_h2) & (l < o) & (c < h))

        # 全部25個條件完全反向（> 改 <，< 改 >，方向全反）
        SC01 = ((l.shift(1) < bb20.shift(1)) & (bb2.shift(1) > bb20.shift(1)) & (bt2 < bt20))
        SC02 = (mosc < mosc.shift(1))
        SC03 = (((ma_c20 < ma_c20.shift(1)) & (bb20 < bb20.shift(1)) & (rsi < rsi.shift(1))) |
                ((ma_c20 > ma_c20.shift(1)) & ((bt20 > bt20.shift(1)) | ((ma_o2 > ma_o2.shift(1)) & (o < ma_o2))) &
                 (rsi.shift(2) > rsi.shift(3)) & (rsi.shift(1) < rsi.shift(2)) & (rsi < rsi.shift(1)) & (ersi.shift(1) < ersi.shift(2))))
        SC04 = (((c.shift(1) < ma_c2.shift(1)) | (c.shift(1) < ma_c20.shift(1))) & (o > ma_c20.shift(1)))

        SC11 = ((bb20.shift(1)>bb20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(rsi.shift(1)>ersi.shift(1))&(rsi<ersi)&(emacd.shift(1)<emacd.shift(2))&(ma_o2.shift(1)>ma_c20.shift(1))&(ma_o2.shift(1)>ma_o2.shift(2))&(ma_o2<ma_o2.shift(1))&(o.shift(1)<ma_o2.shift(1))&(h>ma_o2)&(c<ma_o2))
        SC12 = ((bb20.shift(1)<bb20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(rsi.shift(1)>ersi.shift(1))&(rsi<ersi)&(emacd.shift(1)>emacd.shift(2))&(ma_o2.shift(1)>ma_c20.shift(1))&(ma_o2.shift(1)>ma_o2.shift(2))&(ma_o2<ma_o2.shift(1))&(o.shift(1)<ma_o2.shift(1))&(c<ma_o2))
        SC21 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bb20>bb20.shift(1))&(bt20.shift(1)>bt20.shift(2))&(bt20>bt20.shift(1))&(rsi.shift(1)<rsi.shift(2))&(mosc<mosc.shift(1))&(ma_o2.shift(1)>ma_o2.shift(2))&(o.shift(1)<ma_o2.shift(1)))
        SC22 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(bt20.shift(1)>bt20.shift(2))&(ersi.shift(1)<rsi.shift(1))&(emacd<macd)&(ma_o2.shift(1)>ma_c20.shift(1))&(ma_o2.shift(1)>ma_o2.shift(2))&(o.shift(1)<ma_o2.shift(1))&(ma_o2<ma_o2.shift(1))&(ma_h2<ma_h2.shift(1))&(c<ma_h2))
        SC23 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(bb20<bb20.shift(1))&(macd.shift(1)>msig.shift(1))&(macd<macd.shift(1)))
        SC24 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bb20.shift(1)<bb20.shift(2))&(bt20.shift(1)>bt20.shift(2))&(ma_o2>bt20)&(rsi.shift(1)<rsi.shift(2))&(mosc<mosc.shift(1))&(ma_o2>ma_o2.shift(1))&(o.shift(1)>ma_o2.shift(1))&(o<ma_o2))
        SC25 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bb20.shift(1)<bb20.shift(2))&(bb20<bb20.shift(1))&(bt20.shift(1)>bt20.shift(2))&(c.shift(1)>bt20.shift(1))&(o>bt20.shift(1))&(mosc.shift(1)>mosc.shift(2))&(mosc<mosc.shift(1))&(c<ma_o2))
        SC26 = ((ma_c20.shift(1)>ma_c20.shift(2))&(l.shift(1)<bb20.shift(1))&(bb20.shift(1)>bb20.shift(2))&(bt20<bt20.shift(1))&(rsi.shift(1)<ersi.shift(1))&(ersi.shift(1)<ersi.shift(2))&(macd.shift(1)<macd.shift(2))&(ma_o2<ma_o2.shift(1))&(c<ma_o2)&(c<o))
        SC27 = ((ma_c20.shift(1)>ma_c20.shift(2))&(o.shift(1)>bt20.shift(1))&(macd_o>0)&(rsi.shift(1)>rsi.shift(2))&(ersi.shift(2)>65)&(ma_o2.shift(1)>bt20.shift(1))&(ma_o2>ma_o2.shift(1))&(c<h.shift(1)))
        SC28 = ((ma_c20.shift(1)>ma_c20.shift(2))&(o.shift(1)>ma_c20.shift(1))&(c.shift(1)<ma_c20.shift(1))&(bb20.shift(1)>bb20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(bt20<bt20.shift(1))&(rsi.shift(1)<rsi.shift(2))&(ersi.shift(1)<ersi.shift(2))&(ma_o2.shift(1)>ma_c20.shift(1))&(ma_o2.shift(1)>ma_o2.shift(2))&(ma_o2<ma_o2.shift(1))&(o.shift(1)>ma_o2.shift(1))&(c<ma_o2)&(c<h))
        SC41 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bt20<bt20.shift(1))&(bb20>bb20.shift(1))&(ersi.shift(1)<rsi.shift(1))&(emacd.shift(1)<macd.shift(1))&(o.shift(1)>ma_c20.shift(1))&(l.shift(1)<ma_c20.shift(1))&(ma_o2.shift(1)>ma_c20.shift(1))&(ma_o2>ma_o2.shift(1))&(o>ma_o2)&(c<ma_o2)&(h<=h.shift(1)))
        SC42 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(o>bt20.shift(1))&(o.shift(1)<ma_c20.shift(1))&(ma_o2.shift(1)<h.shift(2))&(ma_o2>ma_o2.shift(1))&(c<ma_o2))
        SC43 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(bb20<bb20.shift(1))&(macd.shift(1)<msig.shift(1))&(macd.shift(1)>macd.shift(2))&(mosc.shift(1)>mosc.shift(2))&(mosc<mosc.shift(1))&(ma_o2.shift(1)>ma_o2.shift(2))&(ma_o2<ma_o2.shift(1))&(c<o))
        SC44 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(emacd.shift(1)<emacd.shift(2))&(o>ma_c20.shift(1))&(ma_o2<ma_c20.shift(1))&(ma_o2.shift(1)<ma_o2.shift(2))&(h>ma_o2)&(ma_h2.shift(1)<=ma_h2.shift(2))&(h>ma_h2)&(c<ma_h2))
        SC45 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(emacd.shift(1)<macd.shift(1))&(ersi.shift(1)<rsi.shift(1))&(o.shift(1)>ma_o2.shift(1))&(rsi.shift(1)<rsi.shift(2))&(o<ma_o2)&(c.shift(1)<ma_o2.shift(1))&(ma_o2>ma_o2.shift(1))&(ma_h2.shift(1)>bt20.shift(1))&(c<o))
        SC46 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(bb20.shift(1)<bb20.shift(2))&(l.shift(1)<bb20.shift(1))&(c<bb20)&(c.shift(1)<ma_o2.shift(1))&(ma_o2<ma_o2.shift(1))&(o>ma_o2))
        SC47 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(bb20.shift(1)<bb20.shift(2))&(macd.shift(1)<msig.shift(1))&(macd.shift(1)>macd.shift(2))&(macd<macd.shift(1))&(ma_o2.shift(1)<ma_c20.shift(1))&(ma_o2>ma_o2.shift(1))&(c<ma_o2.shift(1)))
        SC48 = ((ma_c20.shift(1)<ma_c20.shift(2))&(bt20.shift(2)>bt20.shift(3))&(bt20.shift(1)>bt20.shift(2))&(bb20.shift(1)<bb20.shift(2))&(rsi<rsi.shift(1))&(emacd.shift(1)<emacd.shift(2))&(ma_o2.shift(1)<ma_o2.shift(2))&(ma_o2<ma_o2.shift(1))&(c<ma_o2)&(c<o.shift(1)))
        SC49 = ((ma_c20.shift(1)<ma_c20.shift(2))&(l.shift(2)>bb20.shift(2))&(bb20.shift(1)<bb20.shift(2))&(bt20.shift(1)<bt20.shift(2))&(ersi<ersi.shift(1))&(macd.shift(1)<macd.shift(2))&(o.shift(1)<ma_o2.shift(1))&(o>ma_o2)&(c<ma_o2))
        SC50 = ((ma_c20.shift(2)<ma_c20.shift(3))&(rsi<rsi.shift(1))&(ersi.shift(1)>ersi.shift(2))&(rsi>ersi)&(emacd.shift(1)>macd.shift(1))&(bt20.shift(2)<bt20.shift(3))&(bb20.shift(2)<bb20.shift(3))&(ma_o2.shift(1)>ma_o2.shift(2))&(o.shift(1)>ma_o2.shift(1))&(c<ma_o2))
        SC61 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(ma_o2<ma_o2.shift(1))&(c.shift(2)>o.shift(2))&(c.shift(1)<o.shift(1))&(bt20.shift(1)<bt20.shift(2))&(bb2.shift(1)<bb2.shift(2))&(bt2.shift(1)>bt2.shift(2))&(ma_o2.shift(1)>ma_c20.shift(2))&(ma_o2.shift(1)>ma_o2.shift(2))&(ma_v5>ma_v5.shift(1))&(v>ma_v5)&(l<o))
        SC62 = ((ma_c20.shift(1)>ma_c20.shift(2))&(bb20.shift(1)>bb20.shift(2))&(ma_o2<ma_o2.shift(1))&(c.shift(1)>o.shift(1))&(o.shift(1)<ma_c20.shift(1))&(bt20.shift(1)>bt20.shift(2))&(ma_l2.shift(1)<ma_l2.shift(2))&(ma_h2.shift(1)<ma_h2.shift(2))&(c.shift(1)<=ma_c2.shift(1))&(h>o)&(c<o))
        SC63 = ((ma_c20<ma_c20.shift(1))&(bt20<bt20.shift(1))&(rsi<rsi.shift(1))&(ersi.shift(1)>ersi.shift(2))&(ersi<ersi.shift(1))&(c<ma_o2.shift(1)))
        SC66 = ((c.shift(1)<o.shift(1))&(ma_c20.shift(2)<ma_c20.shift(3))&(ma_c20.shift(1)<ma_c20.shift(2))&(h.shift(1)<ma_c20.shift(1))&(bb20.shift(1)<bb20.shift(2))&(bb2.shift(1)<bb20.shift(1))&(bt20>bt20.shift(1))&(o.shift(1)>bb20.shift(1))&(o>bb20)&(macd<msig)&(o.shift(1)>ma_o2.shift(1))&(c.shift(1)<ma_o2.shift(1))&(c.shift(1)<=ma_h2.shift(1))&(ma_h2<ma_h2.shift(1))&(c<o))
        SC71 = ((c.shift(1)<o.shift(1))&(c.shift(2)>o.shift(2))&(bt2.shift(2)<bt20.shift(2))&(ma_h2.shift(2)<bt20.shift(2))&(bt20<bt20.shift(1))&(o.shift(1)>ma_c20.shift(1))&(((ma_o2<ma_o2.shift(1))&(l.shift(1)>ma_l2.shift(1))&(c<ma_c2))|((ma_o2>ma_o2.shift(1))&(h>ma_o2)&(c<ma_o2)))&(o<ma_o2)&(ma_h2<=ma_h2.shift(1))&(c.shift(1)<=ma_c2.shift(1)))

        short_group_A = (SC01 & SC02 & SC03 & SC04)
        short_group_B = (SC11|SC12|SC21|SC22|SC23|SC24|SC25|SC26|SC27|SC28|SC41|SC42|SC43|SC44|SC45|SC46|SC47|SC48|SC49|SC50|SC61|SC62|SC63|SC66|SC71)

        is_short = bool((short_base & (short_group_A | short_group_B)).iloc[-1])
        if is_short:
            return ('SHORT', c.iloc[-1], bt20.iloc[-1], rsi.iloc[-1])
        return None
    except Exception as e:
        return None

# ============================================================
# 【１１．主掃描函數：scan_stock（引用第2章策略參數）】
# ============================================================
def scan_stock(ticker, is_holding=False):
    global weekly_cache, daily_cache
    
    # ✅ [新增: 5分K 專用短效快取, 避免 30 分鐘內重複抓取過多 API]
    # 使用變數名 five_min_cache (請確保在程式開頭 global 宣告過)
    global five_min_cache 
    if 'five_min_cache' not in globals(): five_min_cache = {}

    try:
        # 0. 【最高優先級①】全額交割股預警
        _code_only = ticker.replace('.TW','').replace('.TWO','')
        _cash_set  = get_cash_delivery_set()
        if _cash_set and _code_only in _cash_set:
            _msg = f'⚠️ 列為全額交割股（代碼：{_code_only}），財務惡化警訊，下市前最重要早期指標！'
            if is_holding:
                return ('DELIST_HOLD', _msg)
            else:
                return ('DELIST_WATCH', _msg)

        # 0. 【最高優先級②】下市風險預警（Yahoo Finance確認）
        is_at_risk, risk_msg = get_delisting_risk(ticker)
        if is_at_risk:
            if is_holding:
                return ('DELIST_HOLD', risk_msg)
            else:
                return ('DELIST_WATCH', risk_msg)

        # ══════════════════════════════════════════════════════════
        # 【三道關卡】依 SCAN_MODE 選擇對應驗證邏輯
        # SCAN_MODE='weekly'：週K三道（週K→日K→5分K）
        # SCAN_MODE='daily' ：日K三道（週K→日K→5分K，日K為第二道）
        # ══════════════════════════════════════════════════════════

        # ── 共用：抓取週K（第一道 or 賣出判斷用）────────────────
        df_w = get_stock_data(ticker, period='2y', interval='1wk', cache=weekly_cache)
        if df_w is None or len(df_w) < 30: return None
        df_w['boll_top20'], df_w['boll_mid20'], df_w['boll_bot20'] = ta.bbands(df_w['Close'], length=20).iloc[:, 0:3].values.T
        df_w['rsi14'] = ta.rsi(df_w['Close'], length=14)

        # 🔴 [優先處理賣出]
        if is_holding:
            if check_sell_condition(df_w):
                c_price = float(df_w['Close'].iloc[-1])
                return ('SELL', c_price, float(df_w['High'].iloc[-1]), float(df_w['boll_top20'].iloc[-1]),
                        float(df_w['rsi14'].iloc[-1]), float(df_w['rsi14'].iloc[-2]))
            return None

        # ── 第一道：週K位階（週K/日K模式共用）──────────────────
        if not TEST_MODE:
            if not check_buy_precondition(df_w):
                return None
        else:
            print(f"🧪 {ticker} 正在進行【驗證篩選】測試中...")

        # ── 第二道：依SCAN_MODE選擇日K或eLeader ─────────────────
        df_d = get_stock_data(ticker, period='6mo', interval='1d', cache=daily_cache)
        if df_d is None or len(df_d) < 50: return None
        df_d = calc_indicators(df_d)
        if df_d is None: return None

        if SCAN_MODE == 'daily':
            # 日K模式第二道：日K 15根條件A/B（BUY_LOOKBACK_DAILY）
            globals()['BUY_LOOKBACK_BARS'] = BUY_LOOKBACK_DAILY
            _2nd_ok = check_buy_precondition(df_d)
            globals()['BUY_LOOKBACK_BARS'] = 3
            if not _2nd_ok:
                return None
        else:
            # 週K模式第二道：日K eLeader 25條件
            is_eleader_ok = check_buy_eleader(df_d) is not None
            if not is_eleader_ok:
                return None

        # ── 第三道：5分K即時轉折（週K/日K模式共用）─────────────
        now_ts = time.time()
        cache_key = f"5m_{ticker}"
        if cache_key in five_min_cache and (now_ts - five_min_cache[cache_key]['ts'] < 300):
            df_5m = five_min_cache[cache_key]['df']
        else:
            df_5m = yf.download(ticker, period='5d', interval='5m', progress=False)
            if df_5m is not None and not df_5m.empty and len(df_5m) >= 10:
                df_5m['rsi14'] = ta.rsi(df_5m['Close'].squeeze(), length=14)
                _macd_5m = ta.macd(df_5m['Close'].squeeze(), fast=12, slow=26, signal=9)
                df_5m['macd_hist'] = _macd_5m.iloc[:, 1]
                five_min_cache[cache_key] = {'df': df_5m, 'ts': now_ts}

        if df_5m is None or len(df_5m) < 2 or 'rsi14' not in df_5m.columns: return None

        last_rsi  = float(df_5m['rsi14'].iloc[-1])
        prev_rsi  = float(df_5m['rsi14'].iloc[-2])
        try:
            last_macd = float(df_5m['macd_hist'].iloc[-1])
            prev_macd = float(df_5m['macd_hist'].iloc[-2])
            macd_5m_ok = (last_macd > prev_macd)
        except:
            macd_5m_ok = False

        rsi_5m_ok = (last_rsi > prev_rsi and last_rsi > BUY_RSI_MIN)

        if SCAN_MODE == 'daily':
            if not (rsi_5m_ok and macd_5m_ok):
                return None
            c = float(df_5m['Close'].iloc[-1])
            print(f"🔥 {ticker} 觸發【日K三道關卡】，成交價：{c}")
            return ('BUY', c, float(df_d['Low'].iloc[-1]), float(df_d['boll_bot20'].iloc[-1]),
                    float(df_d['rsi14'].iloc[-1]), float(df_d['rsi14'].iloc[-2]))
        else:
            route_A = rsi_5m_ok
            route_B = rsi_5m_ok and macd_5m_ok
            if route_A or route_B:
                c = float(df_5m['Close'].iloc[-1])
                trigger = 'eLeader+5分K' if route_A else '截圖條件5分K'
                print(f"🔥 {ticker} 觸發【{trigger}】，成交價：{c}")
                return ('BUY', c, float(df_w['Low'].iloc[-1]), float(df_w['boll_bot20'].iloc[-1]),
                        float(df_w['rsi14'].iloc[-1]), float(df_w['rsi14'].iloc[-2]))
 
    except Exception as e:
        # 靜默跳過錯誤
        return None
    return None
# ============================================================
# 【１１-1．虛擬基金專屬掃描（合成模式）- 3.0 正式版】
# ============================================================
def scan_synthetic_fund(fund_name="安聯月配息基金(合成代標)"):
    global buy_signals, sell_signals
    try:
        print(f"\n🚀 正在啟動合成追蹤：{fund_name}...")
        # 1. 週K (位階門檻)
        s_w = yf.download("SPY", period='2y', interval='1wk', progress=False)
        q_w = yf.download("QQQ", period='2y', interval='1wk', progress=False)
        h_w = yf.download("HYG", period='2y', interval='1wk', progress=False)
        df_w = calc_indicators(build_fund_proxy_df(s_w, q_w, h_w))
        if df_w is None or not check_buy_precondition(df_w):
            print(f"ℹ️ {fund_name}:週K位階尚未符合觸發買進條件")
            return

        # 2. 日K (趨勢判斷)
        s_d = yf.download("SPY", period='1y', interval='1d', progress=False)
        q_d = yf.download("QQQ", period='1y', interval='1d', progress=False)
        h_d = yf.download("HYG", period='1y', interval='1d', progress=False)
        df_d = calc_indicators(build_fund_proxy_df(s_d, q_d, h_d))

        # 3. 5分K (時機偵測 - 確保 RSI 與股票邏輯一致)
        s_5m = yf.download("SPY", period='5d', interval='5m', progress=False)
        q_5m = yf.download("QQQ", period='5d', interval='5m', progress=False)
        h_5m = yf.download("HYG", period='5d', interval='5m', progress=False)
        df_5m = build_fund_proxy_df(s_5m, q_5m, h_5m)
        if df_5m is not None:
            # 強制使用 pandas_ta 計算 RSI 以確保與股票對齊
            df_5m['rsi14'] = ta.rsi(df_5m['Close'].squeeze(), length=14)

        # ✅ 核心呼叫：傳入 4 個參數，觸發多維判定
        result = check_buy_eleader(df_w, df_d, df_5m, fund_name)
        
        if result and result[0] == 'BUY':
            # --- [修正 BUG：補足 7 個變數並存入清單以對齊第 14 章節的 unpack 需求, 由第 14 章彙整發信] ---
            # result 內容為 ('BUY', 當前價, prev_rsi, last_rsi)
            c_price = result[1]
            r_prev  = result[2]
            r_now   = result[3]
            l_val   = float(df_w['Low'].iloc[-1])       # 補上週K最低價
            bb_val  = float(df_w['boll_bot20'].iloc[-1])# 補上布林下緣
            
            # 精準存入 7 個變數：market, code, c, l, bb, r, rp
            buy_signals.append(('基金', fund_name, c_price, l_val, bb_val, r_now, r_prev))
            
            # --- [修正 BUG：send_gmail 內文必須為格式化字串，不可傳入 Tuple] ---
            msg_body = (
                f"⭐【基金買進訊號】⭐\n"
                f"市場：基金　代碼：{fund_name}\n"
                f"收盤價：{c_price:.2f}\n"
                f"RSI轉折：{r_prev:.1f} → {r_now:.1f}\n"
            )
            send_gmail(f"☁️【雲端】🔔基金買進訊號：{fund_name}", msg_body)
            print(f"✅ {fund_name} 已發送觸發買進訊號，已加入今日彙整清單！")
        else:
            print(f"ℹ️ {fund_name}：目前尚未共振達標。")

    except Exception as e:
        print(f"❌ scan_synthetic_fund 異常：{e}")
# ============================================================
# 【１２．防止觸發條件時反覆收到gmail通知】
# ============================================================
notified = load_notified()
today = datetime.now().strftime("%Y-%m-%d")

if today not in notified:
    notified = {today: []}
# ============================================================
# 【１３．主程式。邏輯：執行單次掃描】
# ============================================================
def main_task():
    # 🔥 宣告 global 確保全域共用
    global weekly_cache, daily_cache, buy_signals, sell_signals, delist_signals

    # ✅ [修正: 每次掃描開始前必須清空當次訊號, 避免重複累加發信]
    buy_signals   = []
    sell_signals  = []
    delist_signals = []

    tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(tz)
    now_str = now.strftime('%Y/%m/%d %H:%M')
    now_hour = now.hour

    # ✅ 每天早上 8 點且快取有東西時，執行清空快取
    if now_hour == WEEKLY_REFRESH_HOUR and len(weekly_cache) > 0:
        print(f"♻️ [{now_str}] 偵測到新的一天, 正在清除快取資料以獲取最新K線...")
        weekly_cache.clear()
        daily_cache.clear()

    # ✅ 主流程一定要執行(不能縮在 if 裡)
    active_markets = get_active_markets()

    # (以下請確保第 13, 14 章的掃描與發信代碼, 全部都要縮排在 def main_task 之下)
    print(f"\n{'='*55}")
    print(f"  股票買進賣出建議系統啟動（{"期貨5分K模式" if TEST_MODE == chr(39)+"5mk"+chr(39) else f'{SCAN_MODE}模式'}）")
    print(f"  掃描時間：{now_str}")
    print(f"  本次掃描市場：{', '.join(active_markets)}")
    print(f"{'='*55}")

    if ENABLE_CASH_DELIVERY_CHECK:
        print(f"\n🔍 正在更新全額交割股清單...")
        get_cash_delivery_set()

# ============================================================
# 【１４．台股、美股、虛擬幣掃描】（引用第2-5章大盤過濾策略）
# ============================================================
    # ── 台股掃描 ──────────────────────────────────
    tw_list = []
    if TEST_MODE == '5mk':
        print('\n📊 台股：期貨5分K模式，跳過台股掃描')
    elif 'TW' not in active_markets:
        print('\n📊 台股：非交易時段，跳過')
    else:
        # 🟢 大盤週K守門員（引用第2-5章ENABLE_INDEX_FILTER）
        if ENABLE_INDEX_FILTER:
            print('\n🔍 正在檢查台股大盤位階 (^TWII)...')
        try:
            tse_w = yf.download('^TWII', period='2y', interval='1wk', progress=False)
            if tse_w is not None and len(tse_w) >= 30:
                tse_w = calc_indicators(tse_w)
                last_w = tse_w.iloc[-1]
                prev_w = tse_w.iloc[-2]

                # 判定轉折點 (對應截圖箭頭下彎)
                tse_rsi_down = last_w['rsi14'] < prev_w['rsi14']
                tse_macd_down = last_w['macd_hist'] < prev_w['macd_hist']
                
                if tse_rsi_down and tse_macd_down:
                    print(f"⚠️ 警告：台灣加權指數(大盤)週K出現轉折下彎 (RSI:{last_w['rsi14']:.1f})，謹慎觀望，不建議進場")
                else:
                    print(f"✅ 台灣加權指數(大盤)週K趨勢尚穩 (RSI:{last_w['rsi14']:.1f})，持續觀察等個股訊號觸發後，再決定進場")
# 大盤✅尚穩 → 繼續等個股的3層條件（週K+日K+5分K）全部觸發
#                       ↓ 才發Gmail通知您
#                       ↓ 您再決定是否進場

# 大盤⚠️下彎 → 即使個股觸發訊號，也要提高警覺，
#               不建議大量進場，等大盤回穩再說
        except Exception as e:
            print(f"⚠️ 大盤分析失敗: {e}")

        # --- 台股清單抓取與上市櫃區分邏輯 ---
        try:
            import twstock
            try:
                twstock.update_codes()
            except:
                pass
            
            # 排除確定抓不到資料的黑名單
            BLACKLIST = ['1701','2443', '2809', '2888', '3202',  '3687', '4945', '5383', '5468', '6287','6288',  '6404', '6457', '6514', '6589', '6747', '6785', '8420']
            
            valid_codes = [c for c in twstock.codes.keys()
                           if str(c).isdigit() and len(str(c)) == 4 and c not in BLACKLIST]
            
            tw_list = []
            for c in valid_codes:
                info = twstock.codes.get(c)
                # 根據 twstock 市場屬性加上正確的 Yahoo 後綴
                if info and info.market == '上櫃':
                    tw_list.append(c + '.TWO')
                else:
                    tw_list.append(c + '.TW')
                    
            print(f'  twstock載入成功：共 {len(tw_list)} 支')
        except Exception as e:
            print(f'⚠️ twstock載入失敗（{e}），使用備用清單')
            backup = ['2330','2317','2454','2412','2308','2382','2303','2881','2882']
            tw_list = [c + '.TW' for c in backup]

        # 這裡要兼容 .TW 和 .TWO
        holdings_tw_full = [c + '.TW' for c in HOLDINGS_TW] + [c + '.TWO' for c in HOLDINGS_TW]
        total_tw = len(tw_list)
        print(f'\n📊 台股掃描：共{total_tw}支')
    
        for i, ticker in enumerate(tw_list):
            if (i+1) % 50 == 0:
                print(f'  進度：{i+1}/{total_tw}...')
        
            # --- [優化: 加入 try...except 容錯, 避免網路閃斷中斷掃描] ---
            try:
                is_holding = ticker in holdings_tw_full
                result = scan_stock(ticker, is_holding)
            
                if result:
                    code = ticker.split('.')[0] 
                    if result[0] == 'BUY':
                        buy_signals.append(('台股', code, *result[1:]))
                    elif result[0] == 'SELL':
                        sell_signals.append(('台股', code, *result[1:]))
                    elif result[0] in ('DELIST_HOLD', 'DELIST_WATCH'):
                        delist_signals.append(('台股', code, result[0], result[1]))
            except Exception as e:
            # 發生錯誤時僅列印警告並跳過該支股票
                print(f'  ⚠️ 跳過 {ticker} 掃描異常: {e}')
                continue
            time.sleep(0.1) # 稍微加快掃描速度

# ── 美股掃描（加入道瓊週K過濾，對應截圖轉折邏輯） ──────────
    if TEST_MODE == '5mk':
        print('\n📊 美股：期貨5分K模式，跳過美股掃描')
    elif 'US' not in active_markets:
        print('\n📊 美股：非交易時段，跳過')
    else:
        # 🔵 美股大盤守門員（引用第2-5章ENABLE_INDEX_FILTER）
        if ENABLE_INDEX_FILTER:
            print('\n🔍 正在檢查道瓊指數位階 (^DJI)...')
        try:
            dji_w = yf.download('^DJI', period='2y', interval='1wk', progress=False)
            if dji_w is not None and len(dji_w) >= 30:
                dji_w = calc_indicators(dji_w)
                last_dj = dji_w.iloc[-1]
                prev_dj = dji_w.iloc[-2]

                # 判定轉折點 (對應截圖中的指標下彎)
                dji_rsi_down = last_dj['rsi14'] < prev_dj['rsi14']
                dji_macd_down = last_dj['macd_hist'] < prev_dj['macd_hist']

                if dji_rsi_down or dji_macd_down:
                    print(f"⚠️ 警告：道瓊週K出現轉折下彎 (RSI:{last_dj['rsi14']:.1f})，謹慎觀望，不建議進場")
                else:
                    print(f"✅ 道瓊週K趨勢尚穩 (RSI:{last_dj['rsi14']:.1f})，持續觀察等個股訊號觸發後，再決定進場")
# 道瓊✅尚穩 → 繼續等個股的3層條件（週K+日K+5分K）全部觸發
#                       ↓ 才發Gmail通知您
#                       ↓ 您再決定是否進場

# 道瓊⚠️下彎 → 即使個股觸發訊號，也要提高警覺，
#               不建議進場，等大盤回穩再說
        except Exception as e:
            print(f"⚠️ 美股大盤分析失敗: {e}")

        print(f'\n📊 美股掃描：共{len(US_STOCKS)}支')
        for ticker in US_STOCKS:
            is_holding = ticker in HOLDINGS_US
            result = scan_stock(ticker, is_holding)
            
            # ✅ 正確：result 取得後立刻判斷
            if result:
                if result[0] == 'BUY':
                    buy_signals.append(('美股', ticker, *result[1:]))
                elif result[0] == 'SELL':
                    sell_signals.append(('美股', ticker, *result[1:]))
                elif result[0] in ('DELIST_HOLD', 'DELIST_WATCH'):
                    delist_signals.append(('美股', ticker, result[0], result[1]))
            time.sleep(0.1)

        # 🔥 正確位置：美股「所有股票」掃描完後，才執行「一次」基金追蹤
        # 確保此行與 for 垂直對齊（不縮排進去）
        scan_synthetic_fund("SPY / QQQ / HYG三合一追蹤【安聯月配息基金】(合成代標)")

    # ── 虛擬幣掃描 ────────────────────────────────
    if TEST_MODE == '5mk':
        print('\n📊 虛擬幣：期貨5分K模式，跳過虛擬幣掃描')
    elif 'CRYPTO' not in active_markets:
        print('\n📊 虛擬幣：非交易時段，跳過')
    else:
        print(f'\n📊 虛擬幣掃描：共{len(CRYPTO_LIST)}支')
        for ticker in CRYPTO_LIST:
            is_holding = ticker in HOLDINGS_CRYPTO
            result = scan_stock(ticker, is_holding)
            if result:
                # 【更正處】將原本刪掉的此行「name = ticker.replace('-USD', '')」 下行的買進和賣出全部改為 ticker
                if result[0] == 'BUY':
                    buy_signals.append(('虛擬幣', ticker, *result[1:]))
                elif result[0] == 'SELL':
                    sell_signals.append(('虛擬幣', ticker, *result[1:]))
                elif result[0] in ('DELIST_HOLD', 'DELIST_WATCH'):
                    delist_signals.append(('虛擬幣', ticker, result[0], result[1]))
            time.sleep(0.1) # 稍微加快掃描速度

    # ── 期貨5分K掃描（TEST_MODE='5mk'且FUTURES在active_markets時執行）──
    if 'FUTURES' in active_markets and TEST_MODE == '5mk':
        # ✅ 持倉狀態：跨掃描週期持續追蹤（程式重啟會重置，建議搭配方案B手動開關）
        if '_futures_is_holding' not in dir(): _futures_is_holding = False
        if '_futures_is_short'   not in dir(): _futures_is_short   = False
        _pos_str = '多倉🔴' if _futures_is_holding else ('空倉🔵' if _futures_is_short else '空手⬜')
        print(f'\n📊 期貨5分K掃描：{FUTURES_5MK_TARGETS}')
        print(f'   持倉狀態：{_pos_str}')
        for ticker in FUTURES_5MK_TARGETS:
            try:
                # ══════════════════════════════════════════════
                # ✅【三道關卡】期貨5分K進場先決條件（同本機版）
                # 第一道：週K位階（check_buy_precondition，3根）
                # 第二道：日K eLeader（check_buy_eleader，15根）
                # 第三道：5分K 54根條件A/B + RSI門檻
                # ══════════════════════════════════════════════

                # ── 第一道：週K位階 ──────────────────────────
                df5_w = get_stock_data(ticker, period='2y', interval='1wk', cache=weekly_cache)
                if df5_w is None or len(df5_w) < 30:
                    print(f'  ⚠️ {ticker} 週K資料不足，跳過'); continue
                df5_w = calc_indicators(df5_w)
                if not check_buy_precondition(df5_w):
                    print(f'  ❌ {ticker} 第一道週K未通過，跳過'); continue
                print(f'  ✅ {ticker} 第一道週K通過')

                # ── 第二道：日K eLeader ──────────────────────
                df5_d = get_stock_data(ticker, period='6mo', interval='1d', cache=daily_cache)
                if df5_d is None or len(df5_d) < 50:
                    print(f'  ⚠️ {ticker} 日K資料不足，跳過'); continue
                df5_d = calc_indicators(df5_d)
                if check_buy_eleader(df5_d) is None:
                    print(f'  ❌ {ticker} 第二道日K eLeader未通過，跳過'); continue
                print(f'  ✅ {ticker} 第二道日K eLeader通過')

                # ── 第三道：5分K 54根條件A/B ────────────────
                # ✅【夜盤保留】主力以夜盤為主戰場
                df5 = yf.download(ticker, period='5d', interval='5m', progress=False)
                if df5 is None or df5.empty or len(df5) < BUY_LOOKBACK_5MK + 2:
                    print(f'  ⚠️ {ticker} 5分K資料不足，跳過'); continue
                df5 = calc_indicators(df5)
                if df5 is None: continue

                n5 = BUY_LOOKBACK_5MK
                l5=df5['Low']; h5=df5['High']; bb5=df5['boll_bot20']
                bt5=df5['boll_top20']; bm5=df5['ma_c_20']; mh5=df5['macd_hist']; rsi5=df5['rsi14']
                rsi_now=float(rsi5.iloc[-1]); rsi_prev=float(rsi5.iloc[-2])
                mh_now=float(mh5.iloc[-1]);   mh_prev=float(mh5.iloc[-2])
                close=float(df5['Close'].iloc[-1]); boll_bot=float(bb5.iloc[-1]); boll_top=float(bt5.iloc[-1])
                now_str_f = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M')

                _5mk_cond_A = (l5.iloc[-n5:] <= bb5.iloc[-n5:] * BUY_BOLL_TOLERANCE).any() and rsi_now>rsi_prev and mh_now>mh_prev
                _5mk_low_mid  = (l5.iloc[-n5:] < bm5.iloc[-n5:]).all()
                _5mk_high_top = (h5.iloc[-n5:] < bt5.iloc[-n5:]).all()
                _5mk_macd_shr = len(mh5)>=n5+1 and all(float(mh5.iloc[-n5-1+j])>float(mh5.iloc[-n5+j]) for j in range(n5-1))
                _5mk_cond_B   = _5mk_low_mid and _5mk_high_top and _5mk_macd_shr and mh_now>mh_prev
                _5mk_buy = (_5mk_cond_A or _5mk_cond_B) and rsi_now > BUY_RSI_MIN

                # ── 5分鐘內最多2封上限（防吵機制）────────────
                _now_ts = time.time()
                if not hasattr(send_gmail, '_futures_log'): send_gmail._futures_log = []
                send_gmail._futures_log = [t for t in send_gmail._futures_log if _now_ts - t < 300]
                _can_send = len(send_gmail._futures_log) < 2

                # ── 做空三道關卡 ─────────────────────────────
                _short_1st = check_short_precondition(df5_w)
                _short_2nd = check_short_eleader(df5_d) is not None if _short_1st else False
                _5mk_short_A = (h5.iloc[-n5:] >= bt5.iloc[-n5:] * 1.00).any() and rsi_now<rsi_prev and mh_now<mh_prev
                _5mk_high_mid  = (h5.iloc[-n5:] > bm5.iloc[-n5:]).all()
                _5mk_low_bot   = (l5.iloc[-n5:] > bb5.iloc[-n5:]).all()
                _5mk_macd_exp  = len(mh5)>=n5+1 and all(float(mh5.iloc[-n5-1+j])<float(mh5.iloc[-n5+j]) for j in range(n5-1))
                _5mk_short_B   = _5mk_high_mid and _5mk_low_bot and _5mk_macd_exp and mh_now<mh_prev
                _5mk_short = _short_1st and _short_2nd and (_5mk_short_A or _5mk_short_B) and rsi_now < (100 - BUY_RSI_MIN)

                _is_night_now = (now_str_f[11:16] >= '01:00' and now_str_f[11:16] < '05:00')
                # ✅ 深夜01~05有持倉：只掃平倉，跳過買進
                if _5mk_buy and close <= boll_bot * BUY_BOLL_TOLERANCE and not (_is_night_now and _futures_is_holding):
                    if not _can_send:
                        print(f"  ⚠️ {ticker} 5分鐘內已發2封，跳過（防吵機制）")
                    else:
                        send_gmail._futures_log.append(_now_ts)
                        _ok = send_gmail(f"☁️【雲端】⭐期貨5分K買進 {ticker} - {now_str_f}",
                            f"☁️【雲端】⭐【期貨5分K買進訊號】⭐\n標的：{ticker}\n收盤：{close:.2f}　布林下緣：{boll_bot:.2f}\nRSI：{rsi_prev:.1f}→{rsi_now:.1f}（↑）\n時間：{now_str_f}")
                        print(f"  {'✅' if _ok else '❌'} {ticker} 買進訊號{'已發送' if _ok else '發送失敗'}")
                        # ✅ 方案A：買進訊號發出 → 記錄持倉狀態
                        _futures_is_holding = True
                        write_futures_status_to_firebase('holding', now_str_f, 1)
                        print(f"  📌 持倉狀態已標記：is_futures_holding=True")
                elif rsi_now < rsi_prev and mh_now < mh_prev and close >= boll_top * 1.00:
                    if not _can_send:
                        print(f"  ⚠️ {ticker} 5分鐘內已發2封，跳過（防吵機制）")
                    else:
                        send_gmail._futures_log.append(_now_ts)
                        _ok = send_gmail(f"☁️【雲端】🔔期貨5分K平倉 {ticker} - {now_str_f}",
                            f"☁️【雲端】🔔【期貨5分K平倉訊號】🔔\n標的：{ticker}\n收盤：{close:.2f}　布林上緣：{boll_top:.2f}\nRSI：{rsi_prev:.1f}→{rsi_now:.1f}（↓）\n時間：{now_str_f}")
                        print(f"  {'✅' if _ok else '❌'} {ticker} 平倉訊號{'已發送' if _ok else '發送失敗'}")
                        # ✅ 方案A：平倉訊號發出 → 清除持倉狀態
                        _futures_is_holding = False
                        write_futures_status_to_firebase('no_signal', now_str_f, 0)
                        print(f"  📌 持倉狀態已清除：is_futures_holding=False")
                elif _5mk_short and close >= boll_top * 1.00 and not (_is_night_now and _futures_is_short):
                    if not _can_send:
                        print(f"  ⚠️ {ticker} 5分鐘內已發2封，跳過（防吵機制）")
                    else:
                        send_gmail._futures_log.append(_now_ts)
                        _ok = send_gmail(f"☁️【雲端】🔻期貨5分K做空 {ticker} - {now_str_f}",
                            f"☁️【雲端】🔻【期貨5分K做空訊號】🔻\n標的：{ticker}\n收盤：{close:.2f}　布林上軌：{boll_top:.2f}\nRSI：{rsi_prev:.1f}→{rsi_now:.1f}（↓）\n時間：{now_str_f}")
                        print(f"  {'✅' if _ok else '❌'} {ticker} 做空訊號{'已發送' if _ok else '發送失敗'}")
                        _futures_is_short   = True
                        _futures_is_holding = False
                        write_futures_status_to_firebase('short', now_str_f, 1)
                        print(f"  📌 空倉狀態已標記：is_futures_short=True")

                elif _futures_is_short and _5mk_buy and close <= boll_bot * BUY_BOLL_TOLERANCE:
                    if not _can_send:
                        print(f"  ⚠️ {ticker} 5分鐘內已發2封，跳過（防吵機制）")
                    else:
                        send_gmail._futures_log.append(_now_ts)
                        _ok = send_gmail(f"☁️【雲端】🟢期貨5分K平空回補 {ticker} - {now_str_f}",
                            f"☁️【雲端】🟢【期貨5分K平空回補】🟢\n標的：{ticker}\n收盤：{close:.2f}　布林下軌：{boll_bot:.2f}\nRSI：{rsi_prev:.1f}→{rsi_now:.1f}（↑）\n時間：{now_str_f}")
                        print(f"  {'✅' if _ok else '❌'} {ticker} 平空回補{'已發送' if _ok else '發送失敗'}")
                        _futures_is_short = False
                        write_futures_status_to_firebase('no_signal', now_str_f, 0)
                        print(f"  📌 空倉狀態已清除：is_futures_short=False")

                else:
                    _pos = '多倉中' if _futures_is_holding else ('空倉中' if _futures_is_short else '空手')
                    print(f"  ℹ️ {ticker} RSI={rsi_now:.1f} 未達條件（{_pos}）")
            except Exception as e:
                print(f'  ❌ 期貨5分K掃描 {ticker} 失敗：{e}')

# ============================================================
# 【１５．發送Gmail通知】
# ============================================================
    print(f"\n{'='*55}")
    print(f"  掃描完成！買進訊號：{len(buy_signals)}支 / 賣出訊號：{len(sell_signals)}支 / 下市警報：{len(delist_signals)}支")
    print(f"{'='*55}")

# =====================
# ❌❌ 下市警報通知（最高優先級，最先發送）
# =====================
    if delist_signals:
        filtered_d = []
        for s in delist_signals:
            market, code, dtype, dmsg = s
            key = f"{market}_{code}_DELIST"
            if key not in notified[today]:
                filtered_d.append(s)
                notified[today].append(key)

        if filtered_d:
            hold_list  = [s for s in filtered_d if s[2] == 'DELIST_HOLD']
            watch_list = [s for s in filtered_d if s[2] == 'DELIST_WATCH']

            body = f"掃描時間：{now_str}\n\n"
            body += "═"*35 + "\n"
            body += "⚠️  下市風險警報  ⚠️\n"
            body += "═"*35 + "\n\n"

            if hold_list:
                body += "❌❌【終極警報：您持有的股票，請儘速出清！】❌❌\n"
                body += "─"*35 + "\n"
                for s in hold_list:
                    market, code, dtype, dmsg = s
                    body += (
                        f"市場：{market}　代碼：{code}\n"
                        f"風險原因：{dmsg}\n"
                        f"⚡ 不論RSI/布林帶任何數值，請立即確認新聞並考慮出清！\n"
                        f"{'─'*30}\n"
                    )

            if watch_list:
                body += "\n⚠️【下市預警：您未持有，但請告知可能持有的親友！】⚠️\n"
                body += "─"*35 + "\n"
                for s in watch_list:
                    market, code, dtype, dmsg = s
                    body += (
                        f"市場：{market}　代碼：{code}\n"
                        f"風險原因：{dmsg}\n"
                        f"{'─'*30}\n"
                    )

            subject = f"☁️【雲端】❌下市警報 {len(hold_list)}支持有須出清/{len(watch_list)}支觀察 - {now_str}"
            send_gmail(subject, body)
            save_notified(notified)
            print(f"  ❌ 下市警報已發送：持有{len(hold_list)}支 / 觀察{len(watch_list)}支")

# =====================
# 買進通知
# =====================
    if buy_signals:
        filtered = []

        for s in buy_signals:
            market, code, *_ = s
            key = f"{market}_{code}_BUY"

            if key not in notified[today]:
                filtered.append(s)
                notified[today].append(key)

        if filtered:
            body = f"掃描時間：{now_str}\n\n"

            for s in filtered:
                market, code, c, l, bb, r, rp = s
                body += (
                    f"⭐【買進訊號】⭐\n"
                    f"市場：{market}　代碼：{code}\n"
                    f"週K最低：{l:.2f}　布林下緣：{bb:.2f}\n"
                    f"收盤價：{c:.2f}\n"
                    f"RSI：{rp:.1f} → {r:.1f}（↑上升）\n"
                    f"{'─'*30}\n"
                )

            send_gmail(f"☁️【雲端】⭐買進訊號 {len(filtered)}支 - {now_str}", body)
            save_notified(notified)
        else:
            print(f"🔕 買進訊號 {len(buy_signals)-len(filtered)} 支已通知過")

# =====================
# 賣出通知
# =====================
    if sell_signals:
        filtered = []

        for s in sell_signals:
            market, code, *_ = s
            key = f"{market}_{code}_SELL"

            if key not in notified[today]:
                filtered.append(s)
                notified[today].append(key)

        if filtered:
            body = f"掃描時間：{now_str}\n\n"

            for s in filtered:
                market, code, c, h, bt, r, rp = s
                body += (
                    f"🔔【賣出訊號】🔔\n"
                    f"市場：{market}　代碼：{code}\n"
                    f"週K最高：{h:.2f}　布林上緣：{bt:.2f}\n"
                    f"收盤價：{c:.2f}\n"
                    f"RSI：{rp:.1f} → {r:.1f}（↓下降）\n"
                    f"{'─'*30}\n"
                )

            send_gmail(f"☁️【雲端】🔔賣出訊號 {len(filtered)}支 - {now_str}", body)
            save_notified(notified)
        else:
            print(f"🔕 賣出訊號 {len(sell_signals)-len(filtered)} 支已通知過")

# =====================
# 無訊號
# =====================
    if not buy_signals and not sell_signals and not delist_signals:
        print(f"📊 [{now_str}] 本次掃描無符合條件的股票，不發送通知。")

    # ── 期貨5分K模式：每次掃描結束後寫入 Firebase 狀態 ──
    if TEST_MODE == '5mk':
        _sig_count = len(buy_signals) + len(sell_signals)
        _status    = 'completed' if _sig_count == 0 else 'signal'
        write_futures_status_to_firebase(_status, now_str, _sig_count)

    print("\n✅ 全部完成！請查收Gmail通知。")

# ============================================================
# 【１６．執行守門員：精準測試與正式監控切換】
# ============================================================
if __name__ == "__main__":
    import time
    from datetime import datetime
    import pytz  # ✅ 新增：確保引入時區套件

    # ✅ 強制鎖定台北時區 (防 VPS 雲端主機時差導致錯過開盤)
    tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(tz)
    test_now = now.strftime('%H:%M:%S')

    # === [期貨5分K模式]：TEST_MODE = '5mk' ===
    if TEST_MODE == '5mk':
        tz_f = pytz.timezone('Asia/Taipei')
        now_f = datetime.now(tz_f)
        wd_f  = now_f.weekday()
        tv_f  = now_f.hour * 60 + now_f.minute
        in_futures = ((wd_f==0 and tv_f>=13*60) or (wd_f==1 and (tv_f<1*60 or tv_f>=5*60)) or (wd_f==2 and tv_f<=11*60+30))  # 週一13:00~週三11:30，深夜01~05除外
        if not in_futures:
            print(f"[{test_now}] ❌ 非期貨5分K時段，直接結束")
            time.sleep(5); exit()
        IS_GITHUB = bool(_os_tm.environ.get('GITHUB_ACTIONS', ''))
        if IS_GITHUB:
            print(f"🚀 [☁️GitHub Actions] 期貨5分K單次掃描　標的：{FUTURES_5MK_TARGETS}")
            try: main_task()
            except Exception as e: print(f"掃描發生錯誤: {e}")
            print("✅ 單次掃描完成，等待 cron 下次觸發")
            exit()
        else:
            print(f"🚀 [💻本機] 期貨5分K模式啟動　標的：{FUTURES_5MK_TARGETS}　間隔：{FUTURES_5MK_INTERVAL}秒")
            while True:
                now_l = datetime.now(pytz.timezone('Asia/Taipei'))
                wd_l  = now_l.weekday(); tv_l = now_l.hour*60+now_l.minute
                _is_night_l = (wd_l==1 and 1*60<=tv_l<5*60)
                if not((wd_l==0 and tv_l>=13*60) or (wd_l==1 and (tv_l<1*60 or tv_l>=5*60)) or (wd_l==2 and tv_l<=11*60+30) or (_is_night_l and (_futures_is_holding or _futures_is_short))):
                    print("✅ 期貨5分K時段結束，監控結束"); break
                try: main_task()
                except Exception as e: print(f"掃描發生錯誤: {e}")
                print(f"😴 休息 {FUTURES_5MK_INTERVAL} 秒...")
                time.sleep(FUTURES_5MK_INTERVAL)
            exit()

    # === [精準測試模式]：不受交易時間限制，發送兩封關鍵測試信後即結束 ===
    if TEST_MODE:
        print(f"\n🚀 [{test_now}] 啟動通知權限測試 (不混淆模式)...")
        
        # 1. 驗證【買進標籤】：預期會觸發「響鈴 + 重要標籤 + 黃色星號」
        send_gmail(f"🚨 【測試】買進訊號 - {test_now}", 
                   "此為 Gmail 篩選器測試信，請確認手機是否響鈴、是否有黃色星星。")
        
        # 2. 驗證【週K報告標籤】：預期會觸發「響鈴 + 重要標籤 + 無星號」
        send_gmail(f"📊 【測試】週K掃描完成 - {test_now}", 
                   "此為例行報告測試信，請確認手機是否響鈴，且不應該有星星。")
        
        print("\n✅ 精準測試信已發出。")
        print("💡 確認手機通知與星星正確後，請將第一章節的 TEST_MODE 改為 False 存檔。")
        print("💡 晚上 21:30 再次執行 .bat 即可進入正式監控模式。")
        time.sleep(10)
        exit()

    # === [正式監控模式]：TEST_MODE 為 False 時才會進入以下邏輯 ===
    hour = now.hour
    loops = 0
    market_name = ""

    if 9 <= hour <= 13:
        loops = 9
        market_name = "台股"
    elif hour >= 21 or hour <= 4:
        loops = 13
        market_name = "美股/虛擬幣"
    else:
        print(f"[{now.strftime('%H:%M:%S')}] ❌ 非交易時段啟動，直接結束")
        time.sleep(10)
        exit()

    print(f"🚀 {market_name} 監控模式啟動 (預計執行 {loops} 次)")

    for i in range(loops):
        tz = pytz.timezone('Asia/Taipei')
        current_time = datetime.now(tz).strftime('%H:%M:%S')
        print(f"\n⏰ 第 {i+1}/{loops} 次掃描開始 - {current_time}")
        try:
            main_task()
        except Exception as e:
            print(f"掃描發生錯誤: {e}")

        if i < loops - 1:
            print("😴 休息 30 分鐘...")
            time.sleep(1800)

    print(f"✅ {market_name} 監控行程圓滿結束")
    time.sleep(5)
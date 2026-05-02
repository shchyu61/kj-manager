SCRIPT_VERSION = '05021936'
# ============================================================
# 專案：Python股票週K布林RSI+Gmail推播自動通知
# 版本：(由AI每次改版時自動填寫)
# 更新日期：(由AI每次改版時依照對話視窗提供的日期,並經由使用者確認後為準)（新增期貨5分K模式：TEST_MODE="5mk"，週一13:00~週三11:30）
# 適用：台股1845支 + 美股38支 + 虛擬幣3支 + 三合一追蹤【安聯月配息基金】
# 通知方式：Gmail發信到 shchyu61@gmail.com
# 重要：本程式原則上只適用週K，可視情況用於日K以下
# =========================
# 快取設定（週K決定要不要看，日K決定準不準，5分K決定何時動手）
USE_CACHE = True
WEEKLY_REFRESH_HOUR = 8   # 每天早上更新一次週K
five_min_cache = {}  # ✅ 新增 5 分鐘 K 線快取容器
DELISTING_CHECK_DAYS = 1  # 每1天重新查詢（避免誤快取）（3天兼顧效能與即時性，可改1~7）
DELISTING_FILE = '2_delisting_cache.json'  # 下市風險本地快取檔
# ============================================================
# 【１．設定區】

# 驗證篩選模式：
#   False  = 正式模式（週K三層嚴格過濾，正式交易監控）
#   True   = 測試模式（只發測試Gmail確認通知是否正常）
#   '5mk'  = 期貨5分K模式（週一13:00~週三11:30，只掃台指近月）
TEST_MODE = False   # 切換：False / True / '5mk'

# ── 【期貨5分K專屬設定】（TEST_MODE = '5mk' 時才啟用）──────────
# 執行時段：週一13:00 → 週三大盤收盤11:30（日盤+夜盤+隔日日盤）
# 掃描標的：台灣加權指數（^TWII）作為台指期替代標的
# 通知對象：僅 shchyu61@gmail.com（本人專屬，家人親友不適用）
FUTURES_5MK_TARGETS  = ['^TWII']   # 期貨標的（可加入 'TXFF' 等）
FUTURES_5MK_INTERVAL = 300         # 每300秒（5分鐘）掃描一次
FUTURES_5MK_OWNER    = 'shchyu61@gmail.com'  # 5分K模式專屬帳號

# Firebase設定（本機版：讀取AI預篩清單快取）
FIREBASE_PROJECT_ID    = 'kj-wealth-manager'
FIREBASE_CRED_ENV      = 'FIREBASE_SERVICE_KEY'         # 環境變數名稱
FIREBASE_CRED_FILE     = 'firebase_service_key.json'    # 本機金鑰檔案路徑（與.py同目錄）
# ↑ 請在本機放一份 Firebase Service Account JSON 金鑰，
#   或設定 FIREBASE_SERVICE_KEY 環境變數（內容為JSON字串）

# Gmail設定
# ✅ ☁️【雲端】執行：直接填入帳號密碼
# ✅ ☁️【雲端】GitHub Actions執行：自動從 GitHub Secrets 讀取，不需填寫
import os as _os
GMAIL_ACCOUNT  = _os.environ.get("GMAIL_ACCOUNT",  "shchyu61@gmail.com") # 您Gmail（寄件人）
GMAIL_PASSWORD = _os.environ.get("GMAIL_PASSWORD", "")  # ☁️【雲端】從Secrets讀取；或☁️【雲端】填入密碼格式："xxxx xxxx xxxx xxxx"（密碼可刪。實戰要補上。）。
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

FX_LIST = ['EURUSD=X', 'JPY=X', 'CHFUSD=X', 'USDTWD=X']
HOLDINGS_FX = []
HOLDINGS_TW     = ['2330', '3037','3147','6188']  # 有台股持有時填入，例如：['2330', '2317']

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
#   日K = 3根（與週K統一，確認當下位階即可）
#   5分K = 54根（≈1個台灣日盤：270分÷5=54根，剔除夜盤後使用）
BUY_LOOKBACK_BARS    = 3      # 週K回看根數（條件A/B共用）
BUY_LOOKBACK_DAILY   = 3      # 日K回看根數（與週K統一，3根即可確認位階）
BUY_LOOKBACK_5MK     = 54     # 5分K回看根數（近54根5分K棒，含夜盤，主力夜盤為主戰場）
# ── 【掃描週期模式】切換此處決定scan_stock用哪個週期把關 ──────────
# 'weekly' = 週K三道關卡（第一道週K3根/第二道日K eLeader/第三道5分K3根）
# 'daily'  = 日K三道關卡（第一道日K3根/第二道日K eLeader/第三道5分K3根）
# 'mixed'=混合模式(週K三道 OR 日K三道，任一通過即觸發)
SCAN_MODE = 'mixed'   # 切換：'weekly' / 'daily' / 'mixed'
BUY_RSI_MIN          = 35     # 買進RSI最低門檻（條件A，RSI需 > 此值才視為上升有效）
BUY_BOLL_TOLERANCE   = 1.02   # 布林下緣容忍度（1.02=允許價格在下緣上方2%內仍觸發）

# ── 【２-2】買進策略：eLeader 25個複合條件 ────────────────────────
# （eLeader條件邏輯在第8章，勿修改第8章程式碼，只改這裡的參數）
ELEADER_RSI_MAX      = 78     # eLeader基底條件：前根RSI需 < 此值才允許進場

# ── 【２-3】做多賣出策略（獲利了結）：截圖1、2條件 ─────────────
# 最高價 >= 布林上緣 AND RSI下降 AND MACD柱縮小
SELL_BOLL_TOLERANCE  = 1.00   # 布林上緣容忍度（1.00=嚴格貼上緣；0.97=上緣下方3%即觸發）

# ── 【２-4】做多停損策略（預留，目前未啟用）────────────────────
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
# 全額交割股 = 財務惡化警訊，是下市前最重要的早期指標！
# 持股在清單：❌❌終極警報，請儘速評估是否出清
# 觀察股在清單：⚠️ 勿碰預警，等恢復正常交割再說
ENABLE_CASH_DELIVERY_CHECK = True   # True=啟用全額交割預警 / False=關閉
CASH_DELIVERY_CACHE_HOURS  = 72     # 全額交割清單快取時間（小時，72=3天）

# ── 【２-7】做空入場策略：空頭三道關卡條件 ────────────────────────
# ⚠️ 做空策略嚴禁用於當沖或隔日沖，僅供中長期空頭佈局參考
# 做空條件與買進策略完全鏡像（布林上緣 / eLeader反向 / 5分K空頭轉折）
ENABLE_SHORT          = True    # True=開啟做空掃描 / False=關閉做空掃描
SHORT_RSI_MAX         = 65      # 做空RSI上限門檻（RSI需 < 此值才視為有效空頭）
SHORT_BOLL_TOLERANCE  = 0.98    # 布林上緣容忍度（0.98=允許在上緣下方2%內觸發）
SHORT_LOOKBACK_BARS   = 3       # 做空回看根數（與買進策略對稱）

# ── 【２-8】做空停損策略（預留，目前未啟用）─────────────────────
# 做空停損：當價格反轉突破布林上緣時強制回補
ENABLE_SHORT_STOP_LOSS = False   # 做空停損開關（False=未啟用）

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

        # ── 【第5階段】停用 .info 欄位判斷（yfinance新版台股常回傳空dict）
        elif not delist_date:
            pass  # 只保留官方 delistingDate 作為下市依據

    except Exception as e:
        _es = str(e)
        if any(k in _es for k in ['Too Many Requests','Rate limit','429','HTTPError','ConnectionError']):
            print(f'  ⏭️ {ticker} Yahoo速率限制，跳過下市檢查')
            return False, ''
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
    """全額交割股 Set，重試2次，快取72小時，空白時保留舊快取"""
    if not ENABLE_CASH_DELIVERY_CHECK:
        return set()
    from datetime import datetime as _dt
    import time as _time
    now = _dt.now()
    with _cash_delivery_lock:
        if _cash_delivery_cache['ts'] is not None:
            elapsed = (now - _cash_delivery_cache['ts']).total_seconds() / 3600
            if elapsed < CASH_DELIVERY_CACHE_HOURS:
                return _cash_delivery_cache['codes']
    codes = set()
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
    def _fetch(url):
        import requests as _req
        for attempt in range(1, 3):
            try:
                r = _req.get(url, headers=headers, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    if data:
                        return data
                print(f'  ⚠️ 第{attempt}次失敗，{"重試中..." if attempt < 2 else "放棄"}')
            except Exception as e:
                print(f'  ⚠️ 第{attempt}次失敗：{e}，{"重試中..." if attempt < 2 else "放棄"}')
            if attempt < 2:
                _time.sleep(5)
        return None
    d1 = _fetch('https://openapi.twse.com.tw/v1/company/cashPaymentStocks')
    if d1:
        for item in d1:
            c = item.get('公司代號') or item.get('Code') or item.get('code') or ''
            if c: codes.add(str(c).strip())
        print(f'  ✅ TWSE全額交割上市：{len(codes)} 支')
    else:
        print(f'  ❌ TWSE全額交割：重試2次均失敗')
    before = len(codes)
    d2 = _fetch('https://www.tpex.org.tw/openapi/v1/tpex_cash_payment_stocks')
    if d2:
        for item in d2:
            c = item.get('公司代號') or item.get('Code') or item.get('code') or ''
            if c: codes.add(str(c).strip())
        print(f'  ✅ TPEX全額交割上櫃：{len(codes)-before} 支')
    else:
        print(f'  ❌ TPEX全額交割：重試2次均失敗')
    if not codes:
        try:
            import requests as _req
            from bs4 import BeautifulSoup as _BS
            r3 = _req.get('https://www.twse.com.tw/zh/page/trading/exchange/TWTB4U.html',
                          headers=headers, timeout=15)
            soup = _BS(r3.text, 'html.parser')
            for td in soup.select('table td:first-child'):
                c = td.get_text(strip=True)
                if c.isdigit() and len(c) == 4: codes.add(c)
            print(f'  ✅ 備援HTML全額交割：{len(codes)} 支')
        except Exception as e:
            print(f'  ⚠️ 備援HTML失敗：{e}')
    with _cash_delivery_lock:
        if codes:
            _cash_delivery_cache['codes'] = codes
            _cash_delivery_cache['ts'] = now
        elif _cash_delivery_cache['ts'] is None:
            _cash_delivery_cache['codes'] = codes
            _cash_delivery_cache['ts'] = now
        else:
            print(f'  ⚠️ 本次抓取失敗，保留舊快取（{len(_cash_delivery_cache["codes"])} 支）')
    print(f'  📋 全額交割股共 {len(_cash_delivery_cache["codes"])} 支（含上市+上櫃）')
    return _cash_delivery_cache['codes']

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
    # ✅ 週六補跑：強制加入台股（確保預篩快取能上傳）
    elif weekday == 5:
        import os, json
        _sf = os.path.join(os.path.dirname(os.path.abspath(__file__)), '5_saturday_scan.json')
        _td = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y-%m-%d')
        try:
            _sd = json.load(open(_sf, 'r', encoding='utf-8'))
            if _sd.get('date') == _td:
                active.append('TW')  # 週六補跑旗標存在，允許台股掃描
        except: pass

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
        active.append('FUTURES')  # 台指期5分K

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
# ============================================================
# 【Firebase notified 讀寫】（解決 GitHub Actions 無狀態重複通知）
# ============================================================
def load_notified_firebase():
    """從 Firebase 讀取 notified 記錄（雲端版跨執行共用）"""
    try:
        import json, os
        import requests as _req
        cred_json = os.environ.get(FIREBASE_CRED_ENV)
        if not cred_json:
            return {}
        cred = json.loads(cred_json)
        import google.oauth2.service_account as _sa
        import google.auth.transport.requests as _gtr
        credentials = _sa.Credentials.from_service_account_info(
            cred, scopes=['https://www.googleapis.com/auth/datastore'])
        credentials.refresh(_gtr.Request())
        token = credentials.token
        url = (f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
               f"/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}/public/notified_log")
        resp = _req.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=10)
        if resp.status_code == 200:
            fields = resp.json().get('fields', {})
            data_str = fields.get('data', {}).get('stringValue', '{}')
            return json.loads(data_str)
    except Exception as e:
        print(f"  ⚠️ Firebase notified 讀取失敗：{e}")
    return {}

def save_notified_firebase(data):
    """將 notified 記錄寫入 Firebase（雲端版跨執行共用）"""
    try:
        import json, os
        import requests as _req
        cred_json = os.environ.get(FIREBASE_CRED_ENV)
        if not cred_json:
            return
        cred = json.loads(cred_json)
        import google.oauth2.service_account as _sa
        import google.auth.transport.requests as _gtr
        credentials = _sa.Credentials.from_service_account_info(
            cred, scopes=['https://www.googleapis.com/auth/datastore'])
        credentials.refresh(_gtr.Request())
        token = credentials.token
        url = (f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
               f"/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}/public/notified_log")
        payload = {"fields": {"data": {"stringValue": json.dumps(data, ensure_ascii=False)}}}
        _req.patch(url, json=payload,
                   headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                   timeout=10)
    except Exception as e:
        print(f"  ⚠️ Firebase notified 寫入失敗：{e}")

def load_notified():
    """載入通知紀錄：雲端版優先用Firebase，本機版用json檔"""
    import os
    # 雲端版：FIREBASE_CRED_ENV 存在時用 Firebase
    if os.environ.get(FIREBASE_CRED_ENV):
        return load_notified_firebase()
    # 本機版：用json檔
    if os.path.exists('4_notified_today.json'):
        try:
            with open('4_notified_today.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            print("⚠️ 通知紀錄檔損壞，已重置")
    return {}

def save_notified(data):
    """儲存通知紀錄：雲端版優先用Firebase，本機版用json檔"""
    import os
    if os.environ.get(FIREBASE_CRED_ENV):
        save_notified_firebase(data)
        return
    with open('4_notified_today.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
# ============================================================
# 【７．技術指標計算】
# ============================================================
def calc_indicators(df):
    if df is None or len(df) < 26:
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
def check_buy_precondition(df, is_weekly=False):
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

        # ── 條件C（大盤強多頭時放寬門檻）──────────────────────────
        # 大盤週K近N根最低價都沒碰中軌（強多頭），個股只需近N根任一最低價 <= 布林中軌
        # 且 RSI上升 AND MACD柱放大，視為低相對位階可進場
        try:
            low_touch_mid = (l.iloc[-n:] <= bm.iloc[-n:]).any()
            cond_C = low_touch_mid and rsi_rising and macd_rising
        except Exception:
            cond_C = False


        # ── 條件D（上軌做多：大盤強多頭時，K棒在上軌附近進場）────────────
        # 位階條件：近5根每一根最低價都在布林中軌以上（不得碰中軌）
        # 1a：近5根任意連續3根 OSC 連跌（後根 < 前根）
        # 1b：前1根（倒數第2根）OSC 高於前2根（OSC轉升）
        # 1c：當根 RSI↑ AND MACD柱↑（截圖4原策略）
        # 2a：近5根任意連續3根 DIF 高於 MACD（任意連續3根）
        # 2b：前1根（倒數第2根）DIF 低於 MACD
        # 2c：當根 DIF 高於 MACD
        if not is_weekly:
            cond_D = False  # 條件D只在週K模式生效
        else:
          try:
            ml  = df['macd_line']    # DIF（黃線）
            ms  = df['macd_signal']  # MACD(9)（淡藍線）
            _N  = 5  # 回看5根

            # 位階：近5根每一根最低價都在中軌以上
            _all_above_mid = (l.iloc[-_N:] > bm.iloc[-_N:]).all()

            # 1a：近5根任意連續3根 OSC 連跌
            _osc_vals = [float(mh.iloc[i]) for i in range(-_N, 0)]
            _osc_3drop = any(
                _osc_vals[j] > _osc_vals[j+1] > _osc_vals[j+2]
                for j in range(len(_osc_vals)-2)
            )
            # 1b：前1根（倒數第2根）OSC 高於前2根（OSC轉升）
            _osc_prev_rise = float(mh.iloc[-2]) > float(mh.iloc[-3])
            # 1c：當根 MACD柱↑ AND RSI↑
            _osc_now_rise  = float(mh.iloc[-1]) > float(mh.iloc[-2])

            # ✅ 條件D簡化版：移除2a/2b/2c（DIF vs MACD線），只保留OSC柱狀體條件
            # 原因：日K的DIF/MACD線不一定同步，條件過嚴會錯過進場機會
            cond_D = (
                _all_above_mid and
                _osc_3drop and _osc_prev_rise and _osc_now_rise and rsi_rising
            )
          except Exception:
            cond_D = False

        _abc = cond_A or cond_B or cond_C
        return _abc or cond_D, cond_D  # (整體通過, 是否由條件D觸發)
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
            
            # 基金模式（df_5m=None）：第三道改用日K RSI↑ AND MACD柱↑（方案Q）
            # 使用當根 vs 前1根，與股票策略一致
            try:
                _rsi_daily_ok  = float(rsi.iloc[-1]) > float(rsi.iloc[-2])
                _macd_daily_ok = float(df['macd_hist'].iloc[-1]) > float(df['macd_hist'].iloc[-2])
                if _rsi_daily_ok and _macd_daily_ok:
                    return ('BUY', c.iloc[-1], float(rsi.iloc[-2]), float(rsi.iloc[-1]))
                else:
                    return None  # 日K第三道未通過
            except Exception:
                return None
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

def check_sell_condD(df):
    """條件D多頭專屬出場：
    出場A（主要）：當根 OSC < 前1根 AND 當根 DIF < MACD（兩個同時反轉）
    出場B（防線）：近N根任一最低價 ≤ 布林中軌（結構破壞）
    前提：近N根最低價仍在中軌以上（確認是條件D部位）
    """
    try:
        l  = df['Low']
        bm = df['ma_c_20']
        mh = df['macd_hist']
        ml = df['macd_line']
        ms = df['macd_signal']
        n  = BUY_LOOKBACK_BARS

        # 前提：近N根最低價仍在中軌以上（確認是條件D部位）
        _still_above_mid = (l.iloc[-n:] > bm.iloc[-n:]).all()
        if not _still_above_mid:
            return False, ''

        # 出場A：OSC反轉 AND DIF跌破MACD（兩個同時）
        _osc_drop  = float(mh.iloc[-1]) < float(mh.iloc[-2])
        _dif_below = float(ml.iloc[-1]) < float(ms.iloc[-1])
        exit_A = _osc_drop and _dif_below

        # 出場B：近N根任一最低價 ≤ 布林中軌
        exit_B = (l.iloc[-n:] <= bm.iloc[-n:]).any()

        if exit_A:
            return True, '條件D出場A：MACD指標雙反轉（OSC↓ AND DIF跌破訊號線）'
        if exit_B:
            return True, '條件D出場B：最低價跌破布林中軌，強多頭結構破壞'
        return False, ''
    except Exception as e:
        return False, ''


def check_cover_condD(df):
    """條件D空頭專屬回補（鏡像）：
    出場A：當根 OSC > 前1根 AND 當根 DIF > MACD
    出場B：近N根任一最高價 ≥ 布林中軌
    前提：近N根最高價仍在中軌以下
    """
    try:
        h  = df['High']
        bm = df['ma_c_20']
        mh = df['macd_hist']
        ml = df['macd_line']
        ms = df['macd_signal']
        n  = BUY_LOOKBACK_BARS

        _still_below_mid = (h.iloc[-n:] < bm.iloc[-n:]).all()
        if not _still_below_mid:
            return False, ''

        _osc_rise  = float(mh.iloc[-1]) > float(mh.iloc[-2])
        _dif_above = float(ml.iloc[-1]) > float(ms.iloc[-1])
        exit_A = _osc_rise and _dif_above

        exit_B = (h.iloc[-n:] >= bm.iloc[-n:]).any()

        if exit_A:
            return True, '條件D空頭回補A：MACD指標雙反轉（OSC↑ AND DIF突破訊號線）'
        if exit_B:
            return True, '條件D空頭回補B：最高價突破布林中軌，空頭結構破壞'
        return False, ''
    except:
        return False, ''


# ============================================================
# 【１０-２．做空策略：三道關卡（買進策略完全鏡像）】
# 第一道：週K觸碰上軌（mirror of 觸碰下軌）
# 第二道：日K eLeader 25條件全部反向
# 第三道：5分K RSI↓ AND MACD柱↓
# ============================================================
def check_short_precondition(df, is_weekly=False):
    """做空第一道：週K位階高檔（買進策略完全鏡像）"""
    try:
        l   = df['Low']
        h   = df['High']
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


        # ── 鏡像條件D（上軌做空：強多頭回檔，K棒在中軌以上做空）────────────
        # 位階：近5根每一根最高價都在布林中軌以下
        # 1a鏡像：近5根任意連續3根 OSC 連漲
        # 1b鏡像：前1根 OSC 低於前2根（OSC轉跌）
        # 1c鏡像：當根 MACD柱↓ AND RSI↓
        # 2a鏡像：近5根任意連續3根 DIF < MACD
        # 2b鏡像：前1根 DIF > MACD
        # 2c鏡像：當根 DIF < MACD
        if not is_weekly:
            cond_D_short = False
        else:
          try:
            ml  = df['macd_line']
            ms  = df['macd_signal']
            _N  = 5

            # 位階：近5根每一根最高價都在中軌以下
            _all_below_mid = (h.iloc[-_N:] < bm.iloc[-_N:]).all()

            # 1a：近5根任意連續3根 OSC 連漲
            _osc_vals = [float(mh.iloc[i]) for i in range(-_N, 0)]
            _osc_3rise = any(
                _osc_vals[j] < _osc_vals[j+1] < _osc_vals[j+2]
                for j in range(len(_osc_vals)-2)
            )
            # 1b：前1根 OSC 低於前2根（OSC轉跌）
            _osc_prev_drop = float(mh.iloc[-2]) < float(mh.iloc[-3])
            # 1c：當根 MACD柱↓ AND RSI↓
            _osc_now_drop  = float(mh.iloc[-1]) < float(mh.iloc[-2])

            # ✅ 條件D空頭鏡像簡化版：移除DIF vs MACD線條件，只保留OSC
            cond_D_short = (
                _all_below_mid and
                _osc_3rise and _osc_prev_drop and _osc_now_drop and rsi_falling
            )
          except Exception:
            cond_D_short = False

        _abc_s = cond_A or cond_B
        return _abc_s or cond_D_short, cond_D_short  # (整體通過, 是否由條件D空頭觸發)
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

def analyse_market_index(ticker, label):
    """
    大盤指數全條件分析（週K+日K，A/B/C/D多空）
    回傳 dict：
      'bull_abc': bool  → 多頭A/B/C（下軌/中軌V轉）
      'bull_d'  : bool  → 多頭條件D（上軌做多）
      'bear_abc': bool  → 空頭A/B/C（上軌/中軌M轉）
      'bear_d'  : bool  → 空頭條件D（下軌做空）
      'warn'    : bool  → 無法判定（下彎警告）
      'rsi_w'   : float → 週K RSI
      'rsi_d'   : float → 日K RSI
    """
    result = {'bull_abc':False,'bull_d':False,'bear_abc':False,'bear_d':False,'warn':False,'rsi_w':0,'rsi_d':0}
    try:
        # 週K
        df_w = yf.download(ticker, period='2y', interval='1wk', progress=False)
        if df_w is None or len(df_w) < 30: return result
        df_w = calc_indicators(df_w)
        ok_w, condD_bull_w = check_buy_precondition(df_w, is_weekly=True)
        ok_ws, condD_bear_w = check_short_precondition(df_w, is_weekly=True)
        last_w = df_w.iloc[-1]; prev_w = df_w.iloc[-2]
        result['rsi_w'] = float(last_w['rsi14'])

        # 日K
        df_d = yf.download(ticker, period='1y', interval='1d', progress=False)
        if df_d is not None and len(df_d) >= 30:
            df_d = calc_indicators(df_d)
            ok_d, condD_bull_d = check_buy_precondition(df_d, is_weekly=False)
            ok_ds, condD_bear_d = check_short_precondition(df_d, is_weekly=False)
            result['rsi_d'] = float(df_d.iloc[-1]['rsi14'])
        else:
            ok_d = ok_ds = condD_bull_d = condD_bear_d = False

        # 多頭A/B/C：週K或日K通過（非條件D）
        result['bull_abc'] = (ok_w and not condD_bull_w) or (ok_d and not condD_bull_d)
        # 多頭條件D：週K條件D通過（條件D只適用週K）
        result['bull_d']   = condD_bull_w
        # 空頭A/B/C：週K或日K空頭通過（非條件D）
        result['bear_abc'] = (ok_ws and not condD_bear_w) or (ok_ds and not condD_bear_d)
        # 空頭條件D：週K空頭條件D
        result['bear_d']   = condD_bear_w
        # 下彎警告：RSI↓ AND MACD柱↓（兩個都下彎才警告）
        rsi_down  = float(last_w['rsi14']) < float(prev_w['rsi14'])
        macd_down = float(last_w['macd_hist']) < float(prev_w['macd_hist'])
        result['warn'] = rsi_down and macd_down and not ok_w and not ok_ws

        # 輸出診斷
        flags = []
        if result['bull_d']:   flags.append('條件D多頭✅')
        if result['bull_abc']: flags.append('A/B/C多頭✅')
        if result['bear_d']:   flags.append('條件D空頭✅')
        if result['bear_abc']: flags.append('A/B/C空頭✅')
        if result['warn']:     flags.append('⚠️下彎警告')
        if not flags:          flags.append('中性觀望')
        print(f"  {label}大盤判定：{' / '.join(flags)}（週K RSI:{result['rsi_w']:.1f} 日K RSI:{result['rsi_d']:.1f}）")
    except Exception as e:
        print(f"  ⚠️ {label}大盤分析失敗: {e}")
    return result

def scan_stock(ticker, is_holding=False):
    global _tw_prescreened
    global weekly_cache, daily_cache
    
    # ✅ [新增: 5分K 專用短效快取, 避免 30 分鐘內重複抓取過多 API]
    # 使用變數名 five_min_cache (請確保在程式開頭 global 宣告過)
    global five_min_cache 
    if 'five_min_cache' not in globals(): five_min_cache = {}

    try:
        # 0. 【最高優先級①】全額交割股預警（比Yahoo Finance下市偵測更早）
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
            # 條件D專屬出場（優先判斷，避免MACD反轉卻被普通賣出邏輯漏掉）
            _dD_exit, _dD_msg = check_sell_condD(df_w)
            if _dD_exit:
                c_price = float(df_w['Close'].iloc[-1])
                print(f'  🔔 {ticker} {_dD_msg}')
                return ('SELL', c_price, float(df_w['High'].iloc[-1]), float(df_w['boll_top20'].iloc[-1]),
                        float(df_w['rsi14'].iloc[-1]), float(df_w['rsi14'].iloc[-2]))
            # 普通賣出條件（原有）
            if check_sell_condition(df_w):
                c_price = float(df_w['Close'].iloc[-1])
                return ('SELL', c_price, float(df_w['High'].iloc[-1]), float(df_w['boll_top20'].iloc[-1]),
                        float(df_w['rsi14'].iloc[-1]), float(df_w['rsi14'].iloc[-2]))
            return None

        # ── 第一道：週K/日K統一用3根（條件A or B）────────────────
        # weekly：用週K 3根
        # daily ：用日K 3根（與週K根數相同，確認當下位階即可）
        if not TEST_MODE:
            if SCAN_MODE == 'daily':
                # 日K模式第一道：日K 3根，條件A or B
                _df1st = get_stock_data(ticker, period='6mo', interval='1d', cache=daily_cache)
                if _df1st is None or len(_df1st) < 50: return None
                _df1st = calc_indicators(_df1st)
                if _df1st is None: return None
                # BUY_LOOKBACK_BARS=3，不換算，直接跑
                _is_long_ok, _condD_long   = check_buy_precondition(_df1st)
                _is_short_ok, _condD_short = check_short_precondition(_df1st)
            else:
                # 週K模式第一道：用週K 3根（原設計）
                _is_long_ok, _condD_long   = check_buy_precondition(df_w, is_weekly=True)
                _is_short_ok, _condD_short = check_short_precondition(df_w, is_weekly=True)
            # ✅ 診斷輸出：第一道結果
            _wk_label = '週K' if SCAN_MODE != 'daily' else '日K'
            if _is_long_ok:
                print(f'  ✅ {ticker} 第一道{_wk_label}通過（多頭 {"條件D" if _condD_long else "A/B/C"}）')
            elif _is_short_ok:
                print(f'  ✅ {ticker} 第一道{_wk_label}通過（空頭 {"條件D" if _condD_short else "A/B/C"}）')
            else:
                return None   # 多空都不過才跳過
            # ✅ 第一道通過 → 立刻加入全域預篩清單（不管後續道數是否通過）
            _code_only = ticker.split('.')[0]
            if _code_only not in _tw_prescreened:
                _tw_prescreened.append(_code_only)
        else:
            print(f"🧪 {ticker} 正在進行【驗證篩選】測試中...")
            _condD_long = False; _condD_short = False  # TEST_MODE 預設

        # ── 第二道：週K/日K統一用日K eLeader 25條件（AND邏輯）─────
        # 兩種模式統一：eLeader必過才繼續（確保不在高檔區追高）
        df_d = get_stock_data(ticker, period='6mo', interval='1d', cache=daily_cache)
        if df_d is None or len(df_d) < 50: return None
        df_d = calc_indicators(df_d)
        if df_d is None: return None

        if _is_long_ok:
            is_eleader_ok = check_buy_eleader(df_d) is not None
            if _condD_long:
                # 條件D觸發（高位階上軌）→ eLeader 為可選，不強制
                print(f'  {"✅" if is_eleader_ok else "⚠️"} {ticker} 第二道eLeader多頭 {"通過" if is_eleader_ok else "未通過（條件D補位，繼續）"}')
            else:
                # A/B/C 觸發（低位階下軌/中軌）→ eLeader 為必要條件
                print(f'  {"✅" if is_eleader_ok else "❌"} {ticker} 第二道eLeader多頭 {"通過" if is_eleader_ok else "未通過，跳過"}')
                if not is_eleader_ok:
                    return None
        else:
            is_eleader_short_ok = check_short_eleader(df_d) is not None
            if _condD_short:
                # 條件D空頭（高位階）→ eLeader 為可選
                print(f'  {"✅" if is_eleader_short_ok else "⚠️"} {ticker} 第二道eLeader空頭 {"通過" if is_eleader_short_ok else "未通過（條件D補位，繼續）"}')
            else:
                # A/B/C 空頭（低位階）→ eLeader 為必要條件
                print(f'  {"✅" if is_eleader_short_ok else "❌"} {ticker} 第二道eLeader空頭 {"通過" if is_eleader_short_ok else "未通過，跳過"}')
                if not is_eleader_short_ok:
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

        # ── 第三道：5分K（RSI AND MACD）AND（5分K近3根條件A or B）────
        # 週K/日K模式統一，避免過度密集觸發
        rsi_5m_falling  = (last_rsi < prev_rsi and last_rsi < (100 - BUY_RSI_MIN))
        macd_5m_falling = not macd_5m_ok  # MACD柱下降
        cond_5m_buy     = check_buy_precondition(df_5m)    # 買進：近3根5分K條件A or B
        cond_5m_short   = check_short_precondition(df_5m)  # 做空：近3根5分K鏡像條件A or B

        if _is_long_ok:
            # ── 多頭第三道：（RSI↑ AND MACD↑）AND（5分K買進條件A or B）
            if not (rsi_5m_ok and macd_5m_ok and cond_5m_buy):
                return None
            c = float(df_5m['Close'].iloc[-1])
            ref_df = df_d if SCAN_MODE == 'daily' else df_w
            mode_tag = '中期投資' if SCAN_MODE == 'daily' else '長期投資'
            print(f"🔥 {ticker} 觸發【{mode_tag}三道關卡 多頭買進】，成交價：{c}")
            return ('BUY', c, float(ref_df['Low'].iloc[-1]), float(ref_df['boll_bot20'].iloc[-1]),
                    float(ref_df['rsi14'].iloc[-1]), float(ref_df['rsi14'].iloc[-2]))
        else:
            # ── 空頭第三道：（RSI↓ AND MACD↓）AND（5分K做空條件A or B）
            if not (rsi_5m_falling and macd_5m_falling and cond_5m_short):
                return None
            c = float(df_5m['Close'].iloc[-1])
            ref_df = df_d if SCAN_MODE == 'daily' else df_w
            mode_tag = '中期投資' if SCAN_MODE == 'daily' else '長期投資'
            print(f"🔥 {ticker} 觸發【{mode_tag}三道關卡 空頭做空】，成交價：{c}")
            return ('SHORT', c, float(ref_df['High'].iloc[-1]), float(ref_df['boll_top20'].iloc[-1]),
                    float(ref_df['rsi14'].iloc[-1]), float(ref_df['rsi14'].iloc[-2]))
 
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
        # 1. 週K（第一道：位階門檻）
        s_w = yf.download("SPY", period='2y', interval='1wk', progress=False)
        q_w = yf.download("QQQ", period='2y', interval='1wk', progress=False)
        h_w = yf.download("HYG", period='2y', interval='1wk', progress=False)
        df_w = calc_indicators(build_fund_proxy_df(s_w, q_w, h_w))
        if df_w is None or not check_buy_precondition(df_w, is_weekly=True):
            print(f"ℹ️ {fund_name}:週K位階尚未符合觸發買進條件")
            return

        # 2. 日K（第二道：eLeader 25條件）
        s_d = yf.download("SPY", period='1y', interval='1d', progress=False)
        q_d = yf.download("QQQ", period='1y', interval='1d', progress=False)
        h_d = yf.download("HYG", period='1y', interval='1d', progress=False)
        df_d = calc_indicators(build_fund_proxy_df(s_d, q_d, h_d))

        # ❌ 基金不需要5分K（每日公布一次淨值，5分K無意義）
        # ✅ 第三道改用日K RSI↑ AND MACD柱↑（方案Q）
        result = check_buy_eleader(df_w, df_d, None, fund_name)  # df_5m=None → 跳過5分K
        
        if result and result[0] == 'BUY':
            # --- [修正 BUG：補足 7 個變數並存入清單以對齊第 14 章節的 unpack 需求, 由第 14 章彙整發信] ---
            # result 內容為 ('BUY', 當前價, prev_rsi, last_rsi)
            c_price = result[1]
            r_prev  = result[2]
            r_now   = result[3]
            l_val   = float(df_w['Low'].iloc[-1])       # 補上最低價（依SCAN_MODE）
            bb_val  = float(df_w['boll_bot20'].iloc[-1])# 補上布林下緣
            
            # 精準存入 7 個變數：market, code, c, l, bb, r, rp
            buy_signals.append(('基金', fund_name, c_price, l_val, bb_val, r_now, r_prev))
            
            # --- [修正 BUG：send_gmail 內文必須為格式化字串，不可傳入 Tuple] ---
            # ✅ 修正重複通知Bug：基金當天同標的只發1次
            _fund_key = f"基金_{fund_name}_BUY"
            _today_f  = datetime.now().strftime("%Y-%m-%d")
            if _today_f not in notified: notified[_today_f] = []
            if _fund_key in notified[_today_f]:
                print(f"🔕 {fund_name} 今日買進訊號已通知過，跳過")
            else:
                notified[_today_f].append(_fund_key)
                save_notified(notified)
                msg_body = (
                    f"⭐【基金買進訊號】⭐\n"
                    f"市場：基金　代碼：{fund_name}\n"
                    f"收盤價：{c_price:.2f}\n"
                    f"RSI轉折：{r_prev:.1f} → {r_now:.1f}\n"
                    f"⚠️ 嚴禁用於當沖或隔日沖\n"
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

# ============================================================
# 【Firebase 預篩清單讀取】
# 從 Firebase 讀取雲端版每天14:00更新的台股預篩清單
# 本機版優先使用此快取，避免每次都掃1800支
# ============================================================
def write_tw_prescreened(codes_list):
    """將預篩台股寫入Firebase"""
    try:
        import json, os, requests as _req
        from datetime import datetime; import pytz
        cred_json = os.environ.get(FIREBASE_CRED_ENV)
        if not cred_json:
            _cf = os.path.join(os.path.dirname(os.path.abspath(__file__)), FIREBASE_CRED_FILE)
            if os.path.exists(_cf):
                with open(_cf, 'r', encoding='utf-8') as f: cred_json = f.read()
        if not cred_json: print("  ⚠️ Firebase憑證未設定"); return False
        import google.oauth2.service_account as _sa, google.auth.transport.requests as _gtr
        _c = _sa.Credentials.from_service_account_info(json.loads(cred_json),
            scopes=['https://www.googleapis.com/auth/datastore'])
        _c.refresh(_gtr.Request())
        _now = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M')
        _url = (f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
                f"/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}/public/tw_prescreened")
        _r = _req.patch(_url, timeout=15,
            headers={"Authorization": f"Bearer {_c.token}", "Content-Type": "application/json"},
            json={"fields": {
                "codes": {"arrayValue": {"values": [{"stringValue": c} for c in codes_list]}},
                "count": {"integerValue": str(len(codes_list))},
                "updated_at": {"stringValue": _now}}})
        if _r.status_code in (200,201):
            print(f"  ✅ Firebase 預篩清單已更新：{len(codes_list)} 支 ({_now})"); return True
        print(f"  ❌ 預篩寫入失敗：{_r.status_code}"); return False
    except Exception as e: print(f"  ⚠️ 預篩寫入異常：{e}"); return False


def write_alerts_to_firebase(delist_list, cash_list):
    """寫入下市警報+全額交割到Firebase"""
    try:
        import json, os, requests as _req
        from datetime import datetime; import pytz
        cred_json = os.environ.get(FIREBASE_CRED_ENV)
        if not cred_json:
            _cf = os.path.join(os.path.dirname(os.path.abspath(__file__)), FIREBASE_CRED_FILE)
            if os.path.exists(_cf):
                with open(_cf, 'r', encoding='utf-8') as f: cred_json = f.read()
        if not cred_json: return False
        import google.oauth2.service_account as _sa, google.auth.transport.requests as _gtr
        _c = _sa.Credentials.from_service_account_info(json.loads(cred_json),
            scopes=['https://www.googleapis.com/auth/datastore'])
        _c.refresh(_gtr.Request())
        _now = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M')
        _data = json.dumps({
            'delist': [{'market':s[0],'code':s[1],'type':s[2],'msg':s[3]} for s in delist_list],
            'cash':   [{'code':c} for c in cash_list],
            'updated_at': _now}, ensure_ascii=False)
        _url = (f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
                f"/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}/public/alerts_cache")
        _r = _req.patch(_url, timeout=15,
            headers={"Authorization": f"Bearer {_c.token}", "Content-Type": "application/json"},
            json={"fields": {"data": {"stringValue": _data}}})
        if _r.status_code in (200,201):
            print(f"  ✅ Firebase 警報快取已更新（下市:{len(delist_list)}支 全額:{len(cash_list)}支）({_now})")
            return True
    except Exception as e: print(f"  ⚠️ 警報快取異常：{e}")
    return False


def read_tw_prescreened():
    """從Firebase讀取台股預篩清單（只需讀取public路徑）"""
    try:
        import json, os
        import requests as _req

        # 取得 Firebase 服務帳號憑證（優先環境變數，次選本機檔案）
        cred_json = os.environ.get(FIREBASE_CRED_ENV)
        if not cred_json:
            cred_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), FIREBASE_CRED_FILE)
            if os.path.exists(cred_file):
                with open(cred_file, 'r', encoding='utf-8') as f:
                    cred_json = f.read()

        if not cred_json:
            return None  # 無憑證，跳過

        cred = json.loads(cred_json)
        import google.oauth2.service_account as _sa
        import google.auth.transport.requests as _gtr
        credentials = _sa.Credentials.from_service_account_info(
            cred, scopes=['https://www.googleapis.com/auth/datastore']
        )
        credentials.refresh(_gtr.Request())
        token = credentials.token

        url = (
            f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
            f"/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}/public/tw_prescreened"
        )
        resp = _req.get(url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=10
        )
        if resp.status_code == 200:
            fields = resp.json().get('fields', {})
            codes_raw = fields.get('codes', {}).get('arrayValue', {}).get('values', [])
            codes = [v.get('stringValue','') for v in codes_raw if v.get('stringValue')]
            updated_at = fields.get('updated_at', {}).get('stringValue', '—')
            count = int(fields.get('count', {}).get('integerValue', 0))
            return {'codes': codes, 'updated_at': updated_at, 'count': count}
        else:
            return None
    except Exception as e:
        print(f"  ⚠️ Firebase預篩清單讀取失敗：{e}")
        return None

def scan_stock_mixed(ticker, is_holding=False):
    """混合模式：週K三道 OR 日K三道，任一通過即觸發"""
    global SCAN_MODE
    SCAN_MODE = 'weekly'
    r_w = scan_stock(ticker, is_holding)
    SCAN_MODE = 'mixed'
    if r_w and r_w[0] in ('BUY', 'SHORT'):
        return r_w + ('長期投資',)
    SCAN_MODE = 'daily'
    r_d = scan_stock(ticker, is_holding)
    SCAN_MODE = 'mixed'
    if r_d and r_d[0] in ('BUY', 'SHORT'):
        return r_d + ('中期投資',)
    for r in (r_w, r_d):
        if r and r[0] in ('DELIST_HOLD', 'DELIST_WATCH'):
            return r
    return None


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
    print(f"  股票買進賣出建議系統啟動（{"期貨5分K模式" if TEST_MODE == chr(39)+"5mk"+chr(39) else f'{SCAN_MODE}模式'}）  v{SCRIPT_VERSION}")
    print(f"  掃描時間：{now_str}")
    print(f"  本次掃描市場：{', '.join(active_markets)}")
    print(f"{'='*55}")

    # 掃描前預先抓取全額交割清單（快取後不重複抓）
    _skip_cash = (TEST_MODE == '5mk') or ('TW' not in active_markets and 'CRYPTO' not in active_markets)
    if ENABLE_CASH_DELIVERY_CHECK and not _skip_cash:
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
        # 🟢 台股大盤守門員（A/B/C/D多空全條件）
        print('\n🔍 正在檢查台股大盤位階 (^TWII)...')
        _tse_mkt = analyse_market_index('^TWII', '台股')
        _tw_market_bull_abc = _tse_mkt['bull_abc']
        _tw_market_bull_d   = _tse_mkt['bull_d']
        _tw_market_bear_abc = _tse_mkt['bear_abc']
        _tw_market_bear_d   = _tse_mkt['bear_d']
        _tw_market_warn     = _tse_mkt['warn']

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

        # ── 優先從Firebase讀取預篩快取（避免每次掃1800支）──────────
        _fb_cache = read_tw_prescreened()
        if _fb_cache and len(_fb_cache.get('codes', [])) > 0:
            _cached_codes = _fb_cache['codes']
            _cache_time   = _fb_cache.get('updated_at', '—')
            _ticker_map   = {t.split('.')[0]: t for t in tw_list}
            tw_list       = [_ticker_map[c] for c in _cached_codes if c in _ticker_map]
            print(f'\n📊 台股：使用 Firebase AI預篩快取（更新：{_cache_time}）')
            print(f'   快取共 {len(_cached_codes)} 支 → 本次掃描 {len(tw_list)} 支')
        else:
            print(f'\n📊 台股：Firebase無預篩快取，執行完整掃描')

        total_tw = len(tw_list)
        print(f'📊 台股掃描：共{total_tw}支')
        _tw_prescreened = []

        for i, ticker in enumerate(tw_list):
            if (i+1) % 50 == 0:
                print(f'  進度：{i+1}/{total_tw}...')
        
            # --- [優化: 加入 try...except 容錯, 避免網路閃斷中斷掃描] ---
            try:
                is_holding = ticker in holdings_tw_full
                if SCAN_MODE == 'mixed':
                    result_raw = scan_stock_mixed(ticker, is_holding)
                    _mlabel = result_raw[-1] if result_raw and isinstance(result_raw[-1], str) and result_raw[-1] in ('長期投資','中期投資') else None
                    result = result_raw[:-1] if _mlabel else result_raw
                else:
                    result = scan_stock(ticker, is_holding)
                    _mlabel = '中期投資' if SCAN_MODE == 'daily' else '長期投資'
            
                code = ticker.split('.')[0]
                # ✅ 第一道通過（result非None）→ 加入預篩快取
                # 修正前：只有BUY/SELL才加入，導致預篩永遠空
                # 修正後：只要第一道通過就加入，下次掃描可縮短至幾分鐘
                if result is not None:
                    if code not in _tw_prescreened: _tw_prescreened.append(code)
                if result:
                    if result[0] == 'BUY':
                        # ✅ 大盤條件匹配：個股BUY路線需和大盤一致才發通知
                        _stock_is_d = getattr(result, '_condD', False)  # 個股是否條件D
                        _mkt_match = (
                            (_tw_market_bull_d   and _stock_is_d) or
                            (_tw_market_bull_abc and not _stock_is_d)
                        ) if '_tw_market_bull_abc' in dir() else True
                        if _mkt_match:
                            buy_signals.append(('台股', code, *result[1:], _mlabel if _mlabel else ''))
                        else:
                            print(f'  ⚠️ {code} 個股觸發但大盤條件不符，不發通知')
                    elif result[0] == 'SELL':
                        _stock_is_d_s = getattr(result, '_condD_short', False)
                        _mkt_match_s = (
                            (_tw_market_bear_d   and _stock_is_d_s) or
                            (_tw_market_bear_abc and not _stock_is_d_s)
                        ) if '_tw_market_bear_abc' in dir() else True
                        if _mkt_match_s:
                            sell_signals.append(('台股', code, *result[1:]))
                        else:
                            print(f'  ⚠️ {code} 個股空頭觸發但大盤條件不符，不發通知')
                    elif result[0] in ('DELIST_HOLD', 'DELIST_WATCH'):
                        delist_signals.append(('台股', code, result[0], result[1]))
            except Exception as e:
            # 發生錯誤時僅列印警告並跳過該支股票
                print(f'  ⚠️ 跳過 {ticker} 掃描異常: {e}')
                continue
            time.sleep(0.1) # 稍微加快掃描速度

        if _tw_prescreened:
            print(f'\n🔍 正在將 {len(_tw_prescreened)} 支預篩台股寫入Firebase...')
            write_tw_prescreened(_tw_prescreened)
        else:
            print('\n🔍 本次預篩：無台股通過條件')

# ── 美股掃描（加入道瓊週K過濾，對應截圖轉折邏輯） ──────────
    if TEST_MODE == '5mk':
        print('\n📊 美股：期貨5分K模式，跳過美股掃描')
    elif 'US' not in active_markets:
        print('\n📊 美股：非交易時段，跳過')
    else:
        # 🔵 美股大盤守門員（A/B/C/D多空全條件）
        print('\n🔍 正在檢查道瓊指數位階 (^DJI)...')
        _dji_mkt = analyse_market_index('^DJI', '道瓊')
        _us_market_bull_abc = _dji_mkt['bull_abc']
        _us_market_bull_d   = _dji_mkt['bull_d']
        _us_market_bear_abc = _dji_mkt['bear_abc']
        _us_market_bear_d   = _dji_mkt['bear_d']
        _us_market_warn     = _dji_mkt['warn']

        print(f'\n📊 美股掃描：共{len(US_STOCKS)}支')
        for ticker in US_STOCKS:
            is_holding = ticker in HOLDINGS_US
            if SCAN_MODE == 'mixed':
                result_raw = scan_stock_mixed(ticker, is_holding)
                _mlabel = result_raw[-1] if result_raw and isinstance(result_raw[-1], str) and result_raw[-1] in ('長期投資','中期投資') else None
                result = result_raw[:-1] if _mlabel else result_raw
            else:
                result = scan_stock(ticker, is_holding)
                _mlabel = '中期投資' if SCAN_MODE == 'daily' else '長期投資'
            
            # ✅ 正確：result 取得後立刻判斷
            if result:
                if result[0] == 'BUY':
                    _stock_is_d_us = getattr(result, '_condD', False)
                    _mkt_match_us = (
                        (_us_market_bull_d   and _stock_is_d_us) or
                        (_us_market_bull_abc and not _stock_is_d_us)
                    ) if '_us_market_bull_abc' in dir() else True
                    if _mkt_match_us:
                        buy_signals.append(('美股', ticker, *result[1:], _mlabel if _mlabel else ''))
                    else:
                        print(f'  ⚠️ {ticker} 美股個股觸發但大盤條件不符，不發通知')
                elif result[0] == 'SELL':
                    _stock_is_d_us_s = getattr(result, '_condD_short', False)
                    _mkt_match_us_s = (
                        (_us_market_bear_d   and _stock_is_d_us_s) or
                        (_us_market_bear_abc and not _stock_is_d_us_s)
                    ) if '_us_market_bear_abc' in dir() else True
                    if _mkt_match_us_s:
                        sell_signals.append(('美股', ticker, *result[1:]))
                    else:
                        print(f'  ⚠️ {ticker} 美股個股空頭觸發但大盤條件不符，不發通知')
                elif result[0] in ('DELIST_HOLD', 'DELIST_WATCH'):
                    delist_signals.append(('美股', ticker, result[0], result[1]))
            time.sleep(0.1)

        # 🔥 正確位置：美股「所有股票」掃描完後，才執行「一次」基金追蹤
        # 確保此行與 for 垂直對齊（不縮排進去）
        scan_synthetic_fund("SPY / QQQ / HYG三合一追蹤【安聯月配息基金】(合成代標)")

        # ── 債券基金（EMB/AGG）獨立掃描（各自走台股/美股相同流程）──
        for _bond_ticker in ['EMB', 'AGG']:
            _bond_name = {'EMB': '新興市場債券ETF（參考FR02）', 'AGG': '全球綜合債券ETF（參考FR04）'}.get(_bond_ticker, _bond_ticker)
            _r_bond = scan_stock(_bond_ticker, is_holding=False)
            if _r_bond and _r_bond[0] == 'BUY':
                buy_signals.append(('債券基金', _bond_ticker, *_r_bond[1:], '長期投資' if SCAN_MODE=='weekly' else '中期投資'))
                print(f'⭐ 債券基金 {_bond_ticker}（{_bond_name}）觸發買進訊號')
            elif _r_bond and _r_bond[0] == 'SELL':
                sell_signals.append(('債券基金', _bond_ticker, *_r_bond[1:]))
                print(f'🔔 債券基金 {_bond_ticker}（{_bond_name}）觸發賣出訊號')

    # ── 虛擬幣掃描 ────────────────────────────────
    if TEST_MODE == '5mk':
        print('\n📊 虛擬幣：期貨5分K模式，跳過虛擬幣掃描')
    elif 'CRYPTO' not in active_markets:
        print('\n📊 虛擬幣：非交易時段，跳過')
    else:
        print(f'\n📊 虛擬幣掃描：共{len(CRYPTO_LIST)}支')
        for ticker in CRYPTO_LIST:
            is_holding = ticker in HOLDINGS_CRYPTO
            if SCAN_MODE == 'mixed':
                result_raw = scan_stock_mixed(ticker, is_holding)
                _mlabel = result_raw[-1] if result_raw and isinstance(result_raw[-1], str) and result_raw[-1] in ('長期投資','中期投資') else None
                result = result_raw[:-1] if _mlabel else result_raw
            else:
                result = scan_stock(ticker, is_holding)
                _mlabel = '中期投資' if SCAN_MODE == 'daily' else '長期投資'
            if result:
                # 【更正處】將原本刪掉的此行「name = ticker.replace('-USD', '')」 下行的買進和賣出全部改為 ticker
                if result[0] == 'BUY':
                    buy_signals.append(('虛擬幣', ticker, *result[1:], _mlabel if _mlabel else ''))
                elif result[0] == 'SELL':
                    sell_signals.append(('虛擬幣', ticker, *result[1:]))
                elif result[0] in ('DELIST_HOLD', 'DELIST_WATCH'):
                    delist_signals.append(('虛擬幣', ticker, result[0], result[1]))
            time.sleep(0.1) # 稍微加快掃描速度


    # ── 外匯掃描（做多+做空）────────────────────────────────────
    if TEST_MODE == '5mk':
        print('\n📊 外匯：期貨5分K模式，跳過')
    elif 'US' not in active_markets and 'CRYPTO' not in active_markets:
        print('\n📊 外匯：非交易時段，跳過')
    else:
        print(f'\n📊 外匯掃描：共{len(FX_LIST)}支（做多+做空）')
        for ticker in FX_LIST:
            is_holding = ticker in HOLDINGS_FX
            if SCAN_MODE == 'mixed':
                result_raw = scan_stock_mixed(ticker, is_holding)
                _mlabel = result_raw[-1] if result_raw and isinstance(result_raw[-1], str) and result_raw[-1] in ('長期投資','中期投資') else None
                result = result_raw[:-1] if _mlabel else result_raw
            else:
                result = scan_stock(ticker, is_holding)
                _mlabel = '中期投資' if SCAN_MODE == 'daily' else '長期投資'
            if result:
                if result[0] == 'BUY':
                    buy_signals.append(('外匯', ticker, *result[1:], _mlabel if _mlabel else ''))
                elif result[0] == 'SELL':
                    sell_signals.append(('外匯', ticker, *result[1:]))
                elif result[0] == 'SHORT':
                    sell_signals.append(('外匯做空', ticker, *result[1:]))
            time.sleep(0.1)

    # ── 期貨5分K掃描（TEST_MODE='5mk'且FUTURES在active_markets時執行）──
    if 'FUTURES' in active_markets and TEST_MODE == '5mk':
        # ✅ 持倉狀態：多倉/空倉分開追蹤
        if '_futures_is_holding' not in dir(): _futures_is_holding = False
        if '_futures_is_short'   not in dir(): _futures_is_short   = False
        _pos_str = '多倉🔴' if _futures_is_holding else ('空倉🔵' if _futures_is_short else '空手⬜')
        _mode_str = f'SCAN_MODE={SCAN_MODE}' if 'SCAN_MODE' in dir() else SCAN_MODE
        print(f'\n📊 期貨5分K掃描：{FUTURES_5MK_TARGETS}')
        print(f'   持倉狀態：{_pos_str}　掃描模式：{_mode_str}')
        for ticker in FUTURES_5MK_TARGETS:
            try:
                # ══════════════════════════════════════════════
                # 5分K掃描：RSI + MACD柱 轉折判斷
                # 日K門檻：參考用（印出日K位階供參考，但不強制擋住）
                # ══════════════════════════════════════════════
                # ══════════════════════════════════════════════
                # ✅【三道關卡】期貨5分K進場先決條件
                # 第一道：週K位階（check_buy_precondition，BUY_LOOKBACK_BARS=3根）
                # 第二道：日K eLeader條件（check_buy_eleader）
                # 第三道：5分K 54根條件A/B + RSI門檻（BUY_LOOKBACK_5MK=54根）
                # ══════════════════════════════════════════════

                # ── 第一道：日K 3根位階──
                df5_d = get_stock_data(ticker, period='6mo', interval='1d', cache=daily_cache)
                if df5_d is None or len(df5_d) < 50:
                    print(f'  ⚠️ {ticker} 日K資料不足，跳過')
                    continue
                df5_d = calc_indicators(df5_d)
                if df5_d is None: continue
                _orig5 = BUY_LOOKBACK_BARS
                globals()['BUY_LOOKBACK_BARS'] = BUY_LOOKBACK_DAILY
                _1st_long  = check_buy_precondition(df5_d)
                _1st_short = check_short_precondition(df5_d)
                globals()['BUY_LOOKBACK_BARS'] = _orig5
                if not _1st_long and not _1st_short:
                    print(f'  ❌ {ticker} 第一道日K3根未通過，跳過'); continue
                print(f'  ✅ {ticker} 第一道日K3根通過（多:{_1st_long} 空:{_1st_short}）')

                # ── 第二道：日K eLeader ──────────────────────
                # （df5_d已在上面取得，直接使用）
                if check_buy_eleader(df5_d) is None:
                    print(f'  ❌ {ticker} 第二道日K eLeader未通過，跳過')
                    continue
                print(f'  ✅ {ticker} 第二道日K eLeader通過')

                # ── 第三道：5分K 54根條件A/B ────────────────
                # ✅【夜盤保留】主力以夜盤為主戰場，不剔除夜盤
                # period='5d' 確保足夠近期夜盤+日盤K棒（約1000+根）
                df5 = yf.download(ticker, period='5d', interval='5m', progress=False)
                if df5 is None or df5.empty or len(df5) < BUY_LOOKBACK_5MK + 2:
                    print(f'  ⚠️ {ticker} 5分K資料不足（需>={BUY_LOOKBACK_5MK+2}根），跳過')
                    continue
                df5 = calc_indicators(df5)
                if df5 is None:
                    continue

                # ✅【K棒數量條件A/B】使用BUY_LOOKBACK_5MK=54根，與週K條件邏輯一致
                n5 = BUY_LOOKBACK_5MK
                l5   = df5['Low']
                h5   = df5['High']
                bb5  = df5['boll_bot20']
                bt5  = df5['boll_top20']
                bm5  = df5['ma_c_20']
                mh5  = df5['macd_hist']
                rsi5 = df5['rsi14']

                rsi_now  = float(rsi5.iloc[-1])
                rsi_prev = float(rsi5.iloc[-2])
                mh_now   = float(mh5.iloc[-1])
                mh_prev  = float(mh5.iloc[-2])
                close    = float(df5['Close'].iloc[-1])
                boll_bot = float(bb5.iloc[-1])
                boll_top = float(bt5.iloc[-1])

                rsi_rising   = rsi_now > rsi_prev
                macd_rising  = mh_now  > mh_prev
                rsi_falling  = rsi_now < rsi_prev
                macd_falling = mh_now  < mh_prev

                # ── 條件A：近54根任一最低價<=布林下緣 AND RSI↑ AND MACD柱↑
                _5mk_cond_A = (l5.iloc[-n5:] <= bb5.iloc[-n5:] * BUY_BOLL_TOLERANCE).any() and rsi_rising and macd_rising
                # ── 條件B：近54根最低均<布林中軌 AND 最高均<布林上軌 AND 前N根MACD縮 AND 當根MACD放大
                _5mk_low_mid  = (l5.iloc[-n5:] < bm5.iloc[-n5:]).all()
                _5mk_high_top = (h5.iloc[-n5:] < bt5.iloc[-n5:]).all()
                _5mk_macd_shr = len(mh5) >= n5+1 and all(float(mh5.iloc[-n5-1+j]) > float(mh5.iloc[-n5+j]) for j in range(n5-1))
                _5mk_cond_B   = _5mk_low_mid and _5mk_high_top and _5mk_macd_shr and macd_rising
                _5mk_buy = (_5mk_cond_A or _5mk_cond_B) and rsi_now > BUY_RSI_MIN

                near_lower   = close  <= boll_bot * BUY_BOLL_TOLERANCE
                near_upper   = close  >= boll_top * 1.00
                now_str_f = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M')
                # ── 日K位階參考（印出但不擋住掃描）──────────────
                try:
                    df_d5 = yf.download(ticker, period='10d', interval='1d', progress=False)
                    if df_d5 is not None and not df_d5.empty and len(df_d5) >= 5:
                        df_d5 = calc_indicators(df_d5)
                        if df_d5 is not None:
                            _daily_buy  = check_buy_precondition(df_d5)
                            _daily_sell = check_sell_condition(df_d5)
                            _zone = '低檔✅' if _daily_buy else ('高檔⚠️' if _daily_sell else '中軌區間')
                            print(f'  ℹ️ {ticker} 日K位階：{_zone}（供參考）')
                except: pass

                # ══════════════════════════════════════════════
                # ✅【做空三道關卡】鏡像多方，捕捉高檔反轉訊號
                # 第一道：週K觸碰上軌（check_short_precondition）
                # 第二道：日K eLeader 25條件全部反向
                # 第三道：5分K 54根條件A/B反向 + RSI↓ AND MACD↓
                # ══════════════════════════════════════════════
                _short_1st = _1st_short  # ✅ 已在第一道判斷完成
                _short_2nd = False
                if _short_1st:
                    _short_2nd = check_short_eleader(df5_d) is not None
                # 第三道做空條件A/B（鏡像多方）
                _5mk_short_A = (h5.iloc[-n5:] >= bt5.iloc[-n5:] * 1.00).any() and rsi_falling and macd_falling
                _5mk_high_mid  = (h5.iloc[-n5:] > bm5.iloc[-n5:]).all()
                _5mk_low_bot   = (l5.iloc[-n5:] > bb5.iloc[-n5:]).all()
                _5mk_macd_exp  = len(mh5) >= n5+1 and all(float(mh5.iloc[-n5-1+j]) < float(mh5.iloc[-n5+j]) for j in range(n5-1))
                _5mk_short_B   = _5mk_high_mid and _5mk_low_bot and _5mk_macd_exp and macd_falling
                _5mk_short = _short_1st and _short_2nd and (_5mk_short_A or _5mk_short_B) and rsi_now < (100 - BUY_RSI_MIN)

                _is_night_now = (now_str_f[11:16] >= '01:00' and now_str_f[11:16] < '05:00')
                # ✅ 深夜01~05有持倉：只掃平倉，跳過買進
                if _5mk_buy and near_lower and not (_is_night_now and _futures_is_holding):
                    # ── 5分鐘內最多2封上限 ──────────────────────
                    _now_ts = time.time()
                    if not hasattr(send_gmail, '_futures_log'): send_gmail._futures_log = []
                    send_gmail._futures_log = [t for t in send_gmail._futures_log if _now_ts - t < 300]
                    if len(send_gmail._futures_log) >= 2:
                        print(f"  ⚠️ {ticker} 5分鐘內已發2封，跳過（防吵機制）"); pass
                    else:
                        send_gmail._futures_log.append(_now_ts)
                        msg = (
                            f"☁️【雲端】⭐【期貨5分K買進訊號】⭐\n"
                            f"標的：{ticker}\n"
                            f"收盤：{close:.2f}　布林下緣：{boll_bot:.2f}\n"
                            f"RSI：{rsi_prev:.1f} → {rsi_now:.1f}（↑）\n"
                            f"時間：{now_str_f}"
                        )
                        _ok = send_gmail(f"☁️【雲端】⭐期貨5分K買進 {ticker} - {now_str_f}", msg)
                        print(f"  {'✅' if _ok else '❌'} {ticker} 5分K買進訊號{'已發送' if _ok else '發送失敗'}")
                        # ✅ 方案A：買進訊號發出 → 記錄持倉狀態
                        _futures_is_holding = True
                        print(f"  📌 持倉狀態已標記：is_futures_holding=True")

                elif rsi_falling and macd_falling and near_upper:
                    # ── 5分鐘內最多2封上限 ──────────────────────
                    _now_ts = time.time()
                    if not hasattr(send_gmail, '_futures_log'): send_gmail._futures_log = []
                    send_gmail._futures_log = [t for t in send_gmail._futures_log if _now_ts - t < 300]
                    if len(send_gmail._futures_log) >= 2:
                        print(f"  ⚠️ {ticker} 5分鐘內已發2封，跳過（防吵機制）"); pass
                    else:
                        send_gmail._futures_log.append(_now_ts)
                        msg = (
                            f"☁️【雲端】🔔【期貨5分K平倉訊號】🔔\n"
                            f"標的：{ticker}\n"
                            f"收盤：{close:.2f}　布林上緣：{boll_top:.2f}\n"
                            f"RSI：{rsi_prev:.1f} → {rsi_now:.1f}（↓）\n"
                            f"時間：{now_str_f}"
                        )
                        _ok = send_gmail(f"☁️【雲端】🔔期貨5分K平倉 {ticker} - {now_str_f}", msg)
                        print(f"  {'✅' if _ok else '❌'} {ticker} 5分K平倉訊號{'已發送' if _ok else '發送失敗'}")
                        # ✅ 方案A：平倉訊號發出 → 清除持倉狀態
                        _futures_is_holding = False
                        print(f"  📌 持倉狀態已清除：is_futures_holding=False")
                # ✅【做空訊號】三道關卡通過且接近布林上軌，且深夜無空倉
                elif _5mk_short and near_upper and not (_is_night_now and _futures_is_short):
                    _now_ts = time.time()
                    if not hasattr(send_gmail, '_futures_log'): send_gmail._futures_log = []
                    send_gmail._futures_log = [t for t in send_gmail._futures_log if _now_ts - t < 300]
                    if len(send_gmail._futures_log) >= 2:
                        print(f"  ⚠️ {ticker} 5分鐘內已發2封，跳過（防吵機制）")
                    else:
                        send_gmail._futures_log.append(_now_ts)
                        msg = (
                            f"☁️【雲端】🔻【期貨5分K做空訊號】🔻\n"
                            f"標的：{ticker}\n"
                            f"收盤：{close:.2f}　布林上軌：{boll_top:.2f}\n"
                            f"RSI：{rsi_prev:.1f} → {rsi_now:.1f}（↓）\n"
                            f"時間：{now_str_f}"
                        )
                        _ok = send_gmail(f"☁️【雲端】🔻期貨5分K做空 {ticker} - {now_str_f}", msg)
                        print(f"  {'✅' if _ok else '❌'} {ticker} 做空訊號{'已發送' if _ok else '發送失敗'}")
                        _futures_is_short   = True
                        _futures_is_holding = False  # 做空時清除多倉
                        print(f"  📌 空倉狀態已標記：is_futures_short=True")

                # ✅【空倉回補（平空）】RSI↑ AND MACD柱↑ AND 近布林下軌 → 回補平倉
                elif _futures_is_short and _5mk_buy and near_lower:
                    _now_ts = time.time()
                    if not hasattr(send_gmail, '_futures_log'): send_gmail._futures_log = []
                    send_gmail._futures_log = [t for t in send_gmail._futures_log if _now_ts - t < 300]
                    if len(send_gmail._futures_log) >= 2:
                        print(f"  ⚠️ {ticker} 5分鐘內已發2封，跳過（防吵機制）")
                    else:
                        send_gmail._futures_log.append(_now_ts)
                        msg = (
                            f"☁️【雲端】🟢【期貨5分K平空回補】🟢\n"
                            f"標的：{ticker}\n"
                            f"收盤：{close:.2f}　布林下軌：{boll_bot:.2f}\n"
                            f"RSI：{rsi_prev:.1f} → {rsi_now:.1f}（↑）\n"
                            f"時間：{now_str_f}"
                        )
                        _ok = send_gmail(f"☁️【雲端】🟢期貨5分K平空回補 {ticker} - {now_str_f}", msg)
                        print(f"  {'✅' if _ok else '❌'} {ticker} 平空回補訊號{'已發送' if _ok else '發送失敗'}")
                        _futures_is_short = False
                        print(f"  📌 空倉狀態已清除：is_futures_short=False")

                else:
                    _pos = '多倉中' if _futures_is_holding else ('空倉中' if _futures_is_short else '空手')
                    print(f"  ℹ️ {ticker} 5分K：RSI={rsi_now:.1f}({'↑' if rsi_rising else '↓'})  MACD={'↑' if macd_rising else '↓'}  位置：{_pos}")
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
                market, code, c, l, bb, r, rp = s[:7]
                _ml = s[7] if len(s) > 7 and isinstance(s[7], str) and s[7] in ('長期投資','中期投資') else ('中期投資' if SCAN_MODE=='daily' else '長期投資')
                body += (
                    f"⭐【做多進場】⭐\n"
                    f"市場：{market}　代碼：{code}\n"
                    f"投資模式：{_ml}\n"
                    f"最低價：{l:.2f}　布林下緣：{bb:.2f}\n"
                    f"收盤價：{c:.2f}\n"
                    f"RSI：{rp:.1f} → {r:.1f}（↑上升）\n"
                    f"⚠️ 嚴禁用於當沖或隔日沖\n"
                    f"{'─'*30}\n"
                )

            send_gmail(f"☁️【雲端】⭐做多進場 {len(filtered)}支 - {now_str}", body)
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
                    f"🔔【{"做多出場" if market in ("台股","基金") else "做空進場/做多出場"}】🔔\n"
                    f"市場：{market}　代碼：{code}\n"
                    f"最高價：{h:.2f}　布林上緣：{bt:.2f}\n"
                    f"收盤價：{c:.2f}\n"
                    f"RSI：{rp:.1f} → {r:.1f}（↓下降）\n"
                    f"⚠️ 台股→做多出場；外匯/期貨→請依持倉方向判斷\n"
                    f"{'─'*30}\n"
                )

            send_gmail(f"☁️【雲端】🔔出場訊號 {len(filtered)}支 - {now_str}", body)
            save_notified(notified)
        else:
            print(f"🔕 賣出訊號 {len(sell_signals)-len(filtered)} 支已通知過")

# =====================
# 無訊號（只印Console，不發Gmail，避免通知疲乏）
# =====================
    if not buy_signals and not sell_signals and not delist_signals:
        print(f"📊 [{now_str}] 本次掃描無符合條件的股票，不發送通知。")

    print("\n✅ 全部完成！有訊號才會收到Gmail通知。")

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
        # 檢查是否在期貨交易時段
        tz_f = pytz.timezone('Asia/Taipei')
        now_f = datetime.now(tz_f)
        wd_f  = now_f.weekday()
        tv_f  = now_f.hour * 60 + now_f.minute

        in_futures = (
            (wd_f == 0 and tv_f >= 13*60) or                             # 週一13:00後
            (wd_f == 1 and (tv_f < 1*60 or tv_f >= 5*60)) or            # 週二（排除深夜01:00~05:00）
            (wd_f == 2 and tv_f <= 11*60+30)                             # 週三11:30前
        )

        if not in_futures:
            print(f"[{test_now}] ❌ 非期貨5分K時段（週一13:00~週三11:30，深夜01~05除外），直接結束")
            time.sleep(5)
            exit()

        print(f"🚀 期貨5分K模式啟動（週一13:00~週三11:30）")
        print(f"   標的：{FUTURES_5MK_TARGETS}　間隔：{FUTURES_5MK_INTERVAL}秒")
        print(f"   專屬帳號：{FUTURES_5MK_OWNER}")

        # 持續掃描直到週三11:30結束
        while True:
            now_loop = datetime.now(pytz.timezone('Asia/Taipei'))
            wd_l  = now_loop.weekday()
            tv_l  = now_loop.hour * 60 + now_loop.minute
            _is_night = (wd_l == 1 and 1*60 <= tv_l < 5*60)  # 週二深夜01~05
            in_f  = (
                (wd_l == 0 and tv_l >= 13*60) or
                (wd_l == 1 and (tv_l < 1*60 or tv_l >= 5*60)) or  # 排除深夜01:00~05:00
                (wd_l == 2 and tv_l <= 11*60+30) or
                (_is_night and _futures_is_holding)  # ✅ 深夜有持倉→繼續掃平倉
            )
            if not in_f:
                print(f"✅ 期貨5分K時段結束（週三11:30），監控結束")
                break
            try:
                main_task()
            except Exception as e:
                print(f"掃描發生錯誤: {e}")
            print(f"😴 休息 {FUTURES_5MK_INTERVAL} 秒（5分鐘）...")
            time.sleep(FUTURES_5MK_INTERVAL)
        exit()

    # === [精準測試模式]：不受交易時間限制，發送兩封關鍵測試信後即結束 ===
    if TEST_MODE:
        print(f"\n🚀 [{test_now}] 啟動通知權限測試 (不混淆模式)...")
        
        # 1. 驗證【買進標籤】：預期會觸發「響鈴 + 重要標籤 + 黃色星號」
        send_gmail(f"🚨 【測試】買進訊號 - {test_now}", 
                   "此為 Gmail 篩選器測試信，請確認手機是否響鈴、是否有黃色星星。")
        
        # 2. 驗證【週K報告標籤】：預期會觸發「響鈴 + 重要標籤 + 無星號」
        send_gmail(f"📊 【測試】掃描完成 - {test_now}", 
                   "此為例行報告測試信，請確認手機是否響鈴，且不應該有星星。")
        
        print("\n✅ 精準測試信已發出。")
        print("💡 確認手機通知與星星正確後，請將第一章節的 TEST_MODE 改為 False 存檔。")
        print("💡 晚上 21:30 再次執行 .bat 即可進入正式監控模式。")
        time.sleep(10)
        exit()

    # === [正式監控模式]：TEST_MODE 為 False 時才會進入以下邏輯 ===
    hour   = now.hour
    minute = now.minute  # ✅ minute 必須定義，否則時段判斷會 NameError
    loops = 0
    market_name = ""

    # 台股：開盤後5分(09:05) ~ 收盤前5分(13:25)
    if (hour == 9 and minute >= 5) or (10 <= hour <= 12) or (hour == 13 and minute <= 25):
        loops = 9
        market_name = "台股"
    # 美股：開盤後5分(22:35) ~ 收盤前5分(03:55)，夏冬令統一此區間
    elif (hour == 22 and minute >= 35) or (hour == 23) or (0 <= hour <= 2) or (hour == 3 and minute <= 55):
        loops = 13
        market_name = "美股/虛擬幣"
    elif now.weekday() == 5:  # 週六：執行一次台股預篩快取，確保週五若失敗能補跑
        # 確認今天是否已執行過（用 4_notified_today.json 記錄）
        import json, os
        _sat_flag_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '5_saturday_scan.json')
        _today_str = now.strftime('%Y-%m-%d')
        _sat_done = False
        if os.path.exists(_sat_flag_file):
            try:
                _sat_data = json.load(open(_sat_flag_file, 'r', encoding='utf-8'))
                if _sat_data.get('date') == _today_str:
                    _sat_done = True
            except: pass
        if _sat_done:
            print(f"[{now.strftime('%H:%M:%S')}] ✅ 週六預篩已執行過，今日不再重複")
            time.sleep(10)
            exit()
        print(f"[{now.strftime('%H:%M:%S')}] 📅 週六補跑：執行一次台股預篩快取上傳")
        loops = 1
        market_name = '台股（週六補跑）'
        # 執行完後記錄今天已執行
        try:
            json.dump({'date': _today_str}, open(_sat_flag_file, 'w', encoding='utf-8'), ensure_ascii=False)
        except: pass
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
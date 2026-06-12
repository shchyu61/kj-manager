SCRIPT_VERSION = '06130522'
# ============================================================
# 專案：Python股票週K布林RSI+Gmail推播自動通知
# 版本：(由AI每次改版時自動填寫)
# 更新日期：(由AI每次改版時依照對話視窗提供的日期,並經由使用者確認後為準)（新增期貨5分K模式：TEST_MODE="5mk"，週一12:50~週三11:30）
# 適用：台股1845支 + 美股38支 + 虛擬幣3支 + 三合一追蹤【安聯月配息基金】
# 通知方式：Gmail發信到 shchyu61@gmail.com
# 重要：本程式原則上只適用週K，可視情況用於日K以下
# =========================
# 快取設定（週K決定要不要看，日K決定準不準，5分K決定何時動手）
USE_CACHE = True
WEEKLY_REFRESH_HOUR = 8   # 每天早上更新一次月K快取（long-term）
five_min_cache = {}  # ✅ 新增 5 分鐘 K 線快取容器
DELISTING_CHECK_DAYS = 1  # 每1天重新查詢（避免誤快取）（3天兼顧效能與即時性，可改1~7）
DELISTING_FILE = '2_delisting_cache.json'  # 下市風險本地快取檔
# ============================================================
# 【１．設定區】

# 驗證篩選模式：
#   False  = 正式模式（週K三層嚴格過濾，正式交易監控）
#   True   = 測試模式（只發測試Gmail確認通知是否正常）
#   '5mk'  = 期貨5分K模式（週一12:50~週三11:30，只掃台指近月）
TEST_MODE = False   # 切換：False / True / '5mk'

# ── 【期貨5分K專屬設定】（TEST_MODE = '5mk' 時才啟用）──────────
# 執行時段：週一12:50 → 週三大盤收盤11:30（日盤+夜盤+隔日日盤）
# 掃描標的：台灣加權指數（^TWII）作為台指期替代標的
# 通知對象：僅 shchyu61@gmail.com（本人專屬，家人親友不適用）
FUTURES_5MK_TARGETS  = ['^TWII']   # 期貨標的（可加入 'TXFF' 等）
FUTURES_5MK_INTERVAL = 300         # 每300秒（5分鐘）掃描一次
FUTURES_5MK_OWNER    = 'shchyu61@gmail.com'  # 5分K模式專屬帳號

# FinMind設定（財務篩選 + 集保大戶 + 法人大買）
FINMIND_TOKEN      = __import__('os').environ.get('FINMIND_TOKEN', '')  # 可選Token
FINMIND_MIN_PASS   = 4       # 通過財務篩選最低支數（低於此數不啟用）
FINMIND_CACHE_HOURS = 168    # 財務資料快取7天（週報不常更新）
_finmind_cache     = {}      # ✅ 模組頂層宣告，防止 'not defined' 錯誤

# Firebase設定（本機版：讀取AI預篩清單快取）
FIREBASE_PROJECT_ID    = 'kj-wealth-manager'
FIREBASE_CRED_ENV      = 'FIREBASE_SERVICE_KEY'         # 環境變數名稱
FIREBASE_CRED_FILE     = 'firebase_service_key.json'    # 本機金鑰檔案路徑（與.py同目錄）
# ↑ 請在本機放一份 Firebase Service Account JSON 金鑰，
#   或設定 FIREBASE_SERVICE_KEY 環境變數（內容為JSON字串）

# Gmail設定
# ✅ 💻【本機】執行：直接填入帳號密碼
# ✅ ☁️【雲端】GitHub Actions執行：自動從 GitHub Secrets 讀取，不需填寫
import os as _os
# ✅ v05190013：GitHub Actions偵測
IS_GITHUB_ACTIONS = _os.environ.get('GITHUB_ACTIONS','').lower() == 'true'
SCAN_TYPE = _os.environ.get('SCAN_TYPE', 'tw')  # 'tw'=台股, 'futures'=期貨
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
    'KO', 'O', 'D', 'CL', 'ABT', 'PG', 'SPY',
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

FX_LIST = ['EURUSD=X', 'USDTWD=X', 'GBPJPY=X', 'USDCHF=X', 'JPY=X']
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
_tw_prescreened = []   # 模組層級全域預篩清單（scan_stock 用 global 存取）
_prescreened_ind = {}  # ✅ 05041037新增：第一+第二道指標快取（供Firebase上傳）
_wk_passed_1st = False  # 週K第一道是否通過（供scan_stock_mixed判斷標籤用）
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
ENABLE_CASH_DELIVERY_CHECK = False  # ✅ v05191724：停用全額交割（TWSE/TPEX持續封鎖超過半個月）
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

weekly_cache = {} # 長期投資月K快取（原週K，v05170856改月K）
daily_cache  = {} # 中期投資週K快取（原日K，v05170856改週K）

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
        # ✅ v05282305：任何例外一律跳過下市檢查（只有明確delistingDate才真正警報）
        # 原因：NoneType/網路/資料錯誤都不應觸發下市警報（IBM大漲5%被誤報案例）
        _err_str = str(e).lower()
        _is_skip_err = any(kw in _err_str for kw in [
            # 網路錯誤
            'could not resolve', 'failed to connect', 'connection', 'timeout',
            'timed out', 'curl', 'network', 'ssl', 'name or service not known',
            # Python資料錯誤（yfinance回傳None等）
            'nonetype', 'is not iterable', 'attributeerror', 'typeerror',
            'keyerror', 'valueerror', 'indexerror', 'none'
        ]) or True  # ✅ 預設全部跳過，只信任delistingDate明確欄位
        if _is_skip_err:
            print(f'  ⚠️ {ticker} 資料異常跳過下市檢查：{str(e)[:60]}')
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
    """全額交割股 Set，重試3次，快取72小時，空白時保留舊快取
    ✅ v05170312：改善headers，新增多個備援URL，改用session維持連線
    """
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
    import requests as _req
    _sess = _req.Session()
    _sess.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    })
    def _fetch_json(url, referer=''):
        if referer: _sess.headers['Referer'] = referer
        for attempt in range(1, 4):
            try:
                r = _sess.get(url, timeout=15)
                if r.status_code == 200 and r.text.strip():
                    data = r.json()
                    if data: return data
                elif r.status_code != 200:
                    print(f'  ⚠️ 第{attempt}次失敗(HTTP {r.status_code})，{"重試中..." if attempt < 3 else "放棄"}')
                else:
                    print(f'  ⚠️ 第{attempt}次失敗(空回應)，{"重試中..." if attempt < 3 else "放棄"}')
            except Exception as e:
                print(f'  ⚠️ 第{attempt}次失敗：{e}，{"重試中..." if attempt < 3 else "放棄"}')
            if attempt < 3: _time.sleep(3)
        return None
    def _extract_codes(data, fallback_keys=('公司代號','Code','code','StockCode','CompCode')):
        result = set()
        for item in (data if isinstance(data, list) else [data]):
            for k in fallback_keys:
                v = item.get(k, '') if isinstance(item, dict) else ''
                if v: result.add(str(v).strip()); break
        return result
    # ── TWSE上市全額交割（多備援URL）
    twse_urls = [
        ('https://openapi.twse.com.tw/v1/company/cashPaymentStocks', 'https://www.twse.com.tw/'),
        ('https://www.twse.com.tw/rwd/zh/company/cashPaymentStocks', 'https://www.twse.com.tw/'),
    ]
    twse_ok = False
    for url, ref in twse_urls:
        d1 = _fetch_json(url, ref)
        if d1:
            new_c = _extract_codes(d1)
            codes.update(new_c)
            print(f'  ✅ TWSE全額交割上市：{len(new_c)} 支')
            twse_ok = True; break
    if not twse_ok:
        print(f'  ❌ TWSE全額交割：全部URL均失敗')
    # ── TPEX上櫃全額交割（多備援URL）
    before = len(codes)
    tpex_urls = [
        ('https://www.tpex.org.tw/openapi/v1/tpex_cash_payment_stocks', 'https://www.tpex.org.tw/'),
        ('https://tpex.org.tw/openapi/v1/tpex_cash_payment_stocks', 'https://www.tpex.org.tw/'),
    ]
    tpex_ok = False
    for url, ref in tpex_urls:
        d2 = _fetch_json(url, ref)
        if d2:
            new_c = _extract_codes(d2)
            codes.update(new_c)
            print(f'  ✅ TPEX全額交割上櫃：{len(codes)-before} 支')
            tpex_ok = True; break
    if not tpex_ok:
        print(f'  ❌ TPEX全額交割：全部URL均失敗')
    # ── HTML備援（美化版：抓更多選擇器）
    if not codes:
        try:
            from bs4 import BeautifulSoup as _BS
            for html_url, selector, referer in [
                ('https://www.twse.com.tw/zh/page/trading/exchange/TWTB4U.html',
                 'table td:first-child, .table-data td:first-child', 'https://www.twse.com.tw/'),
            ]:
                _sess.headers['Referer'] = referer
                r3 = _sess.get(html_url, timeout=20)
                if r3.status_code == 200 and len(r3.text) > 500:
                    soup = _BS(r3.text, 'html.parser')
                    found = set()
                    for td in soup.select(selector):
                        t = td.get_text(strip=True)
                        if t.isdigit() and 4 <= len(t) <= 6: found.add(t)
                    if found:
                        codes.update(found)
                        print(f'  ✅ 備援HTML全額交割：{len(found)} 支')
                        break
                    else:
                        print(f'  ⚠️ 備援HTML：頁面存在但未找到股票代碼（可能為動態載入）')
        except Exception as e:
            print(f'  ⚠️ 備援HTML失敗：{e}')
    # ── 更新快取
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

    # 期貨5分K時段：週一12:50~週三11:30（深夜01:00~05:00不觸發，人在睡覺）
    is_futures_time = False
    # 週一 12:50 以後
    if weekday == 0 and time_val >= 12*60+50:
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

def get_stock_data(ticker, period='5y', interval='1mo', cache=None):  # ✅ v05170856 預設改月K
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
    """✅ v05280750：本機版和GitHub都優先用Firebase（共享狀態防重複通知）"""
    # 優先從Firebase讀取（本機版和GitHub Actions共用，防止重複通知）
    try:
        fb_data = load_notified_firebase()
        if fb_data:
            return fb_data
    except: pass
    # Firebase無資料或失敗，從本地JSON讀取
    if os.path.exists('4_notified_today.json'):
        try:
            with open('4_notified_today.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            print("⚠️ 通知紀錄檔損壞，已重置")
    return {}

def _normalize_df(df):
    """✅ v06100600：yfinance新版MultiIndex攤平，確保欄位名稱正常"""
    if df is None or len(df)==0: return df
    import pandas as _pd
    try:
        if isinstance(df.columns, _pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)
        return df
    except: return df


def _safe_float(val):
    """✅ v06100131：安全轉float，處理yfinance新版回傳Series的情況"""
    import pandas as _pd
    if isinstance(val, _pd.Series):
        return float(val.iloc[0]) if len(val)>0 else 0.0
    if isinstance(val, _pd.DataFrame):
        return float(val.iloc[0,0]) if val.size>0 else 0.0
    try: return float(val)
    except: return 0.0


def _get_period_label(mode_label):
    """✅ v05280800：月K/週K轉換為家人親友可理解的長期/中期（不洩漏技術細節）"""
    ml = str(mode_label or '')
    if '月K' in ml and '週K' in ml: return '長期+中期投資'
    if '月K' in ml: return '長期投資'  # 月K觸發→長期投資
    if '週K' in ml: return '中期投資'  # 週K觸發→中期投資
    return '長期投資'  # 預設長期投資


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
        # ✅ v05171702：條件F升級 - 新增Cond72和Cond0
        import pandas_ta as _pta_el
        _rsi10_el = _pta_el.rsi(c, length=10)
        if _rsi10_el is None or len(_rsi10_el) < 3:
            _rsi10_el = rsi  # fallback to rsi14
        Cond72 = ((ma_c20.shift(1)>ma_c20.shift(2)) & (bb20.shift(1)>bb20.shift(2)) &
                  (bb20<bb20.shift(1)) & (h.shift(1)>bt20.shift(1)) &
                  (_rsi10_el.shift(1)<_rsi10_el.shift(2)) & (_rsi10_el>_rsi10_el.shift(1)) &
                  (l.shift(1)>o.shift(2)) & (h.shift(1)>h.shift(2)) &
                  (mosc>mosc.shift(1)))
        Cond0 = (((l.shift(1)<ma_c20.shift(1)) & (ma_c20.shift(1)>ma_c20.shift(2)) &
                  (emacd>emacd.shift(1)) & (mosc>mosc.shift(1))) |
                 ((l.shift(1)>=ma_c20.shift(1)) & (bt20.shift(1)>bt20.shift(2))))

        group_A = (Cond01 & Cond02 & Cond03 & Cond04)
        group_B = (Cond11|Cond12|Cond21|Cond22|Cond23|Cond24|Cond25|Cond26|Cond27|Cond28|Cond41|Cond42|Cond43|Cond44|Cond45|Cond46|Cond47|Cond48|Cond49|Cond50|Cond61|Cond62|Cond63|Cond66|Cond71|Cond72)

        is_buy_candidate = bool((Cond0 & base & (group_A | group_B)).iloc[-1])

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

# ============================================================
# 【條件E：V轉做多 / A轉做空（通用條件，OR 條件A）】
# 整合 Wed3/Wed4（做多）和 Wed1/Wed2（做空）截圖邏輯
# 適用所有週期（週K/日K/5分K）和所有標的
# ============================================================
def check_condE_long(df):
    """條件E做多 v2（新版：更新Cond01/Cond03，新增Cond72）"""
    try:
        import pandas_ta as _pta
        if df is None or len(df) < 20: return False
        if 'boll_top20' not in df.columns or 'boll_bot20' not in df.columns: return False
        if 'ma_c_20' not in df.columns or 'macd_hist' not in df.columns: return False
        bt=df['boll_top20']; bb=df['boll_bot20']; ma20=df['ma_c_20']
        c=df['Close']; o=df['Open']; h=df['High']; l=df['Low']
        v=df['Volume']; mh=df['macd_hist']
        rsi10=_pta.rsi(c,length=10)
        ema_r=_pta.ema(rsi10,length=5) if rsi10 is not None else None
        if rsi10 is None or ema_r is None or len(rsi10)<10: return False
        # C1: 近5根最低低點 < 近5根下軌均值（去除bollbandtop條件）
        C1 = float(l.iloc[-6:-1].min()) < float(bb.iloc[-6:-1].mean())
        # C2: 近5根最大量 > 近5根平均量
        C2 = float(v.iloc[-6:-1].max()) > float(v.iloc[-6:-1].mean())
        # C3（新）: RSI10當根>前根 AND EMA_RSI10當根>前根
        C3 = (float(rsi10.iloc[-1])>float(rsi10.iloc[-2])) and (float(ema_r.iloc[-1])>float(ema_r.iloc[-2]))
        # C4: 近5根最低開盤 < 當根下軌
        C4 = float(o.iloc[-5:].min()) < float(bb.iloc[-1])
        # C5: MACD柱 < -1 AND 當根 > 前根
        C5 = (float(mh.iloc[-1])<-1) and (float(mh.iloc[-1])>float(mh.iloc[-2]))
        # C6: RSI10當根>前根 AND EMA5<51 AND RSI10<55
        C6 = (float(rsi10.iloc[-1])>float(rsi10.iloc[-2])) and (float(ema_r.iloc[-1])<51) and (float(rsi10.iloc[-1])<55)
        # C11/C12（保留原版）
        C11 = ((float(ma20.iloc[-2])<float(ma20.iloc[-3])) and (float(l.iloc[-2])<float(ma20.iloc[-2])) and
               ((float(o.iloc[-3])+float(o.iloc[-2]))/2>(float(o.iloc[-4])+float(o.iloc[-3]))/2) and
               ((float(c.iloc[-3])+float(c.iloc[-2]))/2>(float(l.iloc[-4])+float(l.iloc[-3]))/2) and
               (float(h.iloc[-1])>float(o.iloc[-1])))
        C12 = ((float(ma20.iloc[-2])>float(ma20.iloc[-3])) and (float(ma20.iloc[-1])>float(ma20.iloc[-2])) and
               (float(l.iloc[-2])<float(ma20.iloc[-2])) and
               ((float(o.iloc[-3])+float(o.iloc[-2]))/2>(float(o.iloc[-4])+float(o.iloc[-3]))/2) and
               ((float(c.iloc[-3])+float(c.iloc[-2]))/2>(float(l.iloc[-4])+float(l.iloc[-3]))/2) and
               ((float(c.iloc[-2])+float(c.iloc[-1]))/2>(float(c.iloc[-3])+float(c.iloc[-2]))/2))
        # C72（新增）
        C72 = ((float(ma20.iloc[-2])>float(ma20.iloc[-3])) and (float(bb.iloc[-2])>float(bb.iloc[-3])) and
               (float(bb.iloc[-1])<float(bb.iloc[-2])) and (float(h.iloc[-2])>float(bt.iloc[-2])) and
               (float(rsi10.iloc[-2])<float(rsi10.iloc[-3])) and (float(rsi10.iloc[-1])>float(rsi10.iloc[-2])) and
               (float(l.iloc[-2])>float(o.iloc[-3])) and (float(h.iloc[-2])>float(h.iloc[-3])) and
               (float(mh.iloc[-1])>float(mh.iloc[-2])))
        # 新版Result：(C1&C2&C3&C4&C5&C6) | ((C3)&(C11|C12|C72))
        return (C1 and C2 and C3 and C4 and C5 and C6) or (C3 and (C11 or C12 or C72))
    except Exception as _e:
        return False


def check_condE_short(df):
    """條件E做空 v2（新版：更新Cond01/Cond03，新增Cond72）"""
    try:
        import pandas_ta as _pta
        if df is None or len(df) < 20: return False
        if 'boll_top20' not in df.columns or 'boll_bot20' not in df.columns: return False
        if 'ma_c_20' not in df.columns or 'macd_hist' not in df.columns: return False
        bt=df['boll_top20']; bb=df['boll_bot20']; ma20=df['ma_c_20']
        c=df['Close']; o=df['Open']; h=df['High']; l=df['Low']
        v=df['Volume']; mh=df['macd_hist']
        rsi10=_pta.rsi(c,length=10)
        ema_r=_pta.ema(rsi10,length=5) if rsi10 is not None else None
        if rsi10 is None or ema_r is None or len(rsi10)<10: return False
        # C1: 近5根最高高點 > 近5根上軌均值（簡化）
        C1 = float(h.iloc[-6:-1].max()) > float(bt.iloc[-6:-1].mean())
        # C2: 近5根最大量 > 近5根平均量
        C2 = float(v.iloc[-6:-1].max()) > float(v.iloc[-6:-1].mean())
        # C3（新）: RSI10當根<前根 AND EMA_RSI10當根<前根
        C3 = (float(rsi10.iloc[-1])<float(rsi10.iloc[-2])) and (float(ema_r.iloc[-1])<float(ema_r.iloc[-2]))
        # C4: 近5根最高開盤 > 當根上軌
        C4 = float(o.iloc[-5:].max()) > float(bt.iloc[-1])
        # C5: MACD柱 > 1 AND 當根 < 前根
        C5 = (float(mh.iloc[-1])>1) and (float(mh.iloc[-1])<float(mh.iloc[-2]))
        # C6: RSI10當根<前根 AND EMA5>49 AND RSI10>45
        C6 = (float(rsi10.iloc[-1])<float(rsi10.iloc[-2])) and (float(ema_r.iloc[-1])>49) and (float(rsi10.iloc[-1])>45)
        # C11/C12（保留原版鏡像）
        C11 = ((float(ma20.iloc[-2])>float(ma20.iloc[-3])) and (float(h.iloc[-2])>float(ma20.iloc[-2])) and
               ((float(o.iloc[-3])+float(o.iloc[-2]))/2<(float(o.iloc[-4])+float(o.iloc[-3]))/2) and
               ((float(c.iloc[-3])+float(c.iloc[-2]))/2<(float(h.iloc[-4])+float(h.iloc[-3]))/2) and
               (float(l.iloc[-1])<float(o.iloc[-1])))
        C12 = ((float(ma20.iloc[-2])<float(ma20.iloc[-3])) and (float(ma20.iloc[-1])<float(ma20.iloc[-2])) and
               (float(h.iloc[-2])>float(ma20.iloc[-2])) and
               ((float(o.iloc[-3])+float(o.iloc[-2]))/2<(float(o.iloc[-4])+float(o.iloc[-3]))/2) and
               ((float(c.iloc[-3])+float(c.iloc[-2]))/2<(float(h.iloc[-4])+float(h.iloc[-3]))/2) and
               ((float(c.iloc[-2])+float(c.iloc[-1]))/2<(float(c.iloc[-3])+float(c.iloc[-2]))/2))
        # C72（新增，做空版本）
        C72 = ((float(ma20.iloc[-2])<float(ma20.iloc[-3])) and (float(bt.iloc[-2])<float(bt.iloc[-3])) and
               (float(bt.iloc[-1])>float(bt.iloc[-2])) and (float(l.iloc[-2])<float(bb.iloc[-2])) and
               (float(rsi10.iloc[-2])>float(rsi10.iloc[-3])) and (float(rsi10.iloc[-1])<float(rsi10.iloc[-2])) and
               (float(h.iloc[-2])<float(o.iloc[-3])) and (float(l.iloc[-2])<float(l.iloc[-3])) and
               (float(mh.iloc[-1])<float(mh.iloc[-2])))
        return (C1 and C2 and C3 and C4 and C5 and C6) or (C3 and (C11 or C12 or C72))
    except Exception as _e:
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
        # ✅ v05171702：條件F升級 - 新增SC72和SC0（做空版本）
        import pandas_ta as _pta_els
        _rsi10_els = _pta_els.rsi(c, length=10)
        if _rsi10_els is None or len(_rsi10_els) < 3:
            _rsi10_els = rsi
        SC72 = ((ma_c20.shift(1)<ma_c20.shift(2)) & (bt20.shift(1)<bt20.shift(2)) &
                (bt20>bt20.shift(1)) & (l.shift(1)<bb20.shift(1)) &
                (_rsi10_els.shift(1)>_rsi10_els.shift(2)) & (_rsi10_els<_rsi10_els.shift(1)) &
                (h.shift(1)<o.shift(2)) & (l.shift(1)<l.shift(2)) &
                (mosc<mosc.shift(1)))
        SC0 = (((h.shift(1)>ma_c20.shift(1)) & (ma_c20.shift(1)<ma_c20.shift(2)) &
                (emacd<emacd.shift(1)) & (mosc<mosc.shift(1))) |
               ((h.shift(1)<=ma_c20.shift(1)) & (bb20.shift(1)<bb20.shift(2))))

        short_group_A = (SC01 & SC02 & SC03 & SC04)
        short_group_B = (SC11|SC12|SC21|SC22|SC23|SC24|SC25|SC26|SC27|SC28|SC41|SC42|SC43|SC44|SC45|SC46|SC47|SC48|SC49|SC50|SC61|SC62|SC63|SC66|SC71|SC72)

        is_short = bool((SC0 & short_base & (short_group_A | short_group_B)).iloc[-1])
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
    result = {'bull_abc':False,'bull_d':False,'bear_abc':False,'bear_d':False,'warn':False,'rsi_mo':0,'rsi_wk':0,'rsi_w':0,'rsi_d':0,'macd_mo':'?','macd_wk':'?','macd_d':'?'}
    try:
        # ✅ v05192313：月K+週K+日K三週期全判斷
        df_w = _normalize_df(yf.download(ticker, period='5y', interval='1mo', progress=False))  # 月K
        df_wk = _normalize_df(yf.download(ticker, period='2y', interval='1wk', progress=False))  # 週K
        df_wk = calc_indicators(df_wk) if df_wk is not None and len(df_wk)>=20 else None
        if df_w is None or len(df_w) < 30: return result
        # ✅ v06091344：即時5m補充月K/週K最後一根收盤（不改interval）
        _rt = None
        try:
            _rt = _normalize_df(yf.download(ticker, period='1d', interval='5m', progress=False))
        except: pass
        _cur_price = float(_rt['Close'].iloc[-1]) if _rt is not None and len(_rt)>0 else None
        if _cur_price:
            for _df_ref in [df_w, df_wk]:
                if _df_ref is not None and len(_df_ref)>0:
                    try: _df_ref.iloc[-1, _df_ref.columns.get_loc('Close')] = _cur_price
                    except: pass
        df_w  = calc_indicators(df_w)
        ok_w, condD_bull_w  = check_buy_precondition(df_w, is_weekly=True)
        ok_ws,condD_bear_w  = check_short_precondition(df_w, is_weekly=True)
        # 週K額外判斷
        _ok_wk,_cdD_wk  = check_buy_precondition(df_wk) if df_wk is not None else (False,False)
        _ok_wks,_cdDs_wk= check_short_precondition(df_wk) if df_wk is not None else (False,False)
        last_w = df_w.iloc[-1]; prev_w = df_w.iloc[-2]
        result['rsi_w']  = _safe_float(last_w['rsi14'])  # df_w=月K（舊相容key）
        result['rsi_mo'] = _safe_float(last_w['rsi14'])  # 月K RSI（正確標示）
        # 月K MACD方向
        _macd_mo_up = _safe_float(last_w.get('macd_hist',0)) > _safe_float(prev_w.get('macd_hist',0))
        result['macd_mo'] = '↑' if _macd_mo_up else '↓'
        # 週K RSI和MACD（df_wk）
        if df_wk is not None and len(df_wk)>=2:
            _lw = df_wk.iloc[-1]; _pw = df_wk.iloc[-2]
            result['rsi_wk'] = float(_lw.get('rsi14', result['rsi_d']))
            result['macd_wk'] = '↑' if float(_lw.get('macd_hist',0))>float(_pw.get('macd_hist',0)) else '↓'

        # 日K
        # ✅ v06091336：日K改用1h即時數據（盤中反映當日走勢），不再只看昨日收盤
        df_d = _normalize_df(yf.download(ticker, period='60d', interval='1h', progress=False))
        if df_d is not None and len(df_d) >= 30:
            df_d = calc_indicators(df_d)
            ok_d, condD_bull_d = check_buy_precondition(df_d, is_weekly=False)
            ok_ds, condD_bear_d = check_short_precondition(df_d, is_weekly=False)
            result['rsi_d'] = _safe_float(df_d.iloc[-1]['rsi14'])
            if len(df_d) >= 2:
                _ld = df_d.iloc[-1]; _pd = df_d.iloc[-2]
                result['macd_d'] = '↑' if float(_ld.get('macd_hist',0)) > float(_pd.get('macd_hist',0)) else '↓'
        else:
            ok_d = ok_ds = condD_bull_d = condD_bear_d = False

        # ✅ v05192313：守門員加入條件E，條件D擴展至月K/日K
        _e_bull_w = check_condE_long(df_w) if df_w is not None else False
        _e_bull_d = check_condE_long(df_d) if df_d is not None else False
        _e_bear_w = check_condE_short(df_w) if df_w is not None else False
        _e_bear_d = check_condE_short(df_d) if df_d is not None else False
        # 多頭A/B/C/E：月K或日K通過（非條件D）
        result['bull_abc'] = ((ok_w and not condD_bull_w) or _e_bull_w or
                              (ok_d and not condD_bull_d) or _e_bull_d)
        # 多頭條件D：月K或日K條件D通過
        result['bull_d']   = condD_bull_w or condD_bull_d
        # 空頭A/B/C/E：月K或日K空頭通過（非條件D）
        result['bear_abc'] = ((ok_ws and not condD_bear_w) or _e_bear_w or
                              (ok_ds and not condD_bear_d) or _e_bear_d)
        # 空頭條件D：月K或日K空頭條件D
        result['bear_d']   = condD_bear_w or condD_bear_d
        # 下彎警告：RSI↓ AND MACD柱↓（兩個都下彎才警告）
        rsi_down  = _safe_float(last_w['rsi14']) < float(prev_w['rsi14'])
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

# ============================================================
# 【週選擇權履約價推薦】
# 僅在週三/週五 09:05~10:45 期貨5分K觸發時附加在通知信中
# ============================================================
def get_weekly_option_hint(current_price, signal_type):
    """
    根據當前台指期貨報價和訊號方向，推薦週選擇權履約價範圍
    signal_type: 'buy'（做多→建議CALL）或 'sell'（做空→建議PUT）
    """
    try:
        from datetime import datetime
        import pytz
        _tz = pytz.timezone('Asia/Taipei')
        _now = datetime.now(_tz)
        _wd  = _now.weekday()  # 2=週三, 4=週五
        _date_str = _now.strftime('%m/%d')

        # 到期日說明
        if _wd == 2:
            _expiry = f"本週三（{_date_str}）到期週選擇權"
        elif _wd == 4:
            _expiry = f"本週五（{_date_str}）到期週選擇權"
        else:
            _expiry = f"當日（{_date_str}）週選擇權"

        # ATM 履約價（四捨五入至最近50點）
        _atm = round(current_price / 50) * 50

        if signal_type == 'sell':
            # 做空 → 建議 PUT：ATM和略低於ATM（讓put稍有時間價值但不太貴）
            _strikes = [_atm, _atm - 50, _atm - 100]
            _opt_type = 'PUT'
            _direction = '做空（buy PUT）'
        else:
            # 做多 → 建議 CALL：ATM和略高於ATM
            _strikes = [_atm, _atm + 50, _atm + 100]
            _opt_type = 'CALL'
            _direction = '做多（buy CALL）'

        _hint = (
            f"\n\n{'='*40}\n"
            f"📋 週選擇權建議（{_direction}）\n"
            f"{'='*40}\n"
            f"到期日：{_expiry}\n"
            f"台指現價：{current_price:.0f}\n"
            f"建議標的：{_opt_type}\n"
            f"建議履約價範圍（擇一，依市價決定）：\n"
        )
        for _s in _strikes:
            _hint += f"  → {int(_s)} {_opt_type}\n"
        _hint += (
            f"\n⚠️ 進場條件（請在永豐金確認）：\n"
            f"  ✅ 市價 ≤ 20 元\n"
            f"  ✅ 當日最低價 ≥ 12 元（避免時間價值耗盡）\n"
            f"\n策略：建倉後不停損，持有至期貨5分K碰上/下軌反轉或結算"
        )
        return _hint
    except Exception as _e:
        return ""

def scan_stock(ticker, is_holding=False, _mode_label=None):
    global _tw_prescreened, _wk_passed_1st, _prescreened_ind
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
        df_w = get_stock_data(ticker, period='5y', interval='1mo', cache=weekly_cache)  # ✅ v05170856 長期投資改月K
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
                _df1st = get_stock_data(ticker, period='2y', interval='1wk', cache=daily_cache)  # ✅ v05170856 中期投資改週K
                if _df1st is None or len(_df1st) < 50: return None
                _df1st = calc_indicators(_df1st)
                if _df1st is None: return None
                # BUY_LOOKBACK_BARS=3，不換算，直接跑
                _is_long_ok, _condD_long   = check_buy_precondition(_df1st)
                _is_short_ok, _condD_short = check_short_precondition(_df1st)
                # ✅ 05100733：條件E OR 條件A（日K模式）
                if not _is_long_ok  and check_condE_long(_df1st):
                    _is_long_ok, _condD_long  = True, False
                if not _is_short_ok and check_condE_short(_df1st):
                    _is_short_ok, _condD_short = True, False
            else:
                # ✅ v06100610：月K模式第一道：用月K(df_w,1mo)，非週K！
                _is_long_ok, _condD_long   = check_buy_precondition(df_w, is_weekly=True)
                _is_short_ok, _condD_short = check_short_precondition(df_w, is_weekly=True)
                # ✅ 05100733：條件E OR 條件A（週K模式）
                if not _is_long_ok  and check_condE_long(df_w):
                    _is_long_ok, _condD_long  = True, False
                if not _is_short_ok and check_condE_short(df_w):
                    _is_short_ok, _condD_short = True, False
            # ✅ 診斷輸出：第一道結果（_mode_label由scan_stock_mixed傳入）
            _wk_label = _mode_label if _mode_label else ('月K' if SCAN_MODE != 'daily' else '週K')  # ✅ v05200928
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
            # ✅ 週K模式第一道通過 → 記錄旗標供 scan_stock_mixed 判斷標籤
            if SCAN_MODE == 'weekly':
                _wk_passed_1st = True
        else:
            print(f"🧪 {ticker} 正在進行【驗證篩選】測試中...")
            _condD_long = False; _condD_short = False  # TEST_MODE 預設

        # ── 第二道：週K/日K統一用日K eLeader 25條件（AND邏輯）─────
        # 兩種模式統一：eLeader必過才繼續（確保不在高檔區追高）
        # ✅ v05191855：週K快取保留供其他用途
        df_d = get_stock_data(ticker, period='2y', interval='1wk', cache=daily_cache)  # 週K（保留供參考）
        if df_d is None or len(df_d) < 20: return None
        df_d = calc_indicators(df_d)
        if df_d is None: return None
        # ✅ v05191855：第二道改用日K（A/B/C/E OR F，符合先大後小原則）
        _df_1d = get_stock_data(ticker, period='1y', interval='1d', cache=daily_cache)
        _df_1d = calc_indicators(_df_1d) if _df_1d is not None and len(_df_1d) >= 20 else None

        if _is_long_ok:
            # ✅ v05191855：第二道 = 日K A/B/C/E（事不過三）OR 日K F(eLeader)
            _2nd_abc = (signal_within_n(lambda d: check_buy_precondition(d)[0], _df_1d, n=3) or
                        (_df_1d is not None and check_condE_long(_df_1d))) if _df_1d is not None else False
            _2nd_el  = signal_within_n(lambda d: check_buy_eleader(d) is not None, _df_1d, n=3) if _df_1d is not None else False
            is_eleader_ok = _2nd_abc or _2nd_el
            if _condD_long:
                # ✅ v05192327：高位階第二道 = 日K 條件D OR F（可選，不強制）
                _2nd_d_ok = check_buy_precondition(_df_1d)[1] if _df_1d is not None else False  # condD日K
                is_eleader_ok = is_eleader_ok or _2nd_d_ok  # D OR F 任一通過
                print(f'  {"✅" if is_eleader_ok else "⚠️"} {ticker} 第二道高位階(日K D or F) {"通過" if is_eleader_ok else "未通過（條件D補位，繼續）"}')
            else:
                # A/B/C 觸發（低位階下軌/中軌）→ eLeader 為必要條件
                print(f'  {"✅" if is_eleader_ok else "❌"} {ticker} 第二道日K多頭(A/B/C/E OR eLeader) {"通過" if is_eleader_ok else "未通過，跳過"}')
                if not is_eleader_ok:
                    return None
        else:
            # ✅ v05191855：做空第二道 = 日K A/B/C/E（事不過三）OR 日K F(做空eLeader)
            _2nd_sabc = (signal_within_n(lambda d: check_short_precondition(d)[0], _df_1d, n=3) or
                         (_df_1d is not None and check_condE_short(_df_1d))) if _df_1d is not None else False
            _2nd_sel  = signal_within_n(lambda d: check_short_eleader(d) is not None, _df_1d, n=3) if _df_1d is not None else False
            is_eleader_short_ok = _2nd_sabc or _2nd_sel
            if _condD_short:
                # 條件D空頭（高位階）→ eLeader 為可選
                print(f'  {"✅" if is_eleader_short_ok else "⚠️"} {ticker} 第二道eLeader空頭 {"通過" if is_eleader_short_ok else "未通過（條件D補位，繼續）"}')
            else:
                # A/B/C 空頭（低位階）→ eLeader 為必要條件
                print(f'  {"✅" if is_eleader_short_ok else "❌"} {ticker} 第二道日K空頭(A/B/C/E OR eLeader) {"通過" if is_eleader_short_ok else "未通過，跳過"}')
                if not is_eleader_short_ok:
                    return None

        # ✅ 05041037新增：第一+第二道通過後，儲存指標供Firebase快取
        _code_only2 = ticker.split('.')[0]
        try:
            _ref_df = df_w if (SCAN_MODE == 'weekly') else df_d
            _rsi_now2 = float(_ref_df['rsi14'].iloc[-1])
            _rsi_prv2 = float(_ref_df['rsi14'].iloc[-2])
            _close2   = float(_ref_df['Close'].iloc[-1])
            _boll2    = float(_ref_df['boll_bot20'].iloc[-1]) if 'boll_bot20' in _ref_df.columns else 0
            _boll_t2  = float(_ref_df['boll_top20'].iloc[-1]) if 'boll_top20' in _ref_df.columns else 0
            if _boll_t2 > 0 and _boll2 > 0:
                if _close2 >= _boll_t2 * 0.97:   _bp2 = 'near_upper'
                elif _close2 <= _boll2 * 1.03:   _bp2 = 'near_lower'
                else:                             _bp2 = 'mid_zone'
            else:
                _bp2 = 'mid_zone'
            # ✅ v05170954：加入產業類別（group）上傳雲端
            try:
                import twstock as _tws
                _grp = _tws.codes.get(_code_only2)
                _industry = _grp.group if _grp and _grp.group else ''
                _stk_name = _grp.name if _grp else ''
            except Exception:
                _industry = ''; _stk_name = ''
            _prescreened_ind[_code_only2] = {
                'rsi': round(_rsi_now2, 1), 'rsi_prev': round(_rsi_prv2, 1),
                'boll_pos': _bp2, 'is_long': bool(_is_long_ok), 'is_short': bool(_is_short_ok),
                'condD_l': bool(_condD_long), 'condD_s': bool(_condD_short),
                'industry': _industry,  # 產業類別（例：半導體業、金融保險業）
                'name': _stk_name,      # 股票中文名稱
            }
        except Exception: pass

        # ── 第三道：5分K即時轉折（週K/日K模式共用）─────────────
        now_ts = time.time()
        cache_key = f"5m_{ticker}"
        if cache_key in five_min_cache and (now_ts - five_min_cache[cache_key]['ts'] < 300):
            df_5m = five_min_cache[cache_key]['df']
        else:
            df_5m = _normalize_df(yf.download(ticker, period='5d', interval='5m', progress=False))
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
def check_tw_daytime_extreme(tse_mkt_result):
    """✅ v06130522：台股白天大盤極端異動警報
    台股收盤後顯示，若當日^TWII漲跌超過1000點 → Gmail通知
    """
    try:
        import yfinance as _yf
        from datetime import datetime as _dt
        import pytz as _pytz
        _tz = _pytz.timezone('Asia/Taipei')
        _now = _dt.now(_tz)
        _today_str = _now.strftime('%Y-%m-%d')

        # 取^TWII今日漲跌
        _twii = _normalize_df(_yf.download('^TWII', period='2d', interval='1d', progress=False))
        if _twii is None or len(_twii) < 2: return
        _twii = calc_indicators(_twii)
        _cur  = _safe_float(_twii['Close'].iloc[-1])
        _prev = _safe_float(_twii['Close'].iloc[-2])
        if _prev <= 0: return
        _chg_pts = _cur - _prev
        _chg_pct = _chg_pts / _prev * 100

        print(f"  📊 台股大盤今日：{_cur:.0f}（前收{_prev:.0f}，漲跌{_chg_pts:+.0f}點，{_chg_pct:+.2f}%）")
        if abs(_chg_pts) < 1000: return  # 未達閾值

        _direction = 'DOWN' if _chg_pts < 0 else 'UP'
        _alert_key = f"TWII_DAYTIME_{_direction}_{_today_str}"
        _today_notified = notified.get(_today_str, [])
        if _alert_key in _today_notified:
            print(f"  🔕 台股白天極端異動今日已通知（{_direction}），跳過")
            return

        _emoji = "🔻" if _direction == 'DOWN' else "🚀"
        _action = "暴跌！考慮台股期貨做空或buy put" if _direction == 'DOWN' else "急漲！考慮台股期貨做多或buy call"
        _arr = '↘' if _direction == 'DOWN' else '↗'
        _subject = f'💻【本機】{_emoji}台股白天極端異動！{_arr}{int(abs(_chg_pts))}點({_chg_pct:+.1f}%)'
        _lines = [
            f'⚠️ 台股白天大盤極端異動（台灣時間）',
            '='*35,
            f'今日^TWII：{_cur:.0f}（前收{_prev:.0f}）',
            f'漲跌幅：{_arr}{int(abs(_chg_pts)):,}點（{_chg_pct:+.2f}%）',
            '='*35,
            f'💡 建議：{_action}',
            '⚠️ 嚴禁用於當沖或隔日沖',
            f'掃描時間：{_now.strftime("%Y/%m/%d %H:%M")}'
        ]
        send_gmail(_subject, '\n'.join(_lines))
        if _today_str not in notified: notified[_today_str] = []
        notified[_today_str].append(_alert_key)
        save_notified(notified)
        print(f"  ✅ 台股白天極端異動Gmail已發送！{_chg_pts:+.0f}點")
    except Exception as _e:
        print(f"  ⚠️ check_tw_daytime_extreme異常：{str(_e)[:60]}")


def check_overnight_extreme_move():
    """✅ v06061213：台指夜盤極端異動警報
    監控EWT（台灣ETF，NYSE 21:30~04:00台灣時間）作為台指近全代理
    EWT跌幅 > 2.2%（≈台指1000點）→ 立即發Gmail警報
    不受notified去重限制（每天每個方向只發一次）
    """
    try:
        import yfinance as _yf
        from datetime import datetime as _dt, timedelta as _tdelta
        import pytz as _pytz

        _tz = _pytz.timezone('Asia/Taipei')
        _now = _dt.now(_tz)
        _today_str = _now.strftime('%Y-%m-%d')

        # 使用EWT作為台指近全代理（MSCI Taiwan ETF，NYSE交易）
        _ewt = _yf.download('EWT', period='3d', interval='30m', progress=False)
        if _ewt is None or len(_ewt) < 10:
            return

        # 取最新收盤和前一美股交易日收盤
        _cur_price = _safe_float(_ewt['Close'].iloc[-1])
        # 找前一交易日最後收盤（EWT US收盤 = 台灣時間04:00）
        _prev_close = _safe_float(_ewt['Close'].iloc[-20]) if len(_ewt) >= 20 else _safe_float(_ewt['Close'].iloc[0])

        _chg_pct = (_cur_price - _prev_close) / _prev_close * 100
        _twii_base = 45000  # 台指基準點（可隨市況調整）
        _est_points = abs(_chg_pct / 100 * _twii_base)

        print(f"  📊 EWT即時監控：{_cur_price:.2f}（前收{_prev_close:.2f}，變化{_chg_pct:+.2f}%，估台指{_est_points:+.0f}點）")

        # 閾值：1000點（≈ 2.22%）
        _threshold_pts = 1000
        _threshold_pct = _threshold_pts / _twii_base * 100  # ≈ 2.22%

        if abs(_chg_pct) < _threshold_pct:
            return  # 未達閾值，不通知

        # 確認今日此方向是否已通知過
        _direction = 'DOWN' if _chg_pct < 0 else 'UP'
        _alert_key = f"TWII_EXTREME_{_direction}_{_today_str}"

        # ✅ v06130522：發送前強制重載Firebase，防止重複通知
        try:
            _fb_reload = load_notified_firebase()
            if _fb_reload: notified.update(_fb_reload)
        except: pass
        _today_notified = notified.get(_today_str, [])
        if _alert_key in _today_notified:
            print(f"  🔕 台指夜盤極端異動今日已通知過（{_direction}），跳過")
            return

        # 發送警報
        _emoji = "🔻" if _direction == 'DOWN' else "🚀"
        _action = "暴跌！考慮買進Put選擇權" if _direction == 'DOWN' else "急漲！考慮買進Call選擇權"
        _arr = '↘' if _direction=='DOWN' else '↗'
        _subject = f'💻【本機】{_emoji}台指夜盤極端異動！估計{_arr}{int(_est_points)}點({_chg_pct:+.1f}%)'
        _lines = [
            '⚠️ 台指近全夜盤極端異動警報 ⚠️', '='*35,
            'EWT代理（iShares MSCI Taiwan ETF）',
            f'EWT目前：{_cur_price:.2f}  前收：{_prev_close:.2f}',
            f'變化幅度：{_chg_pct:+.2f}%  估計台指：{_arr}{int(_est_points):,}點',
            '='*35,
            f'💡 建議：{_action}',
            '⚠️ 嚴禁用於當沖或隔日沖',
            f'掃描時間：{_now.strftime("%Y/%m/%d %H:%M")}'
        ]
        _body = '\n'.join(_lines)
        send_gmail(_subject, _body)

        # 記錄已通知
        if _today_str not in notified:
            notified[_today_str] = []
        notified[_today_str].append(_alert_key)
        save_notified(notified)
        print(f"  ✅ 台指夜盤極端異動警報已發送！EWT {_chg_pct:+.2f}%，估台指{int(_est_points):,}點")

    except Exception as _e:
        print(f"  ⚠️ check_overnight_extreme_move異常（非下市）：{str(_e)[:60]}")


def scan_limit_up():
    """✅ v06081833：漲停追蹤課
    每日收盤後執行一次，找出當天漲停(+10%)的台股
    對每支漲停股票檢查月K/週K是否符合買進條件（A/B/C/D/E/F）
    若通過 → 再看日K → 通過則Gmail通知
    """
    try:
        print("\n📈 漲停追蹤課：掃描今日漲停股票...")
        import yfinance as _yf
        from datetime import datetime as _dt
        import pytz as _pytz

        _tz = _pytz.timezone('Asia/Taipei')
        _now = _dt.now(_tz)
        _today_str = _now.strftime('%Y-%m-%d')

        # 取預篩清單（避免掃全部1827支）
        _tw_codes = _tw_prescreened if _tw_prescreened else []
        if not _tw_codes:
            print("  ℹ️ 預篩清單為空，跳過漲停追蹤")
            return

        _limit_up = []
        print(f"  掃描 {len(_tw_codes)} 支預篩台股是否漲停...")

        for _ticker in _tw_codes[:50]:  # 限制最多50支，避免API過載
            try:
                _info = _yf.Ticker(_ticker).fast_info
                _prev = getattr(_info, 'previous_close', None)
                _last = getattr(_info, 'last_price', None)
                if _prev and _last and _prev > 0:
                    _chg = (_last - _prev) / _prev
                    if _chg >= 0.0995:  # 漲停 ≈ +10%
                        _limit_up.append((_ticker, _chg * 100, _last))
                        print(f"  🔥 漲停：{_ticker} +{_chg*100:.1f}%")
            except:
                pass

        if not _limit_up:
            print("  ℹ️ 今日無漲停股票（預篩清單內）")
            return

        print(f"  共 {len(_limit_up)} 支漲停，開始月K/週K策略檢查...")
        _buy_candidates = []

        for _ticker, _chg_pct, _price in _limit_up:
            try:
                # 月K第一道
                _df_mo = calc_indicators(
                    _yf.download(_ticker, period='5y', interval='1mo', progress=False))
                _df_wk = calc_indicators(
                    _yf.download(_ticker, period='2y', interval='1wk', progress=False))
                _df_dk = calc_indicators(
                    _yf.download(_ticker, period='1y', interval='1d', progress=False))

                _ok_mo = (_df_mo is not None and
                    (signal_within_n(lambda d: check_buy_precondition(d)[0], _df_mo, n=3) or
                     check_condE_long(_df_mo) or check_buy_eleader(_df_mo) is not None))
                _ok_wk = (_df_wk is not None and
                    (signal_within_n(lambda d: check_buy_precondition(d)[0], _df_wk, n=3) or
                     check_condE_long(_df_wk) or check_buy_eleader(_df_wk) is not None))

                if not (_ok_mo or _ok_wk):
                    continue  # 月K/週K都沒有買進訊號，跳過

                # 日K第二道確認
                _ok_dk = (_df_dk is not None and
                    (signal_within_n(lambda d: check_buy_precondition(d)[0], _df_dk, n=3) or
                     check_condE_long(_df_dk)))

                _period = '月K+週K' if (_ok_mo and _ok_wk) else ('月K' if _ok_mo else '週K')
                _buy_candidates.append((_ticker, _chg_pct, _price, _period, _ok_dk))
                print(f"  ✅ {_ticker} 漲停+{_chg_pct:.1f}% | {_period}通過 | 日K={'✅' if _ok_dk else '❌'}")

            except Exception as _e:
                print(f"  ⚠️ {_ticker} 漲停分析異常：{str(_e)[:50]}")

        if not _buy_candidates:
            print("  ℹ️ 漲停股票均未符合月K/週K買進條件")
            return

        # Gmail通知
        _notif_key = f"LIMIT_UP_{_today_str}"
        _today_notified = notified.get(_today_str, [])
        if _notif_key in _today_notified:
            print("  🔕 今日漲停追蹤已通知過，跳過")
            return

        _lines = [f"📈 今日漲停股票買進候選（{_now.strftime('%Y/%m/%d')}）", '='*35]
        for _t, _c, _p, _per, _dk in _buy_candidates:
            _dk_str = '日K✅' if _dk else '日K尚未符合'
            _lines.append(f"  {_t}  漲停+{_c:.1f}%  收{_p:.2f}  {_per}買進訊號  {_dk_str}")
        _lines += ['='*35,
                   '⚠️ 漲停股需確認隔日開盤是否繼續，請謹慎進場',
                   '⚠️ 嚴禁用於當沖或隔日沖']

        _subject = f"💻【本機】📈漲停追蹤：{len(_buy_candidates)}支符合月K/週K買進條件"
        send_gmail(_subject, '\n'.join(_lines))

        if _today_str not in notified:
            notified[_today_str] = []
        notified[_today_str].append(_notif_key)
        save_notified(notified)
        print(f"  ✅ 漲停追蹤Gmail已發送！{len(_buy_candidates)}支候選")

    except Exception as _e:
        print(f"  ⚠️ scan_limit_up異常：{str(_e)[:60]}")


def scan_synthetic_fund(fund_name="安聯月配息基金(合成代標)"):
    global buy_signals, sell_signals
    try:
        print(f"\n🚀 正在啟動合成追蹤：{fund_name}...")
        # 1. 週K（第一道：位階門檻）
        s_w = _normalize_df(yf.download("SPY", period='2y', interval='1wk', progress=False))
        q_w = _normalize_df(yf.download("QQQ", period='2y', interval='1wk', progress=False))
        h_w = _normalize_df(yf.download("HYG", period='2y', interval='1wk', progress=False))
        df_w = calc_indicators(build_fund_proxy_df(s_w, q_w, h_w))
        if df_w is None or not check_buy_precondition(df_w, is_weekly=True):
            print(f"ℹ️ {fund_name}:週K位階尚未符合觸發買進條件")
            return

        # 2. 日K（第二道：eLeader 25條件）
        s_d = _normalize_df(yf.download("SPY", period='1y', interval='1d', progress=False))
        q_d = _normalize_df(yf.download("QQQ", period='1y', interval='1d', progress=False))
        h_d = _normalize_df(yf.download("HYG", period='1y', interval='1d', progress=False))
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
                send_gmail(f"💻【本機】🔔【{_get_period_label("月K")}】基金買進訊號：{fund_name}", msg_body)
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
    notified[today] = []  # ✅ 05101039修正：只新增今天的key，不覆蓋整個字典

# ✅ v05231322：跨午夜保護（00:00-06:00視為前一個交易日，避免重複通知）
_cur_hour = datetime.now().hour
if _cur_hour < 6:
    from datetime import timedelta as _tdelta
    _prev_day = (datetime.now()-_tdelta(days=1)).strftime("%Y-%m-%d")
    if _prev_day in notified:
        notified[today] = list(set(notified.get(today,[]) + notified[_prev_day]))
# ============================================================
# 【１３．主程式。邏輯：執行單次掃描】
# ============================================================

# ============================================================
# 【Firebase 預篩清單讀取】
# 從 Firebase 讀取雲端版每天14:00更新的台股預篩清單
# 本機版優先使用此快取，避免每次都掃1800支
# ============================================================
def write_buy_signal_firebase(ticker, price, condition, now_str, market='TW'):
    """✅ 05111049：寫入買進訊號到Firebase，供網頁版T+2追蹤使用"""
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
        _tz = pytz.timezone('Asia/Taipei')
        _today = datetime.now(_tz).strftime('%Y%m%d')
        # 計算漲停價（price × 1.1 → 無條件捨去至合法Tick）
        def _tick(p): return 0.01 if p<=10 else 0.05 if p<=50 else 0.1 if p<=100 else 0.5 if p<=500 else 1 if p<=1000 else 5
        _t = _tick(price)
        _limit = round(int(price * 1.1 / _t) * _t, 10)
        _url = (f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
                f"/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}"
                f"/public/signals_{_today}")
        # 讀取現有訊號
        _rg = _req.get(_url, headers={"Authorization": f"Bearer {_c.token}"}, timeout=10)
        _existing = {}
        if _rg.status_code == 200:
            try: _existing = json.loads(_rg.json().get('fields',{}).get('signals',{}).get('stringValue','{}'))
            except: _existing = {}
        _existing[ticker] = {
            'ticker': ticker, 'market': market,
            'price': float(price), 'limit_price': float(_limit),
            'condition': condition, 'time': now_str, 'date': _today
        }
        _r = _req.patch(_url, timeout=15,
            headers={"Authorization": f"Bearer {_c.token}", "Content-Type": "application/json"},
            json={"fields": {
                "signals":    {"stringValue": json.dumps(_existing, ensure_ascii=False)},
                "updated_at": {"stringValue": now_str}}})
        return _r.status_code in (200, 201)
    except Exception as _e:
        return False

def _export_scan_results(buy_signals, sell_signals, now_str):
    """✅ v05171047：輸出篩選結果到CSV和JSON，方便複製代碼到eleader/三竹股市
    ✅ v05192313：無訊號時不輸出空檔案"""
    if not buy_signals and not sell_signals:
        print("  ℹ️ 無訊號，不輸出CSV/JSON")
        return
    try:
        import json, csv
        from datetime import datetime as _dt
        _date = _dt.now().strftime('%Y%m%d_%H%M')
        # 整理買進訊號
        _buy = [{'market':s[0],'code':s[1],'close':round(s[2],2),'rsi':round(s[3],1),
                 'boll':s[4],'conditions':str(s[5])} for s in buy_signals]
        _sell = [{'market':s[0],'code':s[1],'close':round(s[2],2),'rsi':round(s[3],1)} for s in sell_signals]
        # 買進代碼清單（純代碼，方便複製）
        _buy_codes = [s[1] for s in buy_signals]
        _sell_codes = [s[1] for s in sell_signals]
        # JSON（完整資訊）
        _result = {'scan_time':now_str,'buy':_buy,'sell':_sell,
                   'buy_codes':_buy_codes,'sell_codes':_sell_codes}
        # ✅ v05231340：停用JSON輸出，只保留CSV（JSON不方便複製代碼）
        # ✅ v05280820：CSV改為當日累計，同標的只出現1次（market+code去重）
        if _buy:
            _date_only = datetime.now().strftime('%Y%m%d')  # 日期不含時間
            _csv_path = f'scan_buy_{_date_only}.csv'
            _fields = ['market','code','close','rsi','boll','conditions']
            # 讀取今日既有資料（若已存在）
            _existing_keys = set()
            _existing_rows = []
            if __import__('os').path.exists(_csv_path):
                try:
                    with open(_csv_path,'r',newline='',encoding='utf-8-sig') as _rf:
                        for _row in csv.DictReader(_rf):
                            _existing_rows.append(_row)
                            _existing_keys.add(f"{_row['market']}_{_row['code']}")
                except: pass
            # 只加入今日未出現的標的
            _new_rows = [r for r in _buy if f"{r['market']}_{r['code']}" not in _existing_keys]
            if _new_rows:
                with open(_csv_path,'w',newline='',encoding='utf-8-sig') as f:
                    _w = csv.DictWriter(f,fieldnames=_fields)
                    _w.writeheader()
                    _w.writerows(_existing_rows + _new_rows)
                print(f'  ✅ CSV新增{len(_new_rows)}筆（跳過{len(_buy)-len(_new_rows)}筆重複）')
            else:
                print(f'  ℹ️ CSV無新增（{len(_buy)}筆今日已記錄）')
        print(f"\n📄 當日CSV：{'scan_buy_'+datetime.now().strftime('%Y%m%d')+'.csv' if _buy else '（無買進訊號）'}")
        print(f"   📋 買進代碼（複製到eleader）：{' '.join(_buy_codes) if _buy_codes else '（無）'}")
    except Exception as _e:
        print(f"  ⚠️ 輸出CSV/JSON失敗：{_e}")


def _print_scan_summary(buy_signals, sell_signals):
    """✅ v05171047：掃描完成後底部統整，不用往上翻"""
    print(f"\n{'═'*55}")
    print(f"  📊 掃描結果統整（不用往上翻）")
    print(f"{'═'*55}")
    if buy_signals:
        print(f"  ⭐ 做多進場信號（{len(buy_signals)}支）：")
        for s in buy_signals:
            print(f"    {s[0]} {s[1]}  收：{s[2]:.2f}  RSI：{s[3]:.1f}  {s[4]}")
    else:
        print(f"  ⭐ 做多進場：無")
    if sell_signals:
        print(f"  🔴 賣出/做空信號（{len(sell_signals)}支）：")
        for s in sell_signals:
            print(f"    {s[0]} {s[1]}  收：{s[2]:.2f}  RSI：{s[3]:.1f}")
    else:
        print(f"  🔴 賣出/做空：無")
    print(f"{'═'*55}\n")


def signal_within_n(check_func, df, n=3, reverse_check=None):
    """✅ v05181836：事不過三 — 過去n根K棒內有訊號且無反轉，全市場通用
    用法：signal_within_n(check_buy_precondition, df_monthly, n=3)
    說明：不動策略條件，只擴大回看窗口，符合eleader切換週期原則
    """
    if df is None or len(df) < 25: return False
    if reverse_check:
        try:
            if reverse_check(df): return False
        except: pass
    for i in range(n):
        df_slice = df if i == 0 else df.iloc[:-i]
        if len(df_slice) < 20: continue
        try:
            result = check_func(df_slice)
            if isinstance(result, tuple): result = bool(result[0])
            if result:
                if i > 0 and reverse_check:
                    has_rev = False
                    for j in range(1, i+1):
                        mid = df.iloc[:-(j-1)] if j > 1 else df
                        try:
                            if reverse_check(mid): has_rev = True; break
                        except: pass
                    if has_rev: continue
                return True
        except: continue
    return False


def check_financial_health_finmind(stock_id):
    """✅ v05170954：FinMind財務篩選 流動比率>1.5"""
    global _finmind_cache
    from datetime import datetime as _dt,timedelta as _td
    now=_dt.now()
    if stock_id in _finmind_cache:
        c=_finmind_cache[stock_id]
        if (now-c['ts']).total_seconds()/3600<168: return c['pass']
    try:
        import requests as _req
        r=_req.get('https://api.finmindtrade.com/api/v4/data',
            params={'dataset':'TaiwanStockBalanceSheet','stock_id':stock_id,
                    'date':(now-_td(days=400)).strftime('%Y-%m-%d'),'token':FINMIND_TOKEN},timeout=15)
        if r.status_code!=200: _finmind_cache[stock_id]={'pass':None,'ts':now}; return None
        d=r.json()
        if d.get('status')!=200 or not d.get('data'): _finmind_cache[stock_id]={'pass':None,'ts':now}; return None
        items=sorted(d['data'],key=lambda x:x.get('date',''),reverse=True)
        cur_a=cur_l=None
        for item in items:
            t=item.get('type',''); v=float(item.get('value',0) or 0)
            if t=='CurrentAssets' and cur_a is None: cur_a=v
            if t=='CurrentLiabilities' and cur_l is None: cur_l=v
            if cur_a is not None and cur_l is not None: break
        if cur_a is None or cur_l is None or cur_a<=0: _finmind_cache[stock_id]={'pass':None,'ts':now}; return None
        result=cur_l<(cur_a/1.5)
        _finmind_cache[stock_id]={'pass':result,'ts':now}; return result
    except: _finmind_cache[stock_id]={'pass':None,'ts':now}; return None


def check_institutional_buying(stock_id):
    global _finmind_cache
    """✅ v05171758：法人合計淨買條件（外資+投信+自營，加分制）
    來源：FinMind TaiwanStockInstitutionalInvestorsBuySell（日報）
    說明：主力強度為三竹/eleader平台專屬指標，無公開API
          本函數以「法人合計淨買」作為最接近的替代方案
    條件：(外資淨買 + 投信淨買 + 自營淨買) > 0 → 法人合計大買
    快取：1天（日報，每天更新）
    費用：48支×1次/天=48次，遠低於FinMind免費額度600次/天
    回傳：{'net_buy': float, 'is_buying': bool, 'detail': str}
    """
    global _finmind_cache
    from datetime import datetime as _dt, timedelta as _td
    _cache_key = f'inst_{stock_id}'
    now = _dt.now()
    if _cache_key in _finmind_cache:
        c = _finmind_cache[_cache_key]
        if (now - c['ts']).total_seconds() / 3600 < 24: return c['data']
    _empty = {'net_buy': 0, 'is_buying': False, 'detail': '無資料'}
    try:
        import requests as _req
        # 取最近5個交易日（確保有最新資料）
        date_from = (now - _td(days=10)).strftime('%Y-%m-%d')
        r = _req.get('https://api.finmindtrade.com/api/v4/data',
            params={'dataset': 'TaiwanStockInstitutionalInvestorsBuySell',
                    'stock_id': stock_id, 'date': date_from,
                    'token': FINMIND_TOKEN}, timeout=15)
        if r.status_code != 200:
            _finmind_cache[_cache_key] = {'data': _empty, 'ts': now}; return _empty
        d = r.json()
        if d.get('status') != 200 or not d.get('data'):
            _finmind_cache[_cache_key] = {'data': _empty, 'ts': now}; return _empty
        rows = sorted(d['data'], key=lambda x: x.get('date',''), reverse=True)
        # 取最新一天的資料
        latest_date = rows[0].get('date','') if rows else ''
        latest = [r for r in rows if r.get('date') == latest_date]
        # 計算各法人淨買
        net = {}
        for item in latest:
            name = item.get('name','')
            buy  = float(item.get('buy',0) or 0)
            sell = float(item.get('sell',0) or 0)
            net[name] = buy - sell
        foreign    = net.get('外資', 0)
        trust      = net.get('投信', 0)
        dealer     = net.get('自營商', net.get('自營', 0))
        total      = foreign + trust + dealer
        detail = f"外資{'+' if foreign>=0 else ''}{int(foreign/1000)}K 投信{'+' if trust>=0 else ''}{int(trust/1000)}K 自營{'+' if dealer>=0 else ''}{int(dealer/1000)}K"
        result = {'net_buy': total, 'is_buying': total > 0, 'detail': detail}
        _finmind_cache[_cache_key] = {'data': result, 'ts': now}; return result
    except Exception as _e:
        _finmind_cache[_cache_key] = {'data': _empty, 'ts': now}; return _empty


def apply_institutional_bonus_score(buy_signals):
    """✅ v05171758：法人合計大買加分排序（>4支才啟用，與集保大戶共同排序）
    法人合計淨買>0 → +1分（疊加集保大戶的分數）
    """
    if len(buy_signals) <= 4: return buy_signals
    print(f"  💼 法人合計淨買加分（{len(buy_signals)}支>4支，啟用）")
    scored = []
    for item in buy_signals:
        code = item[1] if isinstance(item, tuple) and len(item) > 1 else (item[0][1] if isinstance(item, tuple) and isinstance(item[0], tuple) else '')
        # 相容已有TDCC分數的格式
        if isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], int):
            prev_score, s = item
            code = s[1]
        else:
            prev_score = 0; s = item
        if not (code.isdigit() and len(code) == 4):
            scored.append((prev_score, s)); continue
        inst = check_institutional_buying(code)
        sc = prev_score + (1 if inst['is_buying'] else 0)
        print(f"    {code}: {inst['detail']} {'✅法人大買+1分' if inst['is_buying'] else '→持平'} 合計{sc}分")
        scored.append((sc, s))
    scored.sort(key=lambda x: x[0], reverse=True)
    print(f"  ✅ 法人+集保加分完成，最高{scored[0][0] if scored else 0}分排前面")
    return [s for _, s in scored]


def apply_finmind_filter(stock_codes):
    global _finmind_cache
    """✅ v05170954：FinMind財務篩選（<4支不啟用）"""
    if not stock_codes: return stock_codes
    results={c:check_financial_health_finmind(c) for c in stock_codes}
    passed=[c for c in stock_codes if results.get(c) is not False]
    pass_count=len([c for c in stock_codes if results.get(c) is True])
    if pass_count<FINMIND_MIN_PASS:
        print(f"  ℹ️ FinMind財務篩選：通過{pass_count}支<最低{FINMIND_MIN_PASS}支，不啟用"); return stock_codes
    print(f"  ✅ FinMind財務篩選：{pass_count}支通過，已篩除{len(stock_codes)-len(passed)}支")
    return passed


def check_tdcc_holder_trend(stock_id):
    """✅ v05171629：集保大戶持股趨勢（加分制，7天快取，週報不超FinMind免費額度）
    大戶=400張以上(level 6~10)，散戶=200張以下(level 1~4)，連1週即符合
    """
    global _finmind_cache
    from datetime import datetime as _dt,timedelta as _td
    _ck=f'tdcc_{stock_id}'; now=_dt.now()
    if _ck in _finmind_cache:
        c=_finmind_cache[_ck]
        if (now-c['ts']).total_seconds()/3600<168: return c['data']
    _e={'big_up':False,'small_down':False,'big_pct':0,'small_pct':0}
    try:
        import requests as _req
        r=_req.get('https://api.finmindtrade.com/api/v4/data',
            params={'dataset':'TaiwanStockHoldingSharesPer','stock_id':stock_id,
                    'date':(now-_td(days=90)).strftime('%Y-%m-%d'),'token':FINMIND_TOKEN},timeout=15)
        if r.status_code!=200: _finmind_cache[_ck]={'data':_e,'ts':now}; return _e
        d=r.json()
        if d.get('status')!=200 or not d.get('data'): _finmind_cache[_ck]={'data':_e,'ts':now}; return _e
        rows=d['data']
        dates=sorted(set(x['date'] for x in rows),reverse=True)[:2]
        if len(dates)<2: _finmind_cache[_ck]={'data':_e,'ts':now}; return _e
        def _sp(rows,dt,lvls): return sum(float(x.get('HoldingPer',0) or 0) for x in rows if x.get('date')==dt and int(x.get('HoldingLevel',0)) in lvls)
        bn=_sp(rows,dates[0],{6,7,8,9,10}); bp2=_sp(rows,dates[1],{6,7,8,9,10})
        sn=_sp(rows,dates[0],{1,2,3,4}); sp2=_sp(rows,dates[1],{1,2,3,4})
        res={'big_up':bn>bp2,'small_down':sn<sp2,'big_pct':round(bn-bp2,3),'small_pct':round(sn-sp2,3)}
        _finmind_cache[_ck]={'data':res,'ts':now}; return res
    except: _finmind_cache[_ck]={'data':_e,'ts':now}; return _e


def apply_tdcc_bonus_score(buy_signals):
    global _finmind_cache
    """✅ v05171629：集保大戶加分排序（>4支才啟用，加分制不阻斷通知）"""
    if len(buy_signals)<=4: return buy_signals
    print(f"  🏆 集保大戶加分排序（{len(buy_signals)}支>4支，啟用）")
    scored=[]
    for s in buy_signals:
        code=s[1]
        if not(code.isdigit() and len(code)==4): scored.append((0,s)); continue
        t=check_tdcc_holder_trend(code)
        sc=(1 if t['big_up'] else 0)+(1 if t['small_down'] else 0)
        print(f"    {code}: 大戶{'↑+'+str(t['big_pct'])+'%' if t['big_up'] else '→'} 散戶{'↓'+str(abs(t['small_pct']))+'%' if t['small_down'] else '→'} +{sc}分")
        scored.append((sc,s))
    scored.sort(key=lambda x:x[0],reverse=True)
    print(f"  ✅ 排序完成，最高{scored[0][0] if scored else 0}分排前面")
    return [s for _,s in scored]


def process_pending_gmail_requests(now_str):
    """✅ v05172348：處理網頁版發起的Gmail通知請求（pending_gmail_*）
    網頁版用戶開啟Gmail通知後，掃到訊號會寫入Firebase
    Python每次掃描前讀取並發送，發送後刪除請求
    """
    try:
        import json, os, requests as _req
        from datetime import datetime; import pytz
        cred_json = os.environ.get(FIREBASE_CRED_ENV)
        if not cred_json:
            _cf = os.path.join(os.path.dirname(os.path.abspath(__file__)), FIREBASE_CRED_FILE)
            if os.path.exists(_cf):
                with open(_cf, 'r', encoding='utf-8') as f: cred_json = f.read()
        if not cred_json: return
        import google.oauth2.service_account as _sa, google.auth.transport.requests as _gtr
        _c = _sa.Credentials.from_service_account_info(json.loads(cred_json),
            scopes=['https://www.googleapis.com/auth/datastore'])
        _c.refresh(_gtr.Request())
        _base = (f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
                 f"/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}/public")
        # 列出所有pending_gmail開頭的文件
        _list_r = _req.get(f"{_base}", headers={"Authorization": f"Bearer {_c.token}"}, timeout=10)
        if _list_r.status_code != 200: return
        _docs = _list_r.json().get('documents', [])
        _pending = [d for d in _docs if '/pending_gmail_' in d.get('name','')]
        if not _pending: return
        print(f"  📧 發現{len(_pending)}個網頁版Gmail通知請求，處理中...")
        for doc in _pending:
            try:
                fields = doc.get('fields', {})
                ticker    = fields.get('ticker',    {}).get('stringValue','')
                signal    = fields.get('signal',    {}).get('stringValue','')
                price     = fields.get('price',     {}).get('doubleValue', 0)
                condition = fields.get('condition', {}).get('stringValue','')
                time_str  = fields.get('time',      {}).get('stringValue','')
                user      = fields.get('user',      {}).get('stringValue','')
                sent      = fields.get('sent',      {}).get('booleanValue', False)
                if sent: continue
                msg = (f"📱【網頁版{signal}訊號】{ticker}\n"
                       f"條件：{condition}\n"
                       f"市價：{price:.2f}\n"
                       f"時間：{time_str}\n"
                       f"用戶：{user}\n"
                       "⚠️ 本訊號由網頁版掃描觸發")
                
                ok = send_gmail(f"📱【網頁版】{signal} {ticker} - {time_str}", msg)
                if ok:
                    # 刪除已處理的請求
                    doc_name = doc['name']
                    _req.delete(f"https://firestore.googleapis.com/v1/{doc_name}",
                               headers={"Authorization": f"Bearer {_c.token}"}, timeout=10)
                    print(f"    ✅ {ticker} Gmail已發送，請求已刪除")
            except Exception as _e:
                print(f"    ⚠️ 處理請求失敗：{_e}")
    except Exception as _e:
        print(f"  ⚠️ process_pending_gmail失敗：{_e}")


def write_scan_status_to_firebase(buy_count, sell_count, now_str):
    """✅ v05170940：寫入掃描完成狀態到Firebase，供網頁版「上次掃描時間」顯示使用"""
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
        _url = (f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
                f"/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}"
                f"/public/futures_status")
        _status = 'signal' if (buy_count + sell_count) > 0 else 'completed'
        _r = _req.patch(_url, timeout=15,
            headers={"Authorization": f"Bearer {_c.token}", "Content-Type": "application/json"},
            json={"fields": {
                "status":       {"stringValue": _status},
                "scan_time":    {"stringValue": now_str},
                "signal_count": {"integerValue": str(buy_count + sell_count)},
                "buy_count":    {"integerValue": str(buy_count)},
                "sell_count":   {"integerValue": str(sell_count)},
                "updated_at":   {"stringValue": now_str},
                "source":       {"stringValue": "python"}
            }})
        return _r.status_code in (200, 201)
    except Exception as _e:
        print(f"  ⚠️ write_scan_status 失敗：{_e}")
        return False

def write_tw_stock_names():
    """✅ 05101039：將twstock中文名稱對照表上傳Firebase（週六補跑時執行）"""
    try:
        import json, os, requests as _req, twstock
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
        # 只取4位數字代碼的台股（過濾權證等）
        _names = {}
        for code, info in twstock.codes.items():
            if str(code).isdigit() and len(str(code)) == 4:
                _names[code] = info.name
        _url = (f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
                f"/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}/public/tw_stock_names")
        _r = _req.patch(_url, timeout=30,
            headers={"Authorization": f"Bearer {_c.token}", "Content-Type": "application/json"},
            json={"fields": {
                "names": {"stringValue": json.dumps(_names, ensure_ascii=False)},
                "count": {"integerValue": str(len(_names))},
                "updated_at": {"stringValue": _now}}})
        if _r.status_code in (200, 201):
            print(f"  ✅ Firebase 台股中文名稱已更新：{len(_names)} 支 ({_now})")
            return True
        print(f"  ❌ 名稱寫入失敗：{_r.status_code}"); return False
    except Exception as e: print(f"  ⚠️ 名稱寫入異常：{e}"); return False

def write_tw_prescreened(codes_list, indicators_dict=None):
    """將預篩台股（代碼清單+第二道指標）寫入Firebase
    ✅ 05052224：平日採合併模式（只增不減），週六採覆蓋模式（完整重建）
    """
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
        _tz = pytz.timezone('Asia/Taipei')
        _today_wd = datetime.now(_tz).weekday()  # 0=週一 ... 5=週六 6=週日
        _is_saturday = (_today_wd == 5)

        # ── 週六：完整覆蓋重建 ──────────────────────────────────
        if _is_saturday:
            _merged = list(codes_list)
            _merged_ind = dict(indicators_dict) if indicators_dict else {}
            print(f"  📅 週六模式：完整覆蓋重建 {len(_merged)} 支")
        else:
            # ── 平日：先讀取舊清單，合併今日通過的（只增不減）──
            _existing = []
            _existing_ind = {}
            _resp = _req.get(_url, headers={"Authorization": f"Bearer {_c.token}"}, timeout=10)
            if _resp.status_code == 200:
                _fields = _resp.json().get('fields', {})
                _codes_raw = _fields.get('codes', {}).get('arrayValue', {}).get('values', [])
                _existing = [v.get('stringValue','') for v in _codes_raw if v.get('stringValue')]
                try:
                    _ind_str = _fields.get('indicators', {}).get('stringValue', '{}')
                    _existing_ind = json.loads(_ind_str)
                except Exception: _existing_ind = {}
            # 合併：取聯集，今日指標覆蓋同代碼的舊指標
            _merged_set = set(_existing) | set(codes_list)
            _merged = list(_merged_set)
            _merged_ind = {**_existing_ind, **(indicators_dict or {})}
            _added = len(_merged_set) - len(set(_existing))
            print(f"  📅 平日模式：舊清單 {len(_existing)} 支 + 今日新增 {_added} 支 = 合併 {len(_merged)} 支")

        _r = _req.patch(_url, timeout=15,
            headers={"Authorization": f"Bearer {_c.token}", "Content-Type": "application/json"},
            json={"fields": {
                "codes": {"arrayValue": {"values": [{"stringValue": c} for c in _merged]}},
                "count": {"integerValue": str(len(_merged))},
                "updated_at": {"stringValue": _now},
                **({"indicators": {"stringValue": json.dumps(_merged_ind, ensure_ascii=False)}} if _merged_ind else {})}})
        if _r.status_code in (200,201):
            _ind_cnt = len(_merged_ind)
            print(f"  ✅ Firebase 預篩清單+指標已更新：{len(_merged)} 支，指標 {_ind_cnt} 支 ({_now})"); return True
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
    """混合模式：月K第一道 OR 週K第一道（任一通過）+ 日K第二道"""  # v06100610：修正
    global SCAN_MODE
    global _wk_passed_1st
    # ✅ 月K先跑（長期投資），先清旗標  # v06100610：修正錯誤註解
    _wk_passed_1st = False
    SCAN_MODE = 'weekly'
    r_w = scan_stock(ticker, is_holding, _mode_label='月K')
    SCAN_MODE = 'mixed'
    if r_w and r_w[0] in ('BUY', 'SHORT'):
        return r_w + ('長期投資(月K)',)  # ✅ v06100610：明確標示月K觸發
    # ✅ 週K再跑（中期投資），用旗標判斷月K是否也通過第一道  # v06100610：修正
    _day_label = '月K+週K' if _wk_passed_1st else '週K'
    SCAN_MODE = 'daily'
    r_d = scan_stock(ticker, is_holding, _mode_label=_day_label)
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
    _tse_mkt = None  # ✅ v06100131：移至最頂部，無論台股/美股時段都已初始化
    if TEST_MODE == '5mk':
        print('\n📊 台股：期貨5分K模式，跳過台股掃描')
    elif 'TW' not in active_markets:
        print('\n📊 台股：非交易時段，跳過')
    else:
        # 🟢 台股大盤守門員（A/B/C/D/E多空全條件，移到底部顯示）
        _tse_mkt = analyse_market_index('^TWII', '台股')
        # ✅ v06100600：_tse_mkt可能為None（美股夜間掃描），加None保護
        _tw_market_bull_abc = _tse_mkt.get('bull_abc', False) if _tse_mkt else False
        _tw_market_bull_d   = _tse_mkt.get('bull_d',   False) if _tse_mkt else False
        _tw_market_bear_abc = _tse_mkt.get('bear_abc', False) if _tse_mkt else False
        _tw_market_bear_d   = _tse_mkt.get('bear_d',   False) if _tse_mkt else False
        _tw_market_warn     = _tse_mkt.get('warn',     False) if _tse_mkt else False

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

        # ── 週六全量掃描 OR 平日使用Firebase快取 ──────────────────
        # ✅ v05201555：週六強制全量1827支重掃（定期更新預篩清單）
        _is_saturday_scan = (datetime.now().weekday() == 5) or (SCAN_TYPE == 'tw_full')
        _fb_cache = read_tw_prescreened()
        if not _is_saturday_scan and _fb_cache and len(_fb_cache.get('codes', [])) > 0:
            _cached_codes = _fb_cache['codes']
            _cache_time   = _fb_cache.get('updated_at', '—')
            _ticker_map   = {t.split('.')[0]: t for t in tw_list}
            tw_list       = [_ticker_map[c] for c in _cached_codes if c in _ticker_map]
            print(f'\n📊 台股：使用 Firebase AI預篩快取（更新：{_cache_time}）')
            print(f'   快取共 {len(_cached_codes)} 支 → 本次掃描 {len(tw_list)} 支')
        else:
            if _is_saturday_scan:
                print(f'\n📊 台股：週六全量掃描 1827支（強制更新預篩清單）')
            else:
                print(f'\n📊 台股：Firebase無預篩快取，執行完整掃描')

        total_tw = len(tw_list)
        print(f'📊 台股掃描：共{total_tw}支')
        global _tw_prescreened
        _tw_prescreened = []
        _prescreened_ind = {}  # ✅ 05041037

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
            # ✅ v05170954：FinMind財務篩選（流動比率>1.5，<4支不啟用）
            if _tw_prescreened:
                print("  🔍 FinMind財務篩選中...")
                _tw_prescreened = apply_finmind_filter(_tw_prescreened)
            print(f'\n🔍 正在將 {len(_tw_prescreened)} 支預篩台股寫入Firebase...')
            write_tw_prescreened(_tw_prescreened, _prescreened_ind)  # ✅ 05041037
            # ✅ 05101039：週六補跑時同步更新中文名稱（直接判斷weekday，不依賴函數內部變數）
            import pytz as _ptz_sat
            if __import__('datetime').datetime.now(_ptz_sat.timezone('Asia/Taipei')).weekday() == 5:
                print("\n🔍 正在更新台股中文名稱對照表...")
                write_tw_stock_names()
        else:
            print('\n🔍 本次預篩：無台股通過條件')

# ── 美股掃描（加入道瓊週K過濾，對應截圖轉折邏輯） ──────────
    if TEST_MODE == '5mk':
        print('\n📊 美股：期貨5分K模式，跳過美股掃描')
    elif 'US' not in active_markets:
        print('\n📊 美股：非交易時段，跳過')
    else:
        # ✅ v06130522：EWT檢查移到底部（不需往上翻）
        # check_overnight_extreme_move() → 已移至底部統整區
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

        # ✅ v06081833：台股收盤後執行漲停追蹤（14:00後，每日一次）
        _scan_hour = datetime.now().hour
        if SCAN_TYPE in ('tw','mixed') and _scan_hour >= 14:
            scan_limit_up()
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
                # ✅【三道關卡】期貨5分K進場先決條件 v05170920更新
                # 第一道：EWT 30分K位階（覆蓋台灣夜盤，美市09:30~16:00 ET=台灣21:30~04:00）
                #         → 同條件check_buy_precondition，餵30分K資料
                #         → 夜盤15:00~05:00已有28根bar，09:10前完整
                #         → 符合「不把日當日看」原則，同eleader切換K線週期
                # 第二道：月K eLeader條件（check_buy_eleader，df_w為月K）
                # 第三道：5分K 54根條件A/B + RSI門檻（BUY_LOOKBACK_5MK=54根）
                # ══════════════════════════════════════════════

                # ── 第一道：30分K位階（含夜盤） ──────────────────────────────
                # ✅ v05170920：改用30分K取代日K作為5分K策略的第一道
                # 【設計理由】：
                #   eLeader策略實測發現，需在09:10~09:20就判斷當天多空
                #   → 日K在09:30才有第1根，太慢
                #   → 30分K：夜盤(15:00~隔日05:00)已有28根完整bar
                #   → 09:10時，這28根夜盤bar已完整，可以直接評估A轉/V轉
                #   eLeader也是把夜盤收盤前幾根K棒納入多空趨勢評估
                #
                # 【不把日當日看的實踐】：
                #   同一套條件（check_buy_precondition）
                #   餵入30分K資料 → 評估30分K位階
                #   不是評估「日K位階」，而是「30分K位階」
                #   但判斷邏輯完全相同，符合eleader切換K線週期原則
                # ✅ v05181836：5分K策略先大後小原則（先日K後EWT 30分K後5分K）
                # 【正確先大後小架構】：
                #   第一道 → 日K（事不過三=3個交易日）← 大週期，看整體位階
                #   第二道 → EWT 30分K（夜盤方向）← 中週期，確認隔夜方向
                #   進場   → ^TWII 5分K ← 小週期，精確入場時機
                # 【為何日K在前】：
                #   昨日13:30收盤後日K完整成形，代表昨日整體位階
                #   符合「先看大週期，後看小週期」投資定律
                # 【EWT設計理由維持不變】：
                #   EWT=iShares MSCI Taiwan ETF，NYSE交易
                #   ET 09:30~16:00 = 台灣夜盤21:30~04:00
                #   完整覆蓋台灣夜盤，Yahoo Finance資料穩定
                import yfinance as _yf30
                # 第一道：日K（事不過三=3個交易日）
                _df_daily_5mk = _yf30.download(ticker, period='60d', interval='1d', progress=False)
                if _df_daily_5mk is not None and not _df_daily_5mk.empty:
                    _df_daily_5mk = calc_indicators(_df_daily_5mk)
                    # ✅ v05192327：5分K第一道日K加E和F
                    _1st_daily_long  = (signal_within_n(lambda d: check_buy_precondition(d)[0], _df_daily_5mk, n=3, reverse_check=check_sell_condition) or
                                        check_condE_long(_df_daily_5mk) or
                                        (check_buy_eleader(_df_daily_5mk) is not None))
                    _1st_daily_short = (signal_within_n(lambda d: check_short_precondition(d)[0], _df_daily_5mk, n=3) or
                                        check_condE_short(_df_daily_5mk) or
                                        (check_short_eleader(_df_daily_5mk) is not None))
                else:
                    _1st_daily_long = _1st_daily_short = False
                # 第二道：EWT 30分K（夜盤方向確認）
                _df30 = _yf30.download('EWT', period='5d', interval='30m', progress=False)
                if _df30 is None or len(_df30) < 20:
                    print(f'  ⚠️ EWT 30分K資料不足，跳過')
                    continue
                df5_d = calc_indicators(_df30)
                if df5_d is None: continue
                _orig5 = BUY_LOOKBACK_BARS
                globals()['BUY_LOOKBACK_BARS'] = BUY_LOOKBACK_DAILY
                # ✅ v05181836：第一道=日K AND EWT 30分K雙重確認
                # ✅ v05192327：5分K第二道EWT 30分K加E和F
                _ewt_long  = ((check_buy_precondition(df5_d)[0] if df5_d is not None else False) or
                              (check_condE_long(df5_d) if df5_d is not None else False) or
                              (check_buy_eleader(df5_d) is not None if df5_d is not None else False))
                _ewt_short = ((check_short_precondition(df5_d)[0] if df5_d is not None else False) or
                              (check_condE_short(df5_d) if df5_d is not None else False) or
                              (check_short_eleader(df5_d) is not None if df5_d is not None else False))
                _1st_long  = _1st_daily_long  and _ewt_long
                _1st_short = _1st_daily_short and _ewt_short
                if not _1st_long and not _1st_short:
                    print(f'  ❌ {ticker} 第一道(日K AND EWT 30分K)未通過，跳過'); continue
                print(f'  ✅ {ticker} 第一道(日K AND EWT 30分K)通過（多:{_1st_long} 空:{_1st_short}）')

                # ── 第二道：日K eLeader ──────────────────────
                # （df5_d已在上面取得，直接使用）
                if check_buy_eleader(df5_d) is None:
                    print(f'  ❌ {ticker} 第二道日K eLeader未通過，跳過')
                    continue
                print(f'  ✅ {ticker} 第二道日K eLeader通過')

                # ── 第三道：5分K 54根條件A/B ────────────────
                # ✅【夜盤保留】主力以夜盤為主戰場，不剔除夜盤
                # period='5d' 確保足夠近期夜盤+日盤K棒（約1000+根）
                df5 = _normalize_df(yf.download(ticker, period='5d', interval='5m', progress=False))
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
                # ✅ v05192313：5分K進場加入條件D（追高/追空）
                _5mk_cond_D_long  = getattr(df5.iloc[-1],'rsi14',0) > getattr(df5.iloc[-2],'rsi14',0) and near_upper
                _5mk_cond_D_short = getattr(df5.iloc[-1],'rsi14',0) < getattr(df5.iloc[-2],'rsi14',0) and near_lower
                # ✅ v05192327：5分K進場加E和F
                _5mk_cond_E_long = check_condE_long(df5) if df5 is not None else False
                _5mk_cond_F_long = check_buy_eleader(df5) is not None if df5 is not None else False
                _5mk_buy = (_5mk_cond_A or _5mk_cond_B or _5mk_cond_D_long or
                            _5mk_cond_E_long or _5mk_cond_F_long) and rsi_now > BUY_RSI_MIN

                near_lower   = close  <= boll_bot * BUY_BOLL_TOLERANCE
                near_upper   = close  >= boll_top * 1.00
                now_str_f = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M')
                # ── 日K位階參考（印出但不擋住掃描）──────────────
                try:
                    df_d5 = _normalize_df(yf.download(ticker, period='10d', interval='1d', progress=False))
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
                        # ✅ 05101133：週三/週五加入週選擇權推薦
                        _wd_now = __import__('datetime').datetime.now(__import__('pytz').timezone('Asia/Taipei')).weekday()
                        _hour_now = __import__('datetime').datetime.now(__import__('pytz').timezone('Asia/Taipei')).hour
                        _min_now  = __import__('datetime').datetime.now(__import__('pytz').timezone('Asia/Taipei')).minute
                        _in_opt_window = (_wd_now in (2,4)) and (9*60+5 <= _hour_now*60+_min_now <= 10*60+45)
                        _opt_hint = get_weekly_option_hint(close, 'buy') if _in_opt_window else ""
                        msg = (
                            f"💻【本機】⭐【期貨5分K買進訊號】⭐\n"
                            f"標的：{ticker}\n"
                            f"收盤：{close:.2f}　布林下緣：{boll_bot:.2f}\n"
                            f"RSI：{rsi_prev:.1f} → {rsi_now:.1f}（↑）\n"
                            f"時間：{now_str_f}"
                            + _opt_hint
                        )
                        _ok = send_gmail(f"💻【本機】⭐期貨5分K買進 {ticker} - {now_str_f}", msg)
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
                        _wd_now2 = __import__('datetime').datetime.now(__import__('pytz').timezone('Asia/Taipei')).weekday()
                        _hour_now2 = __import__('datetime').datetime.now(__import__('pytz').timezone('Asia/Taipei')).hour
                        _min_now2  = __import__('datetime').datetime.now(__import__('pytz').timezone('Asia/Taipei')).minute
                        _in_opt_window2 = (_wd_now2 in (2,4)) and (9*60+5 <= _hour_now2*60+_min_now2 <= 10*60+45)
                        _opt_hint2 = get_weekly_option_hint(close, 'sell') if _in_opt_window2 else ""
                        msg = (
                            f"💻【本機】🔔【期貨5分K平倉訊號】🔔\n"
                            f"標的：{ticker}\n"
                            f"收盤：{close:.2f}　布林上緣：{boll_top:.2f}\n"
                            f"RSI：{rsi_prev:.1f} → {rsi_now:.1f}（↓）\n"
                            f"時間：{now_str_f}"
                            + _opt_hint2
                        )
                        _ok = send_gmail(f"💻【本機】🔔期貨5分K平倉 {ticker} - {now_str_f}", msg)
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
                            f"💻【本機】🔻【期貨5分K做空訊號】🔻\n"
                            f"標的：{ticker}\n"
                            f"收盤：{close:.2f}　布林上軌：{boll_top:.2f}\n"
                            f"RSI：{rsi_prev:.1f} → {rsi_now:.1f}（↓）\n"
                            f"時間：{now_str_f}"
                        )
                        _ok = send_gmail(f"💻【本機】🔻期貨5分K做空 {ticker} - {now_str_f}", msg)
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
                            f"💻【本機】🟢【期貨5分K平空回補】🟢\n"
                            f"標的：{ticker}\n"
                            f"收盤：{close:.2f}　布林下軌：{boll_bot:.2f}\n"
                            f"RSI：{rsi_prev:.1f} → {rsi_now:.1f}（↑）\n"
                            f"時間：{now_str_f}"
                        )
                        _ok = send_gmail(f"💻【本機】🟢期貨5分K平空回補 {ticker} - {now_str_f}", msg)
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
    # ✅ v05172348：處理網頁版發起的Gmail通知請求
    process_pending_gmail_requests(now_str)
    print(f"\n{'='*55}")
    print(f"  掃描完成！買進訊號：{len(buy_signals)}支 / 賣出訊號：{len(sell_signals)}支 / 下市警報：{len(delist_signals)}支")
    print(f"{'='*55}")
    # ✅ v05170940：寫入掃描狀態到Firebase供網頁版「上次掃描時間」顯示
    write_scan_status_to_firebase(len(buy_signals), len(sell_signals), now_str)
    # ✅ v05171047：輸出篩選結果到CSV和JSON，方便複製代碼到eleader/三竹
    # ✅ v05171629+v05171758：集保大戶+法人大買雙重加分排序（>4支才啟用）
    if len(buy_signals) > 4:
        buy_signals = apply_tdcc_bonus_score(buy_signals)
        buy_signals = apply_institutional_bonus_score(buy_signals)
    _export_scan_results(buy_signals, sell_signals, now_str)
    # ✅ v05171047：底部統整大盤位階和符合策略股票清單
    # ✅ v05200928：台股大盤位階判定移到底部（不用往上翻）
    if _tse_mkt is not None and _tse_mkt:
        _flags = ' / '.join(_tse_mkt.get('flags',['中性觀望']))
        print(f"\n🔍 台股大盤位階：{_flags}")
        print(f"   月K RSI:{_tse_mkt.get('rsi_mo',_tse_mkt.get('rsi_w',0)):.1f} MACD{_tse_mkt.get('macd_mo','?')}  "
              f"週K RSI:{_tse_mkt.get('rsi_wk',0):.1f} MACD{_tse_mkt.get('macd_wk','?')}  "
              f"日K RSI:{_tse_mkt.get('rsi_d',0):.1f} MACD{_tse_mkt.get('macd_d','?')}")
    # ✅ v06130522：極端異動警報移到底部（不需往上翻）
    if 'US' in active_markets or 'CRYPTO' in active_markets:
        print('\n🔍 檢查台指夜盤極端異動（EWT代理）...')
        check_overnight_extreme_move()
    if 'TW' in active_markets and _tse_mkt is not None:
        check_tw_daytime_extreme(_tse_mkt)
    _print_scan_summary(buy_signals, sell_signals)

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

            subject = f"💻【本機】❌下市警報 {len(hold_list)}支持有須出清/{len(watch_list)}支觀察 - {now_str}"
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

            send_gmail(f"💻【本機】⭐【{_get_period_label(_signal_label)}】做多進場 {len(filtered)}支 - {now_str}", body)
            save_notified(notified)
            # ✅ 05111049：寫入買進訊號到Firebase供網頁版T+2追蹤
            for _s in filtered:
                _mkt, _cd, _c2, *_ = _s
                write_buy_signal_firebase(_cd, _c2, '⭐做多進場', now_str, _mkt)
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

            send_gmail(f"💻【本機】🔔【{_get_period_label(_signal_label)}】出場訊號 {len(filtered)}支 - {now_str}", body)
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
            (wd_f == 0 and tv_f >= 12*60+50) or                             # 週一12:50後
            (wd_f == 1 and (tv_f < 1*60 or tv_f >= 5*60)) or            # 週二（排除深夜01:00~05:00）
            (wd_f == 2 and tv_f <= 11*60+30)                             # 週三11:30前
        )

        if not in_futures:
            print(f"[{test_now}] ❌ 非期貨5分K時段（週一12:50~週三11:30，深夜01~05除外），直接結束")
            time.sleep(5)
            exit()

        print(f"🚀 期貨5分K模式啟動（週一12:50~週三11:30）")
        print(f"   標的：{FUTURES_5MK_TARGETS}　間隔：{FUTURES_5MK_INTERVAL}秒")
        print(f"   專屬帳號：{FUTURES_5MK_OWNER}")

        # 持續掃描直到週三11:30結束
        while True:
            now_loop = datetime.now(pytz.timezone('Asia/Taipei'))
            wd_l  = now_loop.weekday()
            tv_l  = now_loop.hour * 60 + now_loop.minute
            _is_night = (wd_l == 1 and 1*60 <= tv_l < 5*60)  # 週二深夜01~05
            in_f  = (
                (wd_l == 0 and tv_l >= 12*60+50) or
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
            # ✅ 05052251：無訊號時5秒後結束（工作排程器每5分鐘自動再觸發）
            print("⏱️ 本次無訊號，5秒後結束")
            time.sleep(5)
            break
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
        time.sleep(5)
        exit()

    # === [正式監控模式]：TEST_MODE 為 False 時才會進入以下邏輯 ===
    hour   = now.hour
    minute = now.minute  # ✅ minute 必須定義，否則時段判斷會 NameError
    loops = 0
    market_name = ""

    # ✅ v05190013：GitHub Actions直接依SCAN_TYPE執行，跳過時段限制
    if IS_GITHUB_ACTIONS:
        if SCAN_TYPE == 'futures':
            loops = 1; market_name = '期貨5分K（GitHub Actions）'
        else:
            loops = 1; market_name = '台股（GitHub Actions）'
    # 台股：開盤後5分(09:05) ~ 收盤前5分(13:25)
    elif (hour == 9 and minute >= 5) or (10 <= hour <= 12) or (hour == 13 and minute <= 25):
        loops = 9
        market_name = "台股"
    # ✅ v05190013：22:35~23:30 本機版台股夜間補掃（使用證交所20:00後更新的資料）
    elif (hour == 22 and minute >= 35) or (hour == 23 and minute <= 30):
        loops = 1
        market_name = "台股（22:35夜間補掃）"
    # 美股：23:31~ ~ 收盤前5分(03:55)，夏冬令統一此區間
    elif (hour == 23 and minute > 30) or (0 <= hour <= 2) or (hour == 3 and minute <= 55):
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
            time.sleep(5)  # ✅ 05052251
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
        time.sleep(5)  # ✅ 05052251
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
# ============================================================
# 專案：Python股票週K布林RSI+Gmail推播自動通知
# 版本：3.6
# 更新日期：2026/03/23（新增期貨5分K模式）
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
TEST_MODE = False   # 切換：False / True / '5mk'

# ── 【期貨5分K專屬設定】──────────────────────────────────────────
FUTURES_5MK_TARGETS  = ['^TWII']   # 台指期替代標的
FUTURES_5MK_INTERVAL = 300         # 每5分鐘掃描一次
FUTURES_5MK_OWNER    = 'shchyu61@gmail.com'  # 5分K模式專屬帳號

# Gmail設定
# ✅ 本機執行：直接填入帳號密碼
# ✅ GitHub Actions執行：自動從 GitHub Secrets 讀取，不需填寫
import os as _os
GMAIL_ACCOUNT  = _os.environ.get("GMAIL_ACCOUNT",  "shchyu61@gmail.com")
GMAIL_PASSWORD = _os.environ.get("GMAIL_PASSWORD", "")  # 本機填入；雲端從Secrets讀取
NOTIFY_EMAIL   = "shchyu61@gmail.com"       # 收通知的信箱

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
# 【２．策略參數設定區】← 所有策略的可調數值都在這裡，不需往下翻
# ============================================================

# ── 【２-1】買進策略：截圖3、4條件（條件A + 條件B）──────────────
# 條件A：近N根任一最低價 <= 布林下緣 AND RSI上升 AND MACD柱放大
# 條件B：近N根Low均<布林中軌 AND 近N根High均<布林上軌
#         AND 前N根MACD柱持續縮小 AND 當根MACD柱放大
BUY_LOOKBACK_BARS    = 3      # 近幾根K棒回看數（條件A/B共用）
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

    # 期貨5分K時段：週一15:01~週三13:30
    is_futures_time = False
    if weekday == 0 and time_val >= 15*60+1:
        is_futures_time = True
    elif weekday == 1:
        is_futures_time = True
    elif weekday == 2 and time_val <= 13*60+30:
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

        # 1. 抓取週K (第1層)
        df_w = get_stock_data(ticker, period='2y', interval='1wk', cache=weekly_cache)
        if df_w is None or len(df_w) < 30: return None
        
        # 計算週K技術指標
        df_w['boll_top20'], df_w['boll_mid20'], df_w['boll_bot20'] = ta.bbands(df_w['Close'], length=20).iloc[:, 0:3].values.T
        df_w['rsi14'] = ta.rsi(df_w['Close'], length=14)
        
        # 🔴 [優先處理賣出] (使用您 10:01 以來的上緣賣出邏輯)
        if is_holding:
            if check_sell_condition(df_w):
                c_price = float(df_w['Close'].iloc[-1])
                return ('SELL', c_price, float(df_w['High'].iloc[-1]), float(df_w['boll_top20'].iloc[-1]), 
                        float(df_w['rsi14'].iloc[-1]), float(df_w['rsi14'].iloc[-2]))
            return None # 持有中但未達賣點，跳過買入判斷

       # 2. 買入判斷 - 第一層：週K位階 (低檔且超賣)
        # ✅ [新增測試模式邏輯]：若開啟 TEST_MODE，則跳過此硬性門檻
        if not TEST_MODE:
            if not (df_w['Low'].iloc[-1] <= df_w['boll_bot20'].iloc[-1] * BUY_BOLL_TOLERANCE and df_w['rsi14'].iloc[-1] <= BUY_RSI_MIN):  # 引用第2-1章
                return None
        else:
            print(f"🧪 {ticker} 正在進行【驗證篩選】測試中...")

        # 3. 買入判斷 - 第二層：日K趨勢 (eLeader 25 條件)
        df_d = get_stock_data(ticker, period='6mo', interval='1d', cache=daily_cache)
        if df_d is None or len(df_d) < 50: return None
        df_d = calc_indicators(df_d)
        if df_d is None: return None
        
        # 🎯 [優化 1] 明確型態判斷，避免 Tuple/None 混用風險
        is_eleader_ok = check_buy_eleader(df_d) is not None
 
        # 4. 買入判斷 - 第三層：5分K即時轉折
        now_ts = time.time()
        cache_key = f"5m_{ticker}"
        
        # 檢查快取是否在 5 分鐘(300秒)內
        if cache_key in five_min_cache and (now_ts - five_min_cache[cache_key]['ts'] < 300):
            df_5m = five_min_cache[cache_key]['df']
        else:
            df_5m = yf.download(ticker, period='2d', interval='5m', progress=False)
            if df_5m is not None and not df_5m.empty and len(df_5m) >= 10:
                # 🎯 [優化 2] 先算好 RSI 與 MACD柱 存入快取（成交量不列入條件，截圖說明是期貨口數）
                df_5m['rsi14'] = ta.rsi(df_5m['Close'].squeeze(), length=14)
                _macd_5m = ta.macd(df_5m['Close'].squeeze(), fast=12, slow=26, signal=9)
                df_5m['macd_hist'] = _macd_5m.iloc[:, 1]
                five_min_cache[cache_key] = {'df': df_5m, 'ts': now_ts}
 
        # 確保資料長度足夠取前一根(-2)與指標皆存在
        if df_5m is None or len(df_5m) < 2 or 'rsi14' not in df_5m.columns: return None
 
        # 最終邏輯判斷
        last_rsi  = float(df_5m['rsi14'].iloc[-1])
        prev_rsi  = float(df_5m['rsi14'].iloc[-2])
        # 5分K MACD柱（有值才比較，避免NaN錯誤）
        try:
            last_macd = float(df_5m['macd_hist'].iloc[-1])
            prev_macd = float(df_5m['macd_hist'].iloc[-2])
            macd_5m_ok = (last_macd > prev_macd)
        except:
            macd_5m_ok = False

        # 🎯 最終買入守門員（OR 邏輯，兩條路任一成立即可進場）：
        # 路線A：eLeader 25條件成立 AND 5分K RSI向上轉折（原有邏輯）
        # 路線B：截圖3/4買進條件 = 5分K RSI向上 AND 5分K MACD柱向上（不看成交量）
        route_A = is_eleader_ok and (last_rsi > prev_rsi and last_rsi > 35)
        route_B = (last_rsi > prev_rsi and last_rsi > 35) and macd_5m_ok

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
            send_gmail(f"🔔 基金買進訊號：{fund_name}", msg_body)
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
    print(f"  股票週K監控系統啟動")
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
    if 'TW' not in active_markets:
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
    if 'US' not in active_markets:
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
    if 'CRYPTO' not in active_markets:
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
        print(f'\n📊 期貨5分K掃描：{FUTURES_5MK_TARGETS}')
        for ticker in FUTURES_5MK_TARGETS:
            try:
                df5 = yf.download(ticker, period='2d', interval='5m', progress=False)
                if df5 is None or df5.empty or len(df5) < 20:
                    print(f'  ⚠️ {ticker} 5分K資料不足，跳過')
                    continue
                df5 = calc_indicators(df5)
                if df5 is None: continue
                rsi_now  = float(df5['rsi14'].iloc[-1])
                rsi_prev = float(df5['rsi14'].iloc[-2])
                mh_now   = float(df5['macd_hist'].iloc[-1])
                mh_prev  = float(df5['macd_hist'].iloc[-2])
                close    = float(df5['Close'].iloc[-1])
                boll_bot = float(df5['boll_bot20'].iloc[-1])
                boll_top = float(df5['boll_top20'].iloc[-1])
                now_str_f = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M')
                if rsi_now > rsi_prev and mh_now > mh_prev and close <= boll_bot * 1.02:
                    send_gmail(f"⭐【期貨5分K買進】{ticker} - {now_str_f}",
                        f"⭐【期貨5分K買進訊號】⭐\n標的：{ticker}\n收盤：{close:.2f}　布林下緣：{boll_bot:.2f}\nRSI：{rsi_prev:.1f}→{rsi_now:.1f}（↑）\n時間：{now_str_f}")
                    print(f"  ✅ {ticker} 買進訊號已發送")
                elif rsi_now < rsi_prev and mh_now < mh_prev and close >= boll_top:
                    send_gmail(f"🔔【期貨5分K平倉】{ticker} - {now_str_f}",
                        f"🔔【期貨5分K平倉訊號】🔔\n標的：{ticker}\n收盤：{close:.2f}　布林上緣：{boll_top:.2f}\nRSI：{rsi_prev:.1f}→{rsi_now:.1f}（↓）\n時間：{now_str_f}")
                    print(f"  ✅ {ticker} 平倉訊號已發送")
                else:
                    print(f"  ℹ️ {ticker} RSI={rsi_now:.1f} 未達條件，不發信")
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

            subject = f"❌❌【下市警報】{len(hold_list)}支持有須出清 / {len(watch_list)}支觀察勿碰 - {now_str}"
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

            send_gmail(f"⭐【買進訊號】{len(filtered)}支 - {now_str}", body)
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

            send_gmail(f"🔔【賣出訊號】{len(filtered)}支 - {now_str}", body)
            save_notified(notified)
        else:
            print(f"🔕 賣出訊號 {len(sell_signals)-len(filtered)} 支已通知過")

# =====================
# 無訊號
# =====================
    if not buy_signals and not sell_signals and not delist_signals:
        send_gmail(
            f"📊 週K掃描完成 - {now_str}",
            f"掃描時間：{now_str}\n\n本週無符合條件的股票"
        )

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
        in_futures = ((wd_f==0 and tv_f>=15*60+1) or (wd_f==1) or (wd_f==2 and tv_f<=13*60+30))
        if not in_futures:
            print(f"[{test_now}] ❌ 非期貨5分K時段，直接結束")
            time.sleep(5); exit()
        print(f"🚀 期貨5分K模式啟動　標的：{FUTURES_5MK_TARGETS}　間隔：{FUTURES_5MK_INTERVAL}秒")
        while True:
            now_l = datetime.now(pytz.timezone('Asia/Taipei'))
            wd_l  = now_l.weekday(); tv_l = now_l.hour*60+now_l.minute
            if not((wd_l==0 and tv_l>=15*60+1) or (wd_l==1) or (wd_l==2 and tv_l<=13*60+30)):
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
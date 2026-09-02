# ══════════════════════════════════════════════════════════════
# ★★★【自我舉證表】(09021330)　依鐵律ＡＭ２５②：每條關鍵結論須指回來源
# ══════════════════════════════════════════════════════════════
# ★★★【09022155 新增結論．★依主帥指令補讀交接文件後發現】
# 結論⑲：條件W 兩個方向的 K 棒根數一律 N＝5
#   來源＝交接文件(09020802) 第六章②（★★本輪依主帥 09/02 21:20 指令補讀全文）
#   → ★★★程式原本用 BUY_LOOKBACK_5MK＝54，★09021330 又推出 15分K＝18，★兩者皆錯
# 結論⑳：條件W 的 buy call 與 buy put【共用同一組配額】
#   來源＝交接文件(09020802) 第六章⑧
#   → ★★★09021330 我寫成多空各自獨立，★日盤會變 6 封（定稿為 3 封）
# 結論㉑：條件W【不需要出場通知】
#   來源＝交接文件第六章⑨，★主帥原話「買進後放到結算，就像買彩券，以小博大」
#   → ★本輪確認：★★scan_condition_w() 未加任何出場邏輯，★★★符合定稿
# 結論㉒：未平倉提醒信不需要行情資料
#   來源＝該功能只讀「時間」與「持倉狀態」兩個變數（本檔 _futures_close_alert）
#   → ★★★故不受 ^TWII 只到 13:30 的限制，★可涵蓋到 13:45
# 結論㉓：Firestore 規則已於 2026/09/01 20:39 重新發布
#   來源＝截圖 Firestore規則_09012039.jpg 的版本歷史（最上方「昨天 8:39 下午」）
#   ★★【但必須誠實說明】：★截圖可見範圍僅到第 32 行（isAuthed/safeId 段），
#     ★★★【未涵蓋 public 段】→ ★我無法逐字驗證 read 是否已改回 true，
#     ★只能確認「有一次新發布，時間晚於修正版產出時間 09/01 16:21」
#
# ★★★【09022055 新增結論】
# 結論⑭：_futures_is_holding 原本只是模組層級變數（第225行），不跨執行保存
#   來源＝本檔第225行（實讀）＋ 全檔 grep 無任何 futures_pos 讀寫函式
#   → ★★★「有持倉就繼續掃平倉」在雲端版【從來沒有生效過】
# 結論⑮：出場不受進場時窗限制
#   來源＝主帥 2026/09/02 15:02 原話（對話視窗，已立為否決清單 R-14）
# 結論⑯：期貨嚴禁拿掉關卡1、關卡2
#   來源＝主帥 2026/09/02 14:55 原話「這是期貨，不是週選擇權，可以買進後放著
#         不管等歸零！！！期貨放著等歸零，不是大賺就是破產！！！」
# 結論⑰：做空被做多的第二道 continue 綁架
#   來源＝本檔原第4907行 `if check_buy_eleader(df5_d) is None: continue`（實讀）
# 結論⑱：^TWII 只有 09:00~13:30 有報價
#   來源＝既有 _tw_spot_session_ok 決策註記（F-15）＋ 08160731 定案
#   → ★13:30~13:45 與夜盤【做不到】，★★需待辦B，★★★不假裝有解
#
# ★★★【09022055 推定清單追加】
#   推定5：Firestore public/futures_position 路徑可未登入讀取
#     ・依據＝W-2 規則 public 段為 `allow read: if true`（09/01 主帥已部署）
#     ・★推定為假的後果：每次執行都讀不到 → 回到「從空手開始」；
#       ★★log 會印「無紀錄或無法讀取」，★★★主帥一眼可驗
#   推定6：寫入用的 service account 對該路徑有寫權限
#     ・依據＝同一路徑家族的 futures_status 已由同一憑證寫入多時
#     ・★推定為假的後果：log 印「持倉狀態寫入失敗」；★★不影響進場訊號
#
# ★★★【09021415 重大更正】09021330 版的自我舉證表結論①③有誤，更正如下：
#   ・原結論①把「期貨5mk 閘門」與「條件W 時窗」混為一談 → 錯誤呈現。
#     ★事實：條件W 週三/週五【都是 11:00】，★★對稱，★★★沒有不一致。
#     ★不對稱的是期貨 in_futures（週三11:30／週五11:00），★成因是
#       「週三11:30」為期貨模式 2026/03 的原始窗尾，「週五11:00」為 08/09 跟改。
#   ・原結論③說「兩條路徑全部停擺」→ ★條件W 在11:00後停擺【是設計，不是故障】。
#   ・★★★09021330 版把條件W 窗尾擅自改為 13:30 → 主帥未授權，本版已回退為 11:30。
# 結論⑩：條件W 窗尾 11:30、期貨完全比照條件W、週一不再觸發
#   來源＝主帥 2026/09/02 14:00 訊息第2點原話（對話視窗，已寫入 R-12）
# 結論⑪：11:00 是主帥 2026/08/09 親自指定（非 AI 自行決定）
#   來源＝0_凍結開關與否決清單 R-12 條，內含主帥原話
#         「主力手法多樣，有時候會故意拖到 11:00 才觸發行情發動」
# 結論⑫：09:05 起始（不可改09:00）是主帥當初指定
#   來源＝08060105 改版記錄第三章第1點（實讀）
# 結論⑬：check_tw_intraday_extreme(F-12) 原寄生在期貨5mk 分支內
#   來源＝本檔第5085行（實讀），故必須另開獨立時窗，否則隨期貨縮窗一起消失
# 結論①：5mk 閘門週三只到11:30、週四無日盤、週五只到11:00
#   來源＝本檔前一版 3_stock_monitor_GitHub(08301905).py 第5366-5372行（實讀）
# 結論②：cron 早已補上週三12:00-13:59／週四09:05-13:59／週五11:00-13:59
#   來源＝stock_scan(08301905).yml 第65-67行（實讀）
# 結論③：兩者不一致 → 2026/09/02(週三)11:31~13:30 兩條路徑全部停擺
#   來源＝本輪實跑時間窗對照表（8個時刻），輸出見 改版記錄(09021330) 第四章
# 結論④：條件W 原本只做 buy call、只有5分K、無第一二道
#   來源＝前一版第285-287行註解 ＋ scan_condition_w() 函式本體（實讀）
# 結論⑤：R-08「禁止條件W 加 buy put」已於2026/08/19由主帥正式撤銷
#   來源＝0_凍結開關與否決清單(08231006).txt 第313-317行（實讀，原文
#         「已於 2026/08/19 由主帥正式撤銷，不再有效」「不得再據以拒絕 buy put」）
# 結論⑥：15分K 回看根數＝18（＝5分K 54根÷3）
#   來源＝K棒等比換算原則 ＋ 前一版 scan_futures_15mk() 第4386行已採同一算法
# 結論⑦：get_weekly_option_hint 已支援 'sell'→PUT 且已做權利金≤16篩選
#   來源＝前一版第2763-2795行（實讀，_pick_strikes_by_premium 已存在）
# 結論⑧：主帥要求「5分K與15分K是OR」「不要關卡1關卡2」「直接關卡三」
#   來源＝主帥 2026/09/02 12:05 訊息第3點原話（對話視窗）
# 結論⑨：主帥要求測試須與行情脫鉤
#   來源＝主帥 2026/09/02 12:05 訊息第2點原話（對話視窗）
#
# ★★★【推定清單】★以下【未經實測驗證】，★若推定為假的後果如下：
#   推定1：yfinance 對 '^TWII' 的 interval='15m' 能回傳足夠資料（≥20根）
#     ・依據＝前一版 scan_futures_15mk() 已用同一寫法且在線上運作
#     ・★推定為假的後果：15分K 分支印「資料不足」並跳過，
#       ★★5分K 分支【不受影響】仍正常運作（已用 try/except 隔離）→ 損害有限
#   推定2：多空同時成立時以【15分K 優先】
#     ・★主帥【未明示】此優先序，★★由我自行決定（週期長、雜訊少）
#     ・★★★推定為假的後果：訊號來源標示與主帥預期不同；
#       ★不影響是否發信，★只影響信中「觸發週期」那一行 → 損害輕微
#   推定3：條件W 不需要「週二／週四日盤窗」
#     ・★我【未擅自新增】該兩窗，★★已列入待辦請主帥裁示
#     ・★★★推定為假的後果：週二/週四白天條件W 仍靜音；
#       ★但期貨5mk 路徑本輪已補齊週二~週五日盤，★★有備援 → 損害有限
#   推定4：放寬閘門後 GitHub Actions 用量不會超標
#     ・依據＝cron 班次完全未改，只是原本被擋掉的班次現在會跑完
#     ・★推定為假的後果：Actions 分鐘數上升；★★可由主帥觀察帳單後回報
# ══════════════════════════════════════════════════════════════

SCRIPT_VERSION = '09022155'   # ✅ 鐵律V2：全檔唯一版本識別處，須＝檔名時間戳（本行自07040032起連續4次交付漏改，08031637 由交付前自檢腳本揪出並根治）
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
USE_CROSS_RUN_CACHE = True   # ✅ 方案②(07061319) 月K/週K跨輪(跨程序)快取，大幅降掃描耗時；出錯自動回退、設False可一鍵關閉   # ★凍結開關 F-02（鐵律AD，改動前必讀凍結清單）
KLINE_CACHE_FILE = '6_kline_cache.pkl'   # 跨輪快取檔（程式自動產生，保留勿刪）
five_min_cache = {}  # ✅ 新增 5 分鐘 K 線快取容器
# ╔══════════════════════════════════════════════════════════════════════╗
# ║ ★★★ 凍結開關區（鐵律AD）★★★  改動前必讀                          ║
# ║ 以下常數皆登錄於【0_凍結開關與否決清單(MMDDHHMM).txt】，              ║
# ║ 每一個都有完整決策歷史與「當初為何是這個值」的血淚原因。              ║
# ║ ★AI 注意：你「掃描程式碼後覺得這裡是缺陷」不構成修改理由。            ║
# ║   很多看起來像缺陷的地方，是主帥或前輪AI付出代價後刻意留下的。        ║
# ║   要變更，必須同時提出：①新證據 ②與上次失敗做法的具體差異            ║
# ║   ③一鍵還原機制，並經主帥同意。三者缺一 → 禁止提出。                 ║
# ║ ★交付前自檢腳本 9_交付前自檢.py 第(6)項會自動比對這些值，不符即 FAIL。║
# ╚══════════════════════════════════════════════════════════════════════╝
# ✅ (點1) 06221854：個股「最後一根K棒收盤價」即時補更總開關＋現價快取
# ┌─ 決策註記：REALTIME_LAST_BAR（清單條目 F-01）────────────────────────
# │ 06/24 關閉(False)：逐支呼叫即時價，API 呼叫量＝標的數（250支=250次），
# │        負荷過高、雲端超時。副作用：第一關實際用收盤價而非盤中即時價。
# │ 08/06 恢復(True)：改【批次預抓】prefetch_realtime_prices()，一次100支，
# │        250支→API僅3次，負荷降98.8%；四市場全接上；設False可一鍵還原。
# │ 08/07 GitHub Actions 連續失敗曾誤疑本開關，經 log 證實為
# │        【runner 未分配】之基礎設施故障（見清單 R-07）→ 本開關無罪。
# │ ★變更本開關的唯一許可條件：雲端 log 中【有實際執行步驟輸出】
# │   且卡在預抓相關步驟。若 Annotations 為 "not acquired by Runner"
# │   或 "Internal server error. Correlation ID" ⇒ 屬 GitHub 故障，
# │   嚴禁動此開關，也嚴禁改任何程式碼。
# │ ★若確需降載，順序為：RT_PREFETCH_CHUNK 100→50 → 加獨立timeout
# │   → 只對通過第一關者預抓 → 皆無效才考慮 False。不得直接關閉。
# └──────────────────────────────────────────────────────────────────────
REALTIME_LAST_BAR = True   # True=月K/週K/日K最後一根用即時５m現價覆蓋（盤中即時）；API負載過重可設False停用
RT_PREFETCH_CHUNK    = 100   # ✅08061155 即時價批次預抓每批支數（1827支約19批，取代原逐支抓取）
LIMIT_UP_MAX_SCAN    = 20    # ✅08061155 漲停追蹤每輪最多掃描支數（主帥指定先設20支觀察負荷）   # ★凍結開關 F-03（鐵律AD，改動前必讀凍結清單）
# ✅08061155 漲停追蹤優先序：以台灣大型權值股（0050成分股近似清單）為主，其餘依原順序遞補。
#   ★此為近似清單、非官方即時成分股（0050每季調整），僅作「優先排序」用途，
#     排錯不會漏掃、只影響先後順序；主帥可自行增刪。若20支不夠用，
#     可調高 LIMIT_UP_MAX_SCAN，或在本清單後方續增前100/150大市值與熱門股代碼。
TW_LARGE_CAP_PRIORITY = [
    '2330','2317','2454','2308','2382','2881','2882','2891','2412','2303',
    '3711','1216','2886','2884','2885','2892','5880','2357','3231','2345',
    '3034','3008','2379','3037','6669','3661','2207','1301','1303','1326',
    '6505','2002','2603','2609','2615','2912','3045','4904','4938','5871',
    '2883','2887','2890','2880','1101','1102','2105','9910','6415','2409',
]
_rt_price_cache = {}        # 每ticker每輪只抿一次即時現價，避免重複API
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
# ✅08091258【🔴B(a) 查證後的真正問題】期貨K棒「陳舊資料」防護
# ┌─ 決策註記：為何需要這道防護（清單條目 F-07）──────────────────────────
# │ 現況：期貨5分K與條件W 的第三道關卡，資料來源是 yf.download('^TWII', '5m')。
# │ ★但 ^TWII 是【加權指數】，只在 09:00~13:30 交易，【夜盤完全沒有資料】。
# │ 於是夜盤（15:00~次日05:00）執行掃描時，yfinance 會回傳當日日盤的舊K棒，
# │ 程式仍照常拿 iloc[-1] / iloc[-2] 判斷「V轉」——判的是【白天收盤前那兩根】。
# │ 例：週二 22:15 掃描，實際比對的是週二 13:20 與 13:25 兩根，已陳舊 9 小時。
# │ 而且每 5 分鐘掃一次都會得到相同結果 → 同一組陳舊K棒被反覆判定成立，
# │ 這正是當初需要 CONDW_MAX_PER_WINDOW 配額壓制的深層原因。
# │ ★本防護：K棒過舊即【不進場】，寧可漏訊號也不發假訊號（狼來了原則）。
# │ ★取不到K棒時間時【不阻擋】（回傳 None），避免防護本身造成漏訊號。
# └──────────────────────────────────────────────────────────────────────
FUT_BAR_MAX_AGE_MIN  = 15    # 最後一根5分K超過此分鐘數即視為陳舊，不進場
# ✅08091843【主帥 08/09 14:37 定案】買/賣訊號彙整信的通知額度【日盤與夜盤各自獨立】
# ┌─ 決策註記：SIGNAL_MAX_PER_SESSION（清單條目 F-10）────────────────────
# │ 主帥原話：「改成每日的日盤2次，每日的夜盤2次。我一收到通知信，就迅速在
# │ 3分鐘內手機登入app下單進場（也可能用筆電），避免錯過大賺行情。」
# │ ★問題：原本額度以「整天」計，同一支標的一天只有2次。
# │   台股22:35 也有全量掃描（證交所20:00後更新資料），虛擬幣/外匯更是24小時，
# │   夜盤若先用掉2次，隔天日盤就被靜音 → 與 8/5 漏發900點完全同型的風險。
# │ ★時段界線刻意比條件W 寬（條件W 是 09:05~13:30）：
# │   全量掃描一輪可達45分鐘，若用送信當下時間判定，12:50 那輪常在13:35才送出，
# │   會被誤判成夜盤。故日盤採 08:00~15:00，涵蓋掃描耗時。
# └──────────────────────────────────────────────────────────────────────
# ✅08092144【主帥 08/09 21:44 修正前令】原訂日盤2次/夜盤2次，改為與條件W 一致：
#   主帥原話：「既然條件W是日盤3次/夜盤2次，那 gmail 通知信通知我進場
#   理應也要是日盤3次/夜盤2次才合理。」
#   日盤多一次的理由（沿用08060130）：日盤是主帥能實際下單的時段，值得多一次；
#   夜盤維持2次避免深夜打擾。
SIGNAL_MAX_DAY       = 3   # 買/賣訊號：同標的同方向，日盤最多通知次數
SIGNAL_MAX_NIGHT     = 2   # 買/賣訊號：同標的同方向，夜盤最多通知次數
SIGNAL_DAY_START_MIN   = 8*60    # 日盤起 08:00（台灣時間）
SIGNAL_DAY_END_MIN     = 15*60   # 日盤迄 15:00（台灣時間）

# ✅08091843【🔴B(a) 第二段·階段一】台指期5分K【快照累積器】
# ┌─ 決策註記：TXF_BAR_ENABLED（清單條目 F-11）──────────────────────────
# │ 背景：期交所 MIS 只提供【即時快照】，不提供5分K歷史；Yahoo 無台指期分鐘K。
# │ 主帥 08/09 定案採【乙案】：由程式自行累積 MIS 快照，建立自己的K棒序列。
# │ ★階段一（本輪）：只累積、只回報，【不接入任何進場判斷】。
# │   理由：資料要累積數小時才夠算布林/RSI/MACD；在資料品質未經驗證前
# │   就接進判斷，等於用未驗證資料下單，違反鐵律R-05。
# │ ★階段二（後續）：資料足量且經主帥確認後，才評估是否接入第三道關卡。
# │ ★已知限制（必須誠實記錄，不得在階段二忘記）：
# │   快照式K棒只有「取樣當下的價」，沒有棒內真實的最高/最低價。
# │   深夜 cron 為每15分鐘一次，會有缺口。這兩點決定它【不能等同真實5分K】。
# └──────────────────────────────────────────────────────────────────────
TXF_BAR_ENABLED      = True  # 台指期5分K快照累積器總開關
# ┌─ 決策註記：TXF_LOOP_SAMPLING（08250451 新增）────────────────────────────
# │ 主帥 08/24 已確認 kj-manager 為 public repo → GitHub Actions 分鐘數無限，
# │ 故「單次 job 內迴圈取樣」在成本上已無障礙。
# │ ★但預設仍為 False，原因是【工作流重疊】尚未查證：
# │   futures-scan 每 5 分鐘啟動一次，若單次執行拉長到 4.5 分鐘，
# │   ★前後兩次可能重疊，兩個行程同時寫同一份 Firestore 文件 → 互相覆蓋。
# │ ★開啟前必須先在 yml 加 concurrency 設定（cancel-in-progress: false）。
# │ ★★這一項列為待辦，主帥確認後改 True 即可，程式碼已備妥。
# └──────────────────────────────────────────────────────────────────
TXF_LOOP_SAMPLING    = False # ★迴圈取樣總開關（見上方決策註記）
TXF_LOOP_SECONDS     = 270   # 單次 job 內迴圈取樣總時長（秒）
TXF_LOOP_INTERVAL    = 20    # 迴圈取樣間隔（秒）
TXF_BAR_KEEP         = 120   # Firestore 只保留最近N根，避免文件無限膨脹

# ✅08092108【🆕E】台股白天極端異動：由「收盤後才知道」改為【盤中即時偵測】
# ┌─ 決策註記：TW_INTRADAY_EXTREME_ENABLED（清單條目 F-12）───────────────
# │ 原設計：check_tw_daytime_extreme() 取 ^TWII【日K】，
# │   而日K要收盤後才定形 → 主帥【收盤後才知道今天大漲/大跌750點】，
# │   等於錯過整段可以進場的行情。這與 8/5 漏發 buy call 是同型損失。
# │ 本次新增：盤中每5分鐘用【當下現價 vs 昨收】判斷，達門檻立即通知。
# │ ★資料來源優先序（遵守鐵律AF3：不得混用不同標的的價格序列）：
# │   ①期交所 MIS【臺指現貨】＝加權指數即時值（首選，與昨收同為加權指數，可直接相減）
# │   ②^TWII 5分K 最後一根收盤（備援，需通過 _bar_too_old 陳舊檢查）
# │   ★嚴禁改用【臺指期】現價與加權指數昨收相減——兩者相差百餘點，會虛增漲跌幅。
# │ ★執行時段限台灣 09:00~13:35（台股交易時段），非此時段直接跳過。
# └──────────────────────────────────────────────────────────────────────
TW_INTRADAY_EXTREME_ENABLED = True   # 盤中即時極端異動偵測總開關
TW_EXTREME_PTS              = 750    # 門檻點數（沿用原值，與夜盤同幅度）
TW_CLOSE_CONFIRM_ENABLED    = False  # ✅08092144 收盤後的「收盤確認」信：主帥指示關閉

# ✅08100036【⚪F′】讀取【網頁版待買觀察清單】，命中時標記＋補掃描範圍外標的
# ┌─ 決策註記：WATCHLIST_ENABLED（清單條目 F-14）────────────────────────
# │ 查證結果（08/09）：程式全文搜尋 watchlist／待買觀察／觀察清單 → 0 次命中，
# │   代表主帥在網頁版親手加入的觀察清單，雲端版【完全不知道它存在】。
# │ 影響評估：清單內的台股/美股/虛擬幣/外匯多半已被全市場掃描涵蓋，
# │   但信裡不會標明「這支是你自己加入觀察的」；真正會漏的是【掃描範圍外】的標的
# │   （例：美股只掃道瓊30＋DRIP，主帥若觀察 NVDA 以外的中小型股就掃不到）。
# │ 本次做兩件事：①命中時在信中標記 ⭐【你的觀察清單】 ②補掃範圍外標的。
# │ ★資料來源：網頁版寫入的 Firestore 路徑
# │   artifacts/{專案}/users/{email小寫並把@和.換成_}/data/stocks
# │   欄位 watchlist 為陣列，元素格式 {code, cat}；
# │   cat ∈ tw／us／fund／crypto／fx／gold／bond（與網頁版下拉選單一致）。
# │ ★只讀不寫：本程式【絕不】修改觀察清單，避免與網頁版互相覆蓋。
# └──────────────────────────────────────────────────────────────────────
WATCHLIST_ENABLED    = True   # 觀察清單標記與補掃總開關
WATCHLIST_OWNER      = 'shchyu61@gmail.com'   # 讀取誰的觀察清單（主帥本人）
WATCHLIST_EXTRA_MAX  = 30     # 補掃範圍外標的的上限（防止清單過長拖垮掃描）
FUTURES_5MK_OWNER    = 'shchyu61@gmail.com'  # 5分K模式專屬帳號
_futures_is_holding = False   # ✅ 07031936 正式宣告為模組全域(取代原dir()守門)
_holdings_sent = False        # ✅ (07130626) 持股每日健檢：本次執行是否已寄出(防迴圈重複寄)
_futures_is_short   = False   # ✅ 07031936 同上

# FinMind設定（財務篩選 + 集保大戶 + 法人大買）
FINMIND_TOKEN      = __import__('os').environ.get('FINMIND_TOKEN', '')  # 可選Token
FINMIND_MIN_PASS   = 4       # 通過財務篩選最低支數（低於此數不啟用）
FINMIND_CACHE_HOURS = 168    # 財務資料快取7天（週報不常更新）
# ── 週選擇權履約價推薦設定 ✅(08032126) ──
OPT_PREMIUM_MAX      = 16    # ★主帥定案：以小博大，進場權利金上限(元)；>16 不推薦(歸零損失大)
OPT_SNAPSHOT_TIMEOUT = 6     # 選擇權快照單次逾時秒數（失敗即放棄、不重抓）
OPT_CHAIN_CACHE_SEC  = 60    # 同一分鐘內重用快照，避免重複拉取（單次回應可達2MB）
# ✅08090829【🔴B(b) 解鎖】選擇權權利金來源改用【期交所行情資訊網 MIS】官方免費即時源。
#   ┌─ 決策註記：為何換掉 FinMind（清單條目 R-01）────────────────────────
#   │ FinMind taiwan_options_snapshot 免費層回 HTTP 400「Your level is register」
#   │ ＝需付費贊助會員 → 主帥 08/06 定案【放棄該來源】，並禁止再提付費方案。
#   │ 2026/08/09 主帥實跑探測腳本證實：期交所 MIS 端點 HTTP 200，
#   │ 一次取得 3487 檔契約，其中權利金 ≤16 元者 264 檔 → 付費牆完全繞開。
#   │ ★本來源為期交所官網自己在用的 API：免費、免註冊、免 token。
#   └────────────────────────────────────────────────────────────────────
OPT_MIS_URL          = 'https://mis.taifex.com.tw/futures/api/getQuoteList'
OPT_MIS_REFERER      = 'https://mis.taifex.com.tw/futures/'
OPT_MIS_TIMEOUT      = 12    # MIS 單次回應可達 2MB，逾時需比一般請求寬鬆
_opt_chain_cache     = {'ts': 0, 'rows': None}   # 模組頂層宣告，防 'not defined'
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
TEST_MODE = _os.environ.get('TEST_MODE', TEST_MODE)  # ✅ 07030929 修正：讀環境變數（雲端yml的 TEST_MODE:'5mk'/'condW' 才會生效；未設則沿用上方預設）
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
HOLDINGS_FX = ['EURUSD=X']     # ✅ 07010514 做空回補：EUR/USD做空持倬（對應網頁版預建）
HOLDINGS_SHORT = ['EURUSD=X']  # ✅ 做空持股清單：在此標記為空單→走回補檢查而非賣出
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
# ┌─ 決策註記：條件W 為何「只做 buy call」（清單條目 R-08）──────────────
# │ ★這不是 AI 擅自簡化，是主帥親自定案兩次，且已有 AI 重提被駁回的前例。
# │ 06/30 主帥定案：條件W ＝【週選擇權做多專用】，只做 buy call，
# │        「不需做空 → 省一半邏輯」。
# │ 07/03 AI 曾把「條件W 沒有做空」講成缺口，被主帥當場糾正；
# │        主帥一度說「也要 buy put」，隨即自行更正回「不必動條件W」，
# │        理由是【做空由期貨5分K空方負責】，兩者分工、不重疊。
# │ 07/03 同輪定案【乙案】：把含空方的期貨5mk 時窗「加上」條件W 的夜盤時窗
# │        （週二15:05~週三11:30／週四15:05~週五11:30），見 get_active_markets()。
# │ 08/06 再補齊：週二~週五 09:05~13:30 日盤也納入期貨掃描時段。
# │
# │ ★★分工表（任何 AI 動手前必讀，避免再次搞混）★★
# │   條件W        → 只抓台指【下軌V轉】→ 建議 buy CALL（做多）
# │   期貨5分K/15分K → 多空雙向；【上軌Λ轉】→ 建議 buy PUT（做空）
# │   ＝「多空雙向」是【整個週選擇權通知體系】的性質，
# │     不是【條件W 這一個函式】的性質。條件W 只負責多方那一半。
# │ ★★★【09021330 更新】上述「禁止」已於 2026/08/19 由主帥【正式撤銷】
# │   （見 0_凍結開關與否決清單 R-08：「不得再據以拒絕 buy put」）。
# │   ★本輪 W-4 已在條件W 內實作多空雙向：V轉→buy CALL、Λ轉→buy PUT，
# │   ★★並改為【5分K OR 15分K】（主帥 09/02 三度確認的規格）。
# │   ★以下原文保留供追溯，★★★但【已失效，不得再據以拒絕做空】：
# │ ★禁止：在條件W 內加入 buy put／做空鏡像。要補做空，
# │   正確做法是檢查 get_active_markets() 的期貨時窗有無涵蓋該時段。
# └──────────────────────────────────────────────────────────────────────
# ── 【條件W：週選擇權做多專屬設定】（TEST_MODE = 'condW' 時才啟用）✅ 07011049 純新增 ──
# 執行時段：週二15:05~週三11:30、週四15:05~週五11:30（跨夜）
#   ★✅09021415 窗尾由11:00延伸至11:30（主帥 09/02 14:00 明示）。
#     ★★09021330 曾被 AI 擅自改成 13:30，★★★主帥未授權，已回退。
# 標的：台指（^TWII）；✅09021330 改為多空雙向（buy CALL／buy PUT）；
#   ★跳過第一二道、只跑第三道；★★週期為【5分K OR 15分K】（非 AND）
# 防過頻：同一窗、同方向最多通知2次（主力煙霧彈/真發動順序會互換，故非1次）
CONDW_TARGET         = '^TWII'
CONDW_MAX_PER_WINDOW = 2       # 同一窗、同方向最多通知次數（夜盤用）
CONDW_MAX_DAY        = 3       # ✅08060130 日盤(09:05~13:30)獨立配額3次：日盤是主帥能實際下單、
                               #   且週選結算日決勝的時段，值得多一次；夜盤維持2次避免深夜吵醒。
CONDW_OWNER          = 'shchyu61@gmail.com'
# ── ✅09021330【W-4】條件W 升級：★5分K OR 15分K、★★多空雙向 ──────────────
#   ★主帥 2026/09/02 三度確認的規格（原話）：
#     「條件W，【5分K】【15分K】應該是『or』才對，不應該是『and』！
#       而且，應該也不能有關卡1和關卡2阻擋吧！應該直接以關卡三判斷就好」
#   ★★依據：否決清單 R-08 已於 2026/08/19 由主帥【正式撤銷】，
#     ★★★撤銷後明文「不得再據以拒絕 buy put」→ 本次實作不牴觸凍結清單。
CONDW_ENABLE_15MK    = True   # 條件W 是否併用15分K（與5分K 為 OR，非 AND）
CONDW_ENABLE_SHORT   = True   # 條件W 是否做空方（Λ轉→建議 buy PUT）
# ── ✅09022155【★★★依交接文件第六章「條件W 定稿規格」回正】────────────
#   ★交接文件(09020802) 第六章②白紙黑字：
#     「★★兩個方向的 K 棒根數一律 ★N ＝ 5（★不是事不過三的 3）」
#   ★★但程式一直借用 BUY_LOOKBACK_5MK = 54（那是【期貨5分K】的根數），
#     ★★★而 09021330 我加 15分K 時又依 54÷3 推出 18 —— ★兩個都不是 5。
#   ★這不是我這幾天新造的錯，★★是既有偏差而我【沿用且擴大】了它。
#   ★★★一行回滾：★若主帥要回到 54/18，把 CONDW_LOOKBACK 改回 BUY_LOOKBACK_5MK 即可。
CONDW_LOOKBACK       = 5      # ★條件W 專用回看根數（定稿規格②：N＝5，多空共用）
CONDW_LOOKBACK_15MK  = 5      # ✅09022155 由18改為5（定稿規格②：★兩週期一律N＝5）
# ── ✅09021330【測試開關】主帥 2026/09/02 指示：★要能不管行情有沒有觸發都測得到 ──
#   ★主帥原話：「既然要測試，你應該要讓測試不論行情有沒有觸發，
#     要放寬某個條件成極度寬鬆，這樣才好測試吧？」
#   ★★用途：驗證【Firestore 讀取權限 ＋ Gmail 寄信管線】是否活著，★與行情無關。
#   ★★★安全設計：①預設 False ②信件主旨強制冠上【測試】
#     ③不寫入 Firebase 認領槽（不佔用真實訊號額度）④已納入凍結清單 F-16。
CONDW_FORCE_TEST     = False  # ★True＝強制寄一封測試信後結束；★平時必須為 False
# ── ✅09022155【期貨未平倉·收盤前提醒信】主帥 2026/09/02 21:20 指令 ──────
#   ★主帥原話：「我期貨進場，極少放任到收盤，幾乎9成次數會在13:20前平倉！
#     若沒有平倉，則由AI和雲端版協助寄發gmail通知信給我，提醒我期貨還有口數
#     未平倉，務必在收盤前平倉的通知信。」
#   ★★★關鍵設計：★本提醒信【不需要任何行情資料】，★★只需要「時間」與「持倉狀態」。
#     ★所以它【不受 ^TWII 只到 13:30 的限制】，★★可一路涵蓋到台指期日盤收盤 13:45。
#   ★每日最多一封（Firebase 認領槽防重複）。
FUTURES_CLOSE_ALERT_ENABLED = True
FUTURES_CLOSE_ALERT_START   = 13*60+20   # 13:20 起（★主帥自陳9成在此前已平倉）
FUTURES_CLOSE_ALERT_END     = 13*60+44   # 13:44 止（★台指期日盤 13:45 收盤）
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
SELL_BOLL_TOLERANCE  = 0.98   # ✅08060130 鏡像對稱修正：原1.00=必須真的碰到上軌(零容忍)，
                              #   而多方 BUY_BOLL_TOLERANCE=1.02 允許下軌上方2%內 → 多空門檻嚴重不對稱，
                              #   空方訊號極難觸發，違反本專案「做空＝多方策略鏡像」原則。
                              #   改為0.98(上軌下方2%內即算近上軌)，與1.02完全對稱。若嫌訊號過多可改回1.00。
COVER_BOLL_TOLERANCE = 1.02   # ✅08060130 鏡像對稱：回補為賣出之鏡像，比照 SELL 同等寬鬆(原1.00過嚴)

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

# ✅08301755【ＡＭ１ 通知信時段管制】主帥 08/30 指示
#   非急迫通知嚴禁在睡眠時段(21:30~07:30)寄發；一律延到當天 20:30 統一寄出。
#   ★急迫類（可在睡眠時段直接寄）：條件W進場、期貨/美股/虛擬幣即時進出場訊號。
QUIET_START_HHMM   = (21, 30)   # 睡眠時段起（台灣時間）
QUIET_END_HHMM     = (7, 30)    # 睡眠時段迄
DIGEST_SEND_HHMM   = (20, 30)   # 非急迫通知統一寄發時點（台股盤後20:00公布+容納延遲）
DIGEST_WINDOW_MIN  = 20         # 20:30起 20 分鐘內視為可寄發窗

# ✅08301755【15分K 容忍度改綁通道寬度】★本次做空誤訊號的根因修正
#   舊制 _close >= _boll_top * SELL_BOLL_TOLERANCE(0.98) 在15分K上【數學上必然成立】：
#     15分K布林通道總寬約為價格的 1%，而 2% 的容忍區間 ≈ 通道寬度的 1.8 倍
#     → 價格就算跌到【布林下軌】，該式依然成立 → ★這道防線形同虛設。
#   新制改為【距離軌線 ≤ 半通道寬 × BAND_NEAR_RATIO】，與價格絕對值無關，
#   任何K棒週期都成立（半通道寬＝2σ，0.25 即「距軌線 0.5σ 以內」）。
BAND_NEAR_RATIO    = 0.25       # 多空共用，★修改時兩側同步（ＡＫ１８）
# ✅08301905【容忍度全面改綁通道寬度】主帥 08/30 19:05 核准「一併改」
#   ★★關鍵設計：★取【百分比制】與【通道制】兩者中【較嚴】者，★不是直接換掉。
#   ★理由（實測）：★短週期(5分/15分K)通道窄 → 通道制較嚴 → ★修好誤訊號；
#     ★★長週期(月K)通道寬(±15%) → 百分比制較嚴 → ★★沿用舊制，行為不變。
#   ★★★若一律換成通道制，★月K 反而會被【放寬】——★那是新的錯，不是修正。
#   ★★凍結開關：★★若主帥實測後認為訊號變太少，
#     ★★★把 BAND_GATE_ENABLED 改成 False 即可【完全回到 08301755 前的行為】。
BAND_GATE_ENABLED  = True       # ★一行回滾開關

def _gate_upper(bt, bb):
    """✅08301905【近上軌門檻】回傳價格門檻；★取百分比制與通道制中【較嚴】者（較高者）。
    ★支援純量與 pandas Series（向量化比較用）。"""
    if not BAND_GATE_ENABLED:
        return bt * SELL_BOLL_TOLERANCE
    _half = (bt - bb) / 2.0
    _pct  = bt * SELL_BOLL_TOLERANCE
    _band = bt - _half * BAND_NEAR_RATIO
    try:                                   # Series：逐元素取較嚴者
        return _pct.combine(_band, max)
    except AttributeError:                 # 純量
        return max(float(_pct), float(_band))

def _gate_lower(bt, bb, tol=None):
    """✅08301905【近下軌門檻】回傳價格門檻；★取兩制中【較嚴】者（較低者）。"""
    _t = BUY_BOLL_TOLERANCE if tol is None else tol
    if not BAND_GATE_ENABLED:
        return bb * _t
    _half = (bt - bb) / 2.0
    _pct  = bb * _t
    _band = bb + _half * BAND_NEAR_RATIO
    try:
        return _pct.combine(_band, min)
    except AttributeError:
        return min(float(_pct), float(_band))

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
_rt_price_map = {}  # ✅08061155 即時價對照表 {ticker: 現價}，由 prefetch_realtime_prices() 批次填入
daily_cache  = {} # 中期投資週K快取（原日K，v05170856改週K）

warnings.filterwarnings('ignore')
import logging
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ══════════════════════════════════════════════════════════════
# ✅ (08060719) 台灣時間統一取用函式【時區bug根治】
#   緣由：嘉義房租專案版AI 於 08/06 提供之「日期/時區 bug」教訓——
#   裸用 datetime.now() 取的是【執行環境本地時間】，在 GitHub Actions 上就是 UTC。
#   台灣 UTC+8，故 UTC 00:00~05:59 ＝ 台灣 08:00~13:59（正是台股交易時段），
#   任何以「小時／日期／星期」做判斷的邏輯在雲端都會整組錯位。
#   本專案 08/03 修的漲停追蹤深夜誤發即為此型；本次全專案稽核後統一改用本函式。
#   ★注意：純粹計算「經過多久」(elapsed) 的快取TTL【刻意不改】——
#     那些值與快取寫入端同源相減，改成 aware 反而會與 naive 相減而崩潰。
# ══════════════════════════════════════════════════════════════
def _now_tw():
    """一律回傳台灣時間(Asia/Taipei)；嚴禁在日期/小時/星期判斷上裸用 datetime.now()。"""
    return datetime.now(pytz.timezone('Asia/Taipei'))


# ============================================================
# 【期貨K棒新鮮度防護 + 期交所台指期即時對照】✅08091258
# ============================================================
def _bar_age_minutes(df):
    """回傳「最後一根K棒距今幾分鐘」。取不到時回 None（＝不阻擋）。
    ★時區處理：df.index 若帶時區一律轉為台灣時間後再比較，
      避免 GitHub Actions（UTC）與台灣時間相差8小時造成誤判（歷史地雷）。
    """
    try:
        # ★索引必須是時間型，否則不可計算年齡。
        #   實測抓到的漏洞：若 index 是 0,1,2 這種序號，pd.Timestamp(2) 會被當成
        #   「1970年起算2奈秒」而算出 2900 萬分鐘 → 防護反而把所有訊號全擋掉。
        if not isinstance(df.index, pd.DatetimeIndex):
            return None
        _ts = pd.Timestamp(df.index[-1])
        if _ts.tz is not None:
            _ts = _ts.tz_convert('Asia/Taipei').tz_localize(None)
        _now = _now_tw().replace(tzinfo=None)
        return (_now - _ts.to_pydatetime()).total_seconds() / 60.0
    except Exception:
        return None


def _bar_too_old(df, label):
    """K棒是否過舊。過舊回 True 並印出原因；取不到時間一律回 False（不阻擋）。"""
    _age = _bar_age_minutes(df)
    if _age is None:
        return False
    if _age > FUT_BAR_MAX_AGE_MIN:
        _feat('stale', f'{label} 被擋（陳舊 {_age:.0f} 分）')
        print(f'  ⛔ {label}：最後一根5分K已陳舊 {_age:.0f} 分鐘'
              f'（上限 {FUT_BAR_MAX_AGE_MIN} 分）→ 不進場')
        print('     原因：^TWII 加權指數僅 09:00~13:30 有資料，夜盤無報價；'
              '此時判斷會用到白天的舊K棒。')
        return True
    return False


def _mk_summary(_rows, _unit='支'):
    """✅08242251【主帥指定】由訊號清單彙整出「台股2支／美股1支」這種市場別字串。
    ★_rows 每一列的第 0 欄即為市場別（既有結構，未新增欄位）。
    ★順序固定，不隨字典順序浮動；★遇到預期外的市場別也會列出，不會漏。
    """
    try:
        _c = {}
        for _r in _rows:
            _k = str(_r[0]) if (isinstance(_r, (list, tuple)) and _r) else '其他'
            _c[_k] = _c.get(_k, 0) + 1
        _pref = ['台股', '美股', '虛擬幣', '外匯', '基金', '期貨']
        _keys = [k for k in _pref if k in _c] + [k for k in _c if k not in _pref]
        _s = '／'.join(f'{k}{_c[k]}{_unit}' for k in _keys)
        return _s or f'{len(_rows)}{_unit}'
    except Exception:
        return f'{len(_rows)}{_unit}'


def _taifex_index_snapshot():
    """由期交所 MIS 取得【臺指現貨】與【臺指期近月】即時報價，供對照顯示。
    ★用途限定為「顯示與對照」，不參與進場判斷——因為 MIS 只有即時快照、
      沒有5分K歷史，與 ^TWII 的K棒序列不可混用（兩者價位相差約100~200點，
      混用會使布林通道位階整組偏移而製造假訊號）。
    ★失敗即回 None，絕不影響訊號發送。
    """
    try:
        import requests as _req
        _r = _req.post(OPT_MIS_URL,
                       json={"MarketType": "0", "SymbolType": "F", "KindID": "1",
                             "CID": "TXF", "ExpireMonth": "", "RowSize": "全部",
                             "PageNo": "", "SortColumn": "", "AscDesc": "A"},
                       timeout=OPT_MIS_TIMEOUT,
                       headers={'Referer': OPT_MIS_REFERER,
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        if _r.status_code != 200:
            return None
        _ql = ((_r.json() or {}).get('RtData') or {}).get('QuoteList') or []
        _spot = _fut = _ftime = None
        _fhigh = _flow = _fopen = _fvol = None      # ✅08242251 甲案新增
        def _f2(_v):
            try:
                _x = float(str(_v).replace(',', ''))
                return _x if _x > 0 else None
            except Exception:
                return None
        for _q in _ql:
            if not isinstance(_q, dict):
                continue
            _nm = str(_q.get('DispCName', ''))
            try:
                _px = float(str(_q.get('CLastPrice', '')).replace(',', ''))
            except Exception:
                continue
            if _px <= 0:
                continue
            if '現貨' in _nm and _spot is None:
                _spot = _px
            elif '期' in _nm and _fut is None:      # 清單首檔期貨＝近月
                _fut = _px
                _ftime = str(_q.get('CTime', ''))
                # ✅08242251【甲案核心】★MIS 有提供【當日】最高/最低/開盤/累計量，
                #   ★這四個是【累計值】，其【變化量】即為本根K棒內的真實極值與成交量。
                #   ★08241800 實測欄位：CHighPrice/CLowPrice/COpenPrice/CTotalVolume
                _fhigh = _f2(_q.get('CHighPrice'))
                _flow  = _f2(_q.get('CLowPrice'))
                _fopen = _f2(_q.get('COpenPrice'))
                try:
                    _fvol = int(float(str(_q.get('CTotalVolume', '0')).replace(',', '')))
                except Exception:
                    _fvol = None
        if _fut is None:
            return None
        return {'spot': _spot, 'fut': _fut, 'time': _ftime,
                'basis': (None if _spot is None else _fut - _spot),
                'd_high': _fhigh, 'd_low': _flow, 'd_open': _fopen, 'd_vol': _fvol}
    except Exception:
        return None


def _taifex_hint_text():
    """組出可附在通知信/log 的台指期即時對照字串；取不到回空字串。"""
    _s = _taifex_index_snapshot()
    if not _s:
        return ''
    _t = f"　台指期近月（期交所即時）：{_s['fut']:.0f}"
    if _s.get('spot') is not None:
        _t += f"　加權指數：{_s['spot']:.0f}　基差：{_s['basis']:+.0f}"
    if _s.get('time'):
        _t += f"　報價時間：{_s['time']}"
    return _t




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
    now = datetime.now()  # ⏱️elapsed快取TTL用，非日期/星期判斷 → 時區無關（08090225 依自檢(5)逐筆確認）

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
                days_left = (d - _now_tw().date()).days   # ✅08060719 時區修正：原 _date.today() 在雲端取UTC，天數會差一天
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
    now = _dt.now()  # ⏱️elapsed快取TTL用，非日期/星期判斷 → 時區無關（08090225 依自檢(5)逐筆確認）
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

    # ══ ✅09021415【期貨時窗＝條件W 時窗，單一真實來源】主帥 09/02 14:00 指令 ══
    #   ★★★這裡原本有【三段】各自為政的判斷（週一~週三窗／週選夜盤窗／日盤補齊），
    #     ★三段用了 11:30、11:00、13:30 三個不同的邊界值，
    #     ★★★而 _condw_current_window() 又是第四份抄本 —— 共四份、四種寫法。
    #   ★★2026/09/02 主帥發現「週三11:30、週五11:00」不一致並震怒，根因就是這個。
    #   ★本輪一律改為呼叫 _condw_current_window()，★★全檔只剩【一份】時窗定義。
    #   ★★★改動時序（R-12 沿革）：10:45(06/30) → 11:00(08/09) → 11:30(09/02)
    is_futures_time = (_condw_current_window() is not None)

    # ✅09021415【台股盤中極端異動 F-12 獨立時窗】★不得被期貨時窗綁架。
    #   ★理由：F-12 是台股功能，與週選擇權無關；★★期貨時窗縮成條件W 窗後，
    #     ★★★週一全天、週二/週四日盤、週三/週五11:30後的台股極端異動會一併消失。
    if (not is_futures_time) and TW_INTRADAY_EXTREME_ENABLED \
            and 0 <= weekday <= 4 and (9*60+5 <= time_val <= 13*60+30):
        is_futures_time = True

    if is_futures_time:
        active.append('FUTURES')  # 台指期5分K

    return active

# ============================================================
# 【本輪功能自報】✅08100100（鐵律AE3）
# ------------------------------------------------------------
# 背景：主帥 2026/08/10 質問——待辦🟡H 列了 14 項「待實機實測」，
#   卻沒有任何一步一步的教學，等於把驗收工作丟給主帥。
# ★正確解法不是寫教學叫主帥照做，而是【讓程式自己報告】。
#   每輪掃描結束時，把各新功能「有沒有跑到、跑出什麼結果」印成一個區塊，
#   主帥只要掃一眼這個區塊，就知道全部功能是否正常，不必逐項手動驗證。
# ============================================================
_FEAT = {}


def _feat(key, value):
    """記錄一項功能的執行結果，供結束時的【本輪功能自報】使用。永不拋錯。"""
    try:
        _FEAT[key] = value
    except Exception:
        pass


def _feat_bump(key):
    """次數累加型記錄。"""
    try:
        _FEAT[key] = _FEAT.get(key, 0) + 1
    except Exception:
        pass


def print_feature_report():
    """✅08100100 印出【本輪功能自報】。主帥只要看這一塊即可完成驗收。"""
    try:
        print("\n" + "=" * 58)
        print("  📋【本輪功能自報】主帥只要看這一塊，不必手動驗證任何東西")
        # ✅08101455 標示執行模式：同一個 workflow 有 scan 與 futures-scan 兩個 job，
        #   各自只跑自己那條路徑。不標示的話，主帥會誤以為「該跑的沒跑」。
        _mode = {'5mk': '期貨5分K（futures-scan job）',
                 'condW': '條件W週選擇權（condw-scan job）'}.get(
                     TEST_MODE, '全市場掃描（scan job）')
        print(f"  執行模式：{_mode}　版本：{SCRIPT_VERSION}")
        print("=" * 58)
        _rows = [
            ('通知時段判定（F-10）',      'session',        '未執行到（本輪無訊號）'),
            ('觀察清單讀取（F-14）',      'watchlist',
             '本 job 不負責（只在全市場掃描執行）' if TEST_MODE in ('5mk', 'condW')
             else '未執行到'),
            ('觀察清單補掃（F-14）',      'watchlist_extra', '未執行到'),
            ('台指期K棒累積（F-11）',     'txf_bars',
             '本 job 不負責（請看 futures-scan job）' if TEST_MODE not in ('5mk', 'condW')
             else '未執行到（非期貨時段）'),
            ('夜盤陳舊K棒防護（F-07）',   'stale',          '未觸發（K棒新鮮，正常）'),
            ('選擇權即時權利金（R-01替代）', 'opt_chain',    '未執行到（本輪無選擇權建議）'),
            ('台股盤中極端偵測（F-12）',  'intraday',       '未執行到（非台股交易時段）'),
            ('即時價批次預抓',            'prefetch',       '未執行到'),
        ]
        _bad = 0
        for _label, _k, _na in _rows:
            _v = _FEAT.get(_k)
            # ★本輪實測抓到：異常訊息（以❌開頭）原本也被標成 ✅，會誤導判讀。
            if not _v:
                _icon = '⏭️ '
            elif str(_v).startswith('❌'):
                _icon = '❌'; _bad += 1
            else:
                _icon = '✅'
            print(f"  {_icon} {_label}：{_v if _v else _na}")
        print("-" * 58)
        print("  判讀：✅＝該功能本輪確實執行並回報結果；⏭️＝本輪不適用（非該時段/無訊號）")
        print("  ★若某項【應該要跑卻顯示 ⏭️】，把這個區塊貼給AI 即可定位。")
        if _bad:
            print(f"  ❌★本輪有 {_bad} 項異常（見上方❌），請把本區塊貼給AI 處理。")
        print("=" * 58 + "\n")
    except Exception:
        pass


def _signal_session():
    """✅08091843 判定買/賣訊號目前屬【日盤】或【夜盤】（台灣時間）。
    日盤 08:00~15:00，其餘為夜盤。界線比條件W(09:05~13:30)寬，
    因為全量掃描一輪可達45分鐘，以送信當下時間判定會跨出台股交易時段。
    """
    try:
        _n = _now_tw()
        _tv = _n.hour * 60 + _n.minute
        return 'day' if (SIGNAL_DAY_START_MIN <= _tv < SIGNAL_DAY_END_MIN) else 'night'
    except Exception:
        return 'day'


_txf_acc_done = False   # 本次行程是否已累積過（避免同一輪重複寫入）


def accumulate_txf_bar():
    """✅08091843【🔴B(a) 階段一】把期交所台指期即時快照累積成自建5分K序列。
    ★只累積與回報，不參與任何進場判斷（見 TXF_BAR_ENABLED 決策註記）。
    ★儲存於 Firestore（GitHub Actions 每輪檔案系統會重置，本地檔無法跨輪保存）。
    ★任何失敗都只印訊息並回傳 False，絕不影響掃描與通知。
    """
    global _txf_acc_done
    if (not TXF_BAR_ENABLED) or _txf_acc_done:
        return False
    _txf_acc_done = True
    # ✅08250451【迴圈取樣】開啟後，本次 job 內每 TXF_LOOP_INTERVAL 秒取樣一次，
    #   ★持續 TXF_LOOP_SECONDS 秒，★讓每根 5 分K 有 13~18 個樣本，
    #   ★補強「極值不是當日新高低」那些根的 h/l 估計。
    if TXF_LOOP_SAMPLING:
        import time as _tm
        _t0 = _tm.time()
        _cnt = 0
        while _tm.time() - _t0 < TXF_LOOP_SECONDS:
            _txf_acc_done = False
            try:
                _one_txf_sample()
                _cnt += 1
            except Exception as _le:
                print(f'  ⚠️ 迴圈取樣第{_cnt+1}次異常：{str(_le)[:40]}')
            _tm.sleep(TXF_LOOP_INTERVAL)
        _txf_acc_done = True
        print(f'  🔁 迴圈取樣完成：本次 job 共取樣 {_cnt} 次'
              f'（每{TXF_LOOP_INTERVAL}秒／共{TXF_LOOP_SECONDS}秒）')
        return True
    return _one_txf_sample()


def _one_txf_sample():
    """✅08250451 單次取樣＋寫入（由 accumulate_txf_bar 呼叫，★不再自行檢查旗標）。"""
    try:
        _snap = _taifex_index_snapshot()
        if not _snap or not _snap.get('fut'):
            print('  ⏭️ 台指期K棒累積：取不到即時快照，本輪略過')
            return False
        _px = float(_snap['fut'])

        import json as _json, os as _os, requests as _req
        _cred = _os.environ.get(FIREBASE_CRED_ENV)
        if not _cred:
            _cf = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), FIREBASE_CRED_FILE)
            if _os.path.exists(_cf):
                with open(_cf, 'r', encoding='utf-8') as _f:
                    _cred = _f.read()
        if not _cred:
            print('  ⏭️ 台指期K棒累積：無 Firebase 憑證，本輪略過（不影響掃描）')
            return False
        import google.oauth2.service_account as _sa
        import google.auth.transport.requests as _gtr
        _c = _sa.Credentials.from_service_account_info(
            _json.loads(_cred), scopes=['https://www.googleapis.com/auth/datastore'])
        _c.refresh(_gtr.Request())
        _url = (f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
                f"/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}"
                f"/public/txf_5m_bars")
        _hdr = {"Authorization": f"Bearer {_c.token}", "Content-Type": "application/json"}

        _bars = []
        try:
            _g = _req.get(_url, headers=_hdr, timeout=15)
            if _g.status_code == 200:
                _raw = ((_g.json() or {}).get('fields') or {}).get('bars', {}).get('stringValue', '')
                if _raw:
                    _bars = _json.loads(_raw)
        except Exception:
            _bars = []                      # 讀不到就從空的開始，不中斷

        # 以台灣時間對齊到 5 分鐘桶
        _n = _now_tw()
        _bkey = _n.strftime('%Y-%m-%d %H:') + f"{(_n.minute // 5) * 5:02d}"
        # ✅08242251【甲案核心】★用【當日最高/最低的差分】還原棒內真實極值。
        #   ★舊版只用取樣價 max/min：兩次取樣之間的極端【一定漏掉】，
        #     ★單次取樣時更會變成 o=h=l=c 的「無高度棒」，
        #     ★而條件W 的判斷正是「任一最低價 ≤ 下軌／任一最高價 ≥ 上軌」。
        #   ★新版邏輯：
        #     ・當日最高 CHighPrice 若比本棒開始時上升 → ★本棒內確實成交過那個新高
        #     ・當日最低 CLowPrice  若比本棒開始時下降 → ★本棒內確實成交過那個新低
        #     ★這是【真實成交價】，不是取樣運氣。
        #   ★★已知限制（主帥 08/24 18:19 明示【可接受】）：
        #     若某根的極值不是當日新高/新低，差分抓不到，退回取樣價 max/min（會低估）。
        #     ★條件W 是「近5根【任一】」，只要5根裡有一根創當日新高/低即可成立。
        _dh = _snap.get('d_high') if isinstance(_snap, dict) else None
        _dl = _snap.get('d_low')  if isinstance(_snap, dict) else None
        _dv = _snap.get('d_vol')  if isinstance(_snap, dict) else None
        # ✅08250517【★★跨棒差分．重大修正】
        #   ★08250513 實測（主帥截圖）暴露一個致命問題：
        #     「台指期K棒累積：05:05 O44762 H44762 L44762 C44762 V0（第1次取樣）」
        #     ★★O=H=L=C 且 V=0，★因為 dh0/dl0/dv0 是在【建棒當下】才設定，
        #     ★同一根棒要有【第2次以上取樣】才算得出差分。
        #   ★★而 condw_scan 每 15 分鐘才跑一次 → ★每次都落在不同的 5 分桶
        #     → ★★★每根棒永遠只有「第1次取樣」→ ★差分永遠算不出來。
        #   ★★★所以我上一輪說「迴圈取樣只是補強、缺它不影響核心」是【錯的】，
        #     ★★差分要生效，必須有「同一根棒內≥2次取樣」或「跨棒基準」。
        #   ★本次修正：★建新棒時，基準改取【上一根棒最後看到的當日高/低/量】
        #     （dh1/dl1/dv1），★而不是建棒當下的值。
        #     ★這樣即使每根只取樣一次，也能抓到「自上次取樣以來的當日高低推進」。
        #   ★★已知限制（★據實列出，不誇大）：
        #     取樣間隔越長，極值的【歸屬誤差】越大——
        #     15 分鐘間隔下，極值可能實際發生在中間某根，卻被歸給取樣當下那根。
        #     ★所以迴圈取樣仍然必要，★它是把誤差從 15 分鐘縮到 20 秒。
        _prev = _bars[-1] if _bars else None
        if _bars and _bars[-1].get('t') == _bkey:
            _b = _bars[-1]
            _b['h'] = max(_b['h'], _px)
            _b['l'] = min(_b['l'], _px)
            if _dh is not None and _b.get('dh0') is not None and _dh > _b['dh0']:
                _b['h'] = max(_b['h'], _dh)
            if _dl is not None and _b.get('dl0') is not None and _dl < _b['dl0']:
                _b['l'] = min(_b['l'], _dl)
            if _dv is not None and _b.get('dv0') is not None and _dv >= _b['dv0']:
                _b['v'] = _dv - _b['dv0']
            _b['c'] = _px
            _b['n'] = _b.get('n', 1) + 1
            _b['src'] = 'diff' if (_dh is not None and _dl is not None) else 'sample'
        else:
            # ★★新棒起始：基準取【前一根最後看到的當日高/低/量】，★不是當下值
            _h0 = _prev.get('dh1') if (_prev and _prev.get('dh1') is not None) else _dh
            _l0 = _prev.get('dl1') if (_prev and _prev.get('dl1') is not None) else _dl
            _v0 = _prev.get('dv1') if (_prev and _prev.get('dv1') is not None) else _dv
            _nb = {'t': _bkey, 'o': _px, 'h': _px, 'l': _px, 'c': _px, 'n': 1,
                   'v': 0, 'dh0': _h0, 'dl0': _l0, 'dv0': _v0,
                   'src': 'diff' if (_dh is not None and _dl is not None) else 'sample'}
            # ★建棒當下就先做一次差分（相對於前一根的基準）
            if _dh is not None and _h0 is not None and _dh > _h0:
                _nb['h'] = max(_nb['h'], _dh)
            if _dl is not None and _l0 is not None and _dl < _l0:
                _nb['l'] = min(_nb['l'], _dl)
            if _dv is not None and _v0 is not None and _dv >= _v0:
                _nb['v'] = _dv - _v0
            _bars.append(_nb)
        # ★★每次取樣都記下「當下的當日高/低/量」，供【下一根】當基準
        _bars[-1]['dh1'] = _dh
        _bars[-1]['dl1'] = _dl
        _bars[-1]['dv1'] = _dv
        _bars = _bars[-TXF_BAR_KEEP:]

        _p = _req.patch(_url, headers=_hdr, timeout=15,
                        json={"fields": {
                            "bars":       {"stringValue": _json.dumps(_bars, ensure_ascii=False)},
                            "updated_at": {"stringValue": _n.strftime('%Y-%m-%d %H:%M:%S')},
                            "source":     {"stringValue": "taifex_mis_snapshot"}}})
        if _p.status_code not in (200, 201):
            print(f'  ⚠️ 台指期K棒累積：寫入失敗 HTTP {_p.status_code}')
            return False
        _bb = _bars[-1]
        print(f"  📈 台指期K棒累積：{_bkey} "
              f"O{_bb['o']:.0f} H{_bb['h']:.0f} L{_bb['l']:.0f} C{_bb['c']:.0f} "
              f"V{_bb.get('v', 0)}（第{_bb['n']}次取樣．來源={_bb.get('src', '?')}）"
              f"　已累積 {len(_bars)}/{TXF_BAR_KEEP} 根")   # ✅08242251 印出完整OHLCV供主帥核對
        _feat('txf_bars', f"已累積 {len(_bars)}/{TXF_BAR_KEEP} 根（本棒收 {_px:.0f}）")
        if len(_bars) < 54:
            print(f"     ⏳ 距可計算指標(54根)還差 {54 - len(_bars)} 根；"
                  f"★階段一只累積不判斷，屬正常。")
        return True
    except Exception as _e:
        print(f'  ⚠️ 台指期K棒累積異常（{str(_e)[:50]}）→ 略過，不影響掃描')
        return False


def check_tw_intraday_extreme():
    """✅08092108【🆕E】台股白天大盤極端異動【盤中即時】偵測。
    原本只有收盤後的日K版本（check_tw_daytime_extreme），主帥收到通知時
    行情早已結束。本函式在台股交易時段內每輪執行，達門檻立即發信。
    ★任何失敗都只印訊息並返回，絕不影響掃描與其他通知。
    """
    if not TW_INTRADAY_EXTREME_ENABLED:
        return
    try:
        _n = _now_tw()
        _tv = _n.hour * 60 + _n.minute
        if not (9 * 60 <= _tv <= 13 * 60 + 35):
            return                      # 非台股交易時段，直接跳過
        if _n.weekday() > 4:
            return                      # 週末不跑
        _today_str = _n.strftime('%Y-%m-%d')

        # ── 取昨收（加權指數日K）──
        import yfinance as _yf
        _d = _normalize_df(_yf.download('^TWII', period='7d', interval='1d', progress=False))
        if _d is None or len(_d) < 2:
            return
        # 盤中時 yfinance 的日K最後一根是「今天的未完成棒」，昨收要取前一根。
        _prev = None
        try:
            _last_day = str(_d.index[-1])[:10]
            _prev = _safe_float(_d['Close'].iloc[-2] if _last_day == _today_str
                                else _d['Close'].iloc[-1])
        except Exception:
            _prev = _safe_float(_d['Close'].iloc[-2])
        if not _prev or _prev <= 0:
            return

        # ── 取現價：①期交所MIS臺指現貨（首選）②^TWII 5分K（備援）──
        _cur, _src = None, ''
        _snap = _taifex_index_snapshot()
        if _snap and _snap.get('spot'):
            _cur, _src = float(_snap['spot']), '期交所即時'
        if _cur is None:
            _f = _normalize_df(_yf.download('^TWII', period='1d', interval='5m', progress=False))
            if _f is None or _f.empty:
                return
            if _bar_too_old(_f, '台股盤中極端偵測'):
                return                  # 陳舊資料不得據以發信（鐵律AF2）
            _cur, _src = _safe_float(_f['Close'].iloc[-1]), '^TWII 5分K'
        if not _cur or _cur <= 0:
            return

        _pts = _cur - _prev
        _pct = _pts / _prev * 100
        _feat('intraday', f'{_pts:+.0f}點 {_pct:+.2f}%（來源：{_src}）')
        print(f"  📊 台股盤中大盤：{_cur:.0f}（昨收{_prev:.0f}，{_pts:+.0f}點 {_pct:+.2f}%）"
              f"　來源：{_src}")
        if abs(_pts) < TW_EXTREME_PTS:
            return

        _dir = 'DOWN' if _pts < 0 else 'UP'
        # 額度：同方向當日最多2次（與買賣訊號同為原子佔位，跨雲端並行安全）
        # ✅08092144 本函式只在日盤執行，故採日盤上限
        if not _claim_notify_slot(f"TWII_INTRADAY_{_dir}", _today_str, notified,
                                  SIGNAL_MAX_DAY):
            print(f"  🔕 台股盤中極端異動（{_dir}）今日額度已用完，靜音")
            return

        _emoji = '🔻' if _dir == 'DOWN' else '🚀'
        _arr = '↘' if _dir == 'DOWN' else '↗'
        _act = ('暴跌！考慮台指期做空或 buy put' if _dir == 'DOWN'
                else '急漲！考慮台指期做多或 buy call')
        _subject = (f"💻【本機】{_emoji}【盤中即時】台股極端異動！"
                    f"{_arr}{int(abs(_pts))}點({_pct:+.1f}%)")
        _body = '\n'.join([
            f'⚠️ 台股大盤【盤中即時】極端異動（台灣時間 {_n.strftime("%H:%M")}）',
            '=' * 35,
            f'現在加權指數：{_cur:.0f}（昨收 {_prev:.0f}）',
            f'漲跌幅：{_arr}{int(abs(_pts)):,}點（{_pct:+.2f}%）',
            f'報價來源：{_src}',
            '=' * 35,
            f'💡 建議：{_act}',
            '',
            '★這是【盤中即時】通知，行情仍在進行中。',
            '　收盤後若仍達門檻，會再收到一封【收盤確認】信（主旨不含「盤中即時」）。',
        ])
        _ok = send_gmail(_subject, _body)
        print(f"  {'✅' if _ok else '❌'} 台股盤中極端異動通知"
              f"{'已發送' if _ok else '發送失敗'}（{_dir} {_pts:+.0f}點）")
    except Exception as _e:
        print(f"  ⚠️ 台股盤中極端偵測異常（{str(_e)[:50]}）→ 略過，不影響掃描")


_watchlist_cache = {'ts': 0, 'items': None}


def _load_watchlist():
    """✅08100036【⚪F′】讀取網頁版「待買觀察清單」。回傳 [{'code':..,'cat':..}, ...]。
    ★只讀不寫，絕不修改，避免與網頁版互相覆蓋。
    ★任何失敗都回空清單並印訊息，絕不影響掃描與通知。
    """
    global _watchlist_cache
    if not WATCHLIST_ENABLED:
        return []
    import time as _t
    if _watchlist_cache['items'] is not None and (_t.time() - _watchlist_cache['ts']) < 300:
        return _watchlist_cache['items']
    _items = []
    try:
        import json as _json, os as _os, requests as _req
        _cred = _os.environ.get(FIREBASE_CRED_ENV)
        if not _cred:
            _cf = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), FIREBASE_CRED_FILE)
            if _os.path.exists(_cf):
                with open(_cf, 'r', encoding='utf-8') as _f:
                    _cred = _f.read()
        if not _cred:
            print('  ⏭️ 觀察清單：無 Firebase 憑證，略過（不影響掃描）')
            _feat('watchlist', '❌無 Firebase 憑證 → 請告知AI')
            _watchlist_cache = {'ts': _t.time(), 'items': []}
            return []
        import google.oauth2.service_account as _sa
        import google.auth.transport.requests as _gtr
        _c = _sa.Credentials.from_service_account_info(
            _json.loads(_cred), scopes=['https://www.googleapis.com/auth/datastore'])
        _c.refresh(_gtr.Request())
        # 文件ID規則與網頁版 getSafeDocId 完全一致：小寫後把 @ 和 . 換成 _
        _safe = WATCHLIST_OWNER.lower().replace('@', '_').replace('.', '_')
        _url = (f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
                f"/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}"
                f"/users/{_safe}/data/stocks")
        _r = _req.get(_url, headers={"Authorization": f"Bearer {_c.token}"}, timeout=15)
        if _r.status_code != 200:
            print(f'  ⏭️ 觀察清單：讀取 HTTP {_r.status_code}，略過')
            _watchlist_cache = {'ts': _t.time(), 'items': []}
            return []
        _arr = (((_r.json() or {}).get('fields') or {})
                .get('watchlist', {}).get('arrayValue', {}).get('values', []))
        for _v in _arr:
            _mv = (_v or {}).get('mapValue', {}).get('fields', {})
            _code = (_mv.get('code', {}) or {}).get('stringValue', '').strip()
            _cat = (_mv.get('cat', {}) or {}).get('stringValue', '').strip().lower()
            if _code:
                _items.append({'code': _code.upper(), 'cat': _cat or 'tw'})
        print(f'  ⭐ 觀察清單：讀到 {len(_items)} 檔（來源：網頁版待買觀察）')
        _feat('watchlist', f'讀到 {len(_items)} 檔')
    except Exception as _e:
        print(f'  ⚠️ 觀察清單讀取異常（{str(_e)[:45]}）→ 視為空清單，不影響掃描')
        _items = []
    _watchlist_cache = {'ts': _t.time(), 'items': _items}
    return _items


def _watchlist_codes():
    """回傳觀察清單的純代碼集合（大寫、去除交易所後綴），供比對用。"""
    _s = set()
    for _it in _load_watchlist():
        _c = _it['code'].upper()
        _s.add(_c)
        _s.add(_c.split('.')[0])            # 2330.TW → 2330
        _s.add(_c.replace('-USD', ''))      # BTC-USD → BTC
        _s.add(_c.replace('=X', ''))        # USDTWD=X → USDTWD
    return _s


def _wl_mark(code):
    """若該代碼在觀察清單內，回傳標記字串；否則回空字串。"""
    try:
        if not WATCHLIST_ENABLED:
            return ''
        _c = str(code).upper()
        _set = _watchlist_codes()
        # ★傳入代碼也要做同樣的正規化，否則 BTC-USD／USDTWD=X 會比不到。
        #   （本輪實測抓到：清單存 BTC，訊號傳 BTC-USD → 原寫法不命中）
        _cands = {_c, _c.split('.')[0], _c.replace('-USD', ''), _c.replace('=X', '')}
        if _cands & _set:
            return '⭐【你的觀察清單】\n'
    except Exception:
        pass
    return ''


def _watchlist_to_ticker(item):
    """把觀察清單項目換算成 yfinance 代碼。無法判斷回 None。"""
    _c, _cat = item['code'].upper(), item.get('cat', 'tw')
    if '.' in _c or '=' in _c or '-' in _c:
        return _c                      # 已是完整代碼，直接用
    if _cat == 'tw':
        return _c + '.TW'              # 上櫃(.TWO)由呼叫端失敗後自動再試
    if _cat == 'crypto':
        return _c + '-USD'
    if _cat == 'fx':
        return _c + '=X'
    return _c                          # us／fund／gold／bond 多為原代碼


def scan_watchlist_extras(scanned_tickers):
    """✅08100036【⚪F′】補掃【全市場掃描範圍外】的觀察清單標的。
    ★只掃「主流程沒掃過」的，避免重複耗時。
    ★任何單一標的失敗都跳過，絕不中斷整體掃描。
    """
    if not WATCHLIST_ENABLED:
        return []
    _out = []
    try:
        _items = _load_watchlist()
        if not _items:
            return []
        _done = {str(t).upper() for t in (scanned_tickers or [])}
        _done |= {str(t).upper().split('.')[0] for t in (scanned_tickers or [])}
        _todo = []
        for _it in _items:
            _tk = _watchlist_to_ticker(_it)
            if not _tk:
                continue
            if _tk.upper() in _done or _tk.upper().split('.')[0] in _done:
                continue
            _todo.append(_tk)
        _todo = _todo[:WATCHLIST_EXTRA_MAX]
        if not _todo:
            print('  ⭐ 觀察清單：全部已在主掃描範圍內，無需補掃')
            _feat('watchlist_extra', '全部已在主掃描範圍內，無需補掃')
            return []
        print(f'  ⭐ 觀察清單補掃：{len(_todo)} 檔（主掃描範圍外）{_todo}')
        for _tk in _todo:
            try:
                _raw = scan_stock_mixed(_tk, False) if SCAN_MODE == 'mixed' else scan_stock(_tk, False)
                if not _raw:
                    continue
                _ml = _raw[-1] if isinstance(_raw[-1], str) and _raw[-1] in ('長期投資', '中期投資') else None
                _res = _raw[:-1] if _ml else _raw
                if _res and _res[0]:
                    _out.append(('⭐觀察', _tk, *_res[1:], _ml if _ml else ''))
                    print(f'    ✅ {_tk} 觀察清單補掃：買進訊號')
            except Exception as _e:
                print(f'    ⚠️ {_tk} 補掃失敗（{str(_e)[:40]}），跳過')
        print(f'  ⭐ 觀察清單補掃完成：{len(_out)} 支觸發買進訊號')
        _feat('watchlist_extra', f'補掃 {len(_todo)} 檔，{len(_out)} 支觸發')
    except Exception as _e:
        print(f'  ⚠️ 觀察清單補掃異常（{str(_e)[:45]}）→ 略過，不影響主掃描')
    return _out


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
def _in_quiet_hours(_now=None):
    """✅08301755【ＡＭ１】判斷現在是否落在睡眠時段(21:30~07:30 台灣時間)"""
    _t = _now or datetime.now(pytz.timezone('Asia/Taipei'))
    _m = _t.hour * 60 + _t.minute
    _s = QUIET_START_HHMM[0] * 60 + QUIET_START_HHMM[1]
    _e = QUIET_END_HHMM[0]   * 60 + QUIET_END_HHMM[1]
    return _m >= _s or _m < _e          # 跨午夜區間

def _digest_window_now(_now=None):
    """✅08301755【ＡＭ１】現在是否落在 20:30 起的統一寄發窗"""
    _t = _now or datetime.now(pytz.timezone('Asia/Taipei'))
    _m = _t.hour * 60 + _t.minute
    _d = DIGEST_SEND_HHMM[0] * 60 + DIGEST_SEND_HHMM[1]
    return _d <= _m < _d + DIGEST_WINDOW_MIN

# ✅08301755【ＡＭ１④】睡眠時段產生的非急迫通知暫存區（延發不是丟棄）
_PENDING_DIGEST = []

def _flush_digest():
    """✅08301755【ＡＭ１④】把暫存的非急迫通知合併為一封寄出"""
    if not _PENDING_DIGEST:
        return False
    _n = len(_PENDING_DIGEST)
    _sub = f"☁️【雲端】🌙 夜間暫存通知彙整（{_n} 則）"
    _body = ("【本信為 21:30~07:30 睡眠時段暫存、於 20:30 統一寄發】\n"
             "（依通用SOP 鐵律ＡＭ１：非急迫通知不在睡眠時段打擾）\n"
             + "=" * 46 + "\n\n")
    for _i, (_s_, _b_) in enumerate(_PENDING_DIGEST, 1):
        _body += f"── 第 {_i} 則：{_s_} ──\n{_b_}\n\n" + "=" * 46 + "\n\n"
    _PENDING_DIGEST.clear()
    return send_gmail(_sub, _body, urgent=True)   # 彙整信本身直接寄，不再遞迴攔截

def maybe_flush_digest():
    """✅08301755【ＡＭ１④】每輪掃描開頭呼叫：若已進入 20:30 寄發窗，把暫存通知合併寄出。
    ★★★跨行程說明（★誠實標示，★不隱藏限制）：
      GitHub Actions 每次執行都是全新行程，_PENDING_DIGEST【不會跨行程保留】。
      ★但這【不構成 ＡＭ１④「延發不是丟棄」的違反】，理由：
        ★持股健檢／投組健檢等非急迫通知，★每次掃描都是【即時重算】，
        ★不是一次性事件。★睡眠時段跳過該次寄發後，
        ★★20:30 的排程會重新掃描並產生【同樣的示警】，★內容不會遺失。
      ★★★因此本輪同時在 stock_scan.yml 新增 20:30(TW) 排程，
        ★否則跳過的通知就真的不會再出現 → ★那才是丟棄。"""
    if _digest_window_now() and _PENDING_DIGEST:
        print(f'  📬 進入 20:30 寄發窗，合併寄出 {len(_PENDING_DIGEST)} 則暫存通知')
        _flush_digest()

def send_gmail(subject, body, urgent=False):
    """✅08301755【ＡＭ１⑥】時段判定放在寄信函式【入口】統一攔截。
    ★不得改放各呼叫點——否則日後新增通知必然漏掉（ＡＫ１８ 型錯誤）。
    urgent=True  → 急迫類，任何時段都直接寄（條件W進場、即時進出場訊號）
    urgent=False → 非急迫類，睡眠時段暫存，於當天 20:30 合併寄出
    """
    if (not urgent) and _in_quiet_hours():
        _PENDING_DIGEST.append((subject, body))
        print(f"  🌙 睡眠時段(21:30~07:30)，非急迫通知不寄發：{subject}")
        print(f"     → 改由 20:30 排程(cron: 30 12 * * 1-5, UTC)重算後寄出")
        return True
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
        _headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        for _attempt in range(3):  # ✅ v06131404：讀取-合併-帶precondition寫回，避免並行整份覆寫互相洗掉key
            _r = _req.get(url, headers=_headers, timeout=10)
            if _r.status_code == 200:
                _doc = _r.json()
                _update_time = _doc.get('updateTime')
                _remote = json.loads(_doc.get('fields', {}).get('data', {}).get('stringValue', '{}'))
                _precond = f"?currentDocument.updateTime={_update_time}" if _update_time else ""
            elif _r.status_code == 404:
                _remote = {}
                _precond = "?currentDocument.exists=false"
            else:
                return  # 讀取異常 → 放棄本次寫入，不覆寫遠端
            # 以遠端為底，逐日聯集本地key（雙方的key都不會被洗掉）
            _merged = dict(_remote)
            for _day, _keys in (data or {}).items():
                _base = list(_merged.get(_day, []))
                for _k in _keys:
                    if _k not in _base:
                        _base.append(_k)
                _merged[_day] = _base
            _payload = {"fields": {"data": {"stringValue": json.dumps(_merged, ensure_ascii=False)}}}
            _pr = _req.patch(url + _precond, json=_payload, headers=_headers, timeout=10)
            if _pr.status_code == 200:
                return
            # 非200 = 期間被其他機器/排程工作搶寫 → 重讀重試
    except Exception as e:
        print(f"  ⚠️ Firebase notified 寫入失敗：{e}")

def _claim_alert_firebase(alert_key, today_str):
    """✅ v06131103：Firebase原子佔位（樂觀並行控制 updateTime precondition）。
    跨多台機器(本機A/B/... + GitHub Actions)防重複通知，與機器數量無關。
    回傳：True=本機成功佔位(可發送)；False=已被佔位或佔位衝突(不發送)；None=無憑證或讀取失敗(交呼叫端走本地後援)。
    """
    try:
        import json, os
        import requests as _req
        cred_json = os.environ.get(FIREBASE_CRED_ENV)
        if not cred_json:
            return None
        cred = json.loads(cred_json)
        import google.oauth2.service_account as _sa
        import google.auth.transport.requests as _gtr
        credentials = _sa.Credentials.from_service_account_info(
            cred, scopes=['https://www.googleapis.com/auth/datastore'])
        credentials.refresh(_gtr.Request())
        token = credentials.token
        url = (f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
               f"/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}/public/notified_log")
        _headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        for _attempt in range(3):  # 最多嘗試3次（處理並行衝突）
            _r = _req.get(url, headers=_headers, timeout=10)
            if _r.status_code == 200:
                _doc = _r.json()
                _update_time = _doc.get('updateTime')
                _data = json.loads(_doc.get('fields', {}).get('data', {}).get('stringValue', '{}'))
                _precond = f"?currentDocument.updateTime={_update_time}" if _update_time else ""
            elif _r.status_code == 404:
                _data = {}
                _precond = "?currentDocument.exists=false"
            else:
                return None  # 讀取異常 → 交呼叫端走本地後援
            # 已被佔位 → 不發送
            if alert_key in _data.get(today_str, []):
                return False
            # 嘗試原子寫入（precondition 失敗代表期間被其他機器搶先）
            _data.setdefault(today_str, []).append(alert_key)
            _payload = {"fields": {"data": {"stringValue": json.dumps(_data, ensure_ascii=False)}}}
            _pr = _req.patch(url + _precond, json=_payload, headers=_headers, timeout=10)
            if _pr.status_code == 200:
                return True   # 佔位成功 → 由本機發送
            # 非200 = 並行衝突 → 重讀重試
        return False  # 重試後仍衝突 → 視為他機已佔位，保守不重複發送
    except Exception as _e:
        print(f"  ⚠️ _claim_alert_firebase異常：{str(_e)[:60]}")
        return None

def _claim_notify_slot(key, today_str, notified, max_slots=2):
    """✅08091324【🔴A】通知額度的【原子佔位】取得。回傳 True＝可發送。
    ★問題背景：買/賣訊號彙整信原本只靠本地 4_notified_today.json 計數去重。
      雲端 GitHub Actions 可能同時有多個 job 在跑（scan／futures-scan），
      各自持有自己的 json，彼此看不見對方 → 同一支股票會被重複寄出。
      漲停追蹤曾因同型問題「一晚重複發3封」，後改 Firebase 原子佔位才解決。
    ★本函式把同樣的機制套用到買/賣訊號，並【保留原本每日2次的額度語意】：
      逐一嘗試佔位 key#1、key#2，任一成功即可發送；兩個都被佔走才靜音。
      （若直接用單一 key，額度會從 2 次變成 1 次，屬未經主帥同意的行為變更。）
    ★Firebase 不可用時（無憑證／讀取失敗，回傳 None）自動退回本地計數後援，
      確保沒有 Firebase 也能運作，不會因此漏發訊號。
    """
    try:
        for _i in range(1, max_slots + 1):
            _c = _claim_alert_firebase(f"{key}#{_i}", today_str)
            if _c is True:
                return True
            if _c is None:                     # Firebase 不可用 → 本地後援
                _lst = notified.setdefault(today_str, [])
                if _lst.count(key) < max_slots:
                    _lst.append(key)
                    return True
                return False
        return False                            # 兩個槽位都已被別的行程佔走
    except Exception as _e:
        print(f"  ⚠️ 原子佔位異常（{str(_e)[:40]}）→ 退回本地計數")
        _lst = notified.setdefault(today_str, [])
        if _lst.count(key) < max_slots:
            _lst.append(key)
            return True
        return False


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


# ✅ (點1-B 06240532) 負荷哨兵：監測5m抓取失敗率與掃描耗時，超門檻寄Gmail警報
LOAD_SENTINEL = True       # 負荷哨兵總開關
LOAD_FAIL_PCT = 0.30       # 5m抓取失敗率門檻（>30%視為yfinance限流/負荷過重）
LOAD_MAX_MIN  = 50         # 單輪掃描耗時門檻（分鐘）✅07071728上修25→50：台股1827支全量掃描正常約45分(首掃建方案②快取)，原25分過低致頻繁誤報；50分僅真正卡住才警報   # ★凍結開關 F-04（鐵律AD，改動前必讀凍結清單）
_load_stats   = {'fetch_total': 0, 'fetch_fail': 0, 'scan_start': None}

def _get_rt_price(ticker, cache=None):
    """✅ (點1) 取個股即時現價（5m最後一根Close），每ticker每輪只抿一次。失敗回None。"""
    if cache is not None and ticker in cache:
        return cache[ticker]
    px = None
    try:
        _rt = _normalize_df(yf.download(ticker, period='1d', interval='5m', progress=False))
        if _rt is not None and len(_rt) > 0:
            px = float(_rt['Close'].iloc[-1])
    except Exception:
        px = None
    if cache is not None:
        cache[ticker] = px
    return px

def _patch_ref_realtime(df, px):
    """✅ (點1-B) 用已收完5m現價覆蓋ref_df最後一根並重算布林/RSI，供訊號顯示即時化（不改根數）。"""
    if df is None or not px or len(df) < 20:
        return df
    try:
        df.iloc[-1, df.columns.get_loc('Close')] = px
        c = df['Close']
        df['ma_c_20'] = c.rolling(20).mean()
        _std20 = c.rolling(20).std()
        df['boll_mid20'] = df['ma_c_20']
        df['boll_top20'] = df['ma_c_20'] + 2 * _std20
        df['boll_bot20'] = df['ma_c_20'] - 2 * _std20
        df['rsi14'] = ta.rsi(c, length=14)
    except Exception:
        pass
    return df

def prefetch_realtime_prices(tickers, label=''):
    """✅ (08061155)【(點1) 即時補更恢復】批次預抓各標的即時現價。
    ・背景：06240532 方案B 為降負荷，把 gate1/2 的即時化整個關閉（_rt_px 恆為 None），
      使月K/週K/日K 第一關實際採用【收盤資料、而非盤中即時價】——盤中訊號因此可能延遲。
      主帥 2026/08/06 決定恢復，但不得重蹈「1827支各抓一次5分K」的負荷覆轍。
    ・做法：改用 yfinance【批次下載】，一次抓一整批（預設100支）當日5分K，
      只取每支最後一根收盤價 → API 呼叫次數由約1827次降為約19次。
    ・容錯：任一批失敗只影響該批（退回非即時），不中斷掃描、不影響訊號發送；
      整體異常亦只是全部退回原本的非即時行為，等同關閉本功能。
    ・REALTIME_LAST_BAR=False 可一鍵停用，完全還原 08060719 行為。
    """
    global _rt_price_map
    _rt_price_map = {}
    if not REALTIME_LAST_BAR or not tickers:
        return
    try:
        _ts = [t for t in tickers if t]
        if not _ts:
            return
        _batches = (len(_ts) + RT_PREFETCH_CHUNK - 1) // RT_PREFETCH_CHUNK
        _ok = 0
        for _bi in range(_batches):
            _batch = _ts[_bi * RT_PREFETCH_CHUNK:(_bi + 1) * RT_PREFETCH_CHUNK]
            try:
                _df = yf.download(_batch, period='1d', interval='5m',
                                  progress=False, group_by='ticker', threads=True)
                if _df is None or len(_df) == 0:
                    continue
                for _t in _batch:
                    try:
                        _sub = _df[_t] if len(_batch) > 1 else _df
                        _c = _sub['Close'].dropna()
                        if len(_c) > 0:
                            _px = float(_c.iloc[-1])
                            if _px > 0:
                                _rt_price_map[_t] = _px
                                _ok += 1
                    except Exception:
                        continue
            except Exception as _e:
                print(f'  ⚠️ 即時價批次 {_bi+1}/{_batches} 失敗（{str(_e)[:40]}）→ 該批退回非即時')
        print(f'  ⚡ {label}即時價預抓：{_ok}/{len(_ts)} 支成功（共 {_batches} 批，取代逐支抓取）')
        _feat('prefetch', f'{label} {_ok}/{len(_ts)} 支成功（{_batches} 批）')
    except Exception as _e:
        print(f'  ⚠️ 即時價預抓整體異常（{str(_e)[:40]}）→ 全部退回非即時（不影響掃描）')


def _patch_last_close(df, px):
    """✅ (點1) 用即時現價覆蓋df最後一根K棒收盤價（不改interval/根數）。"""
    if df is not None and px and len(df) > 0:
        try:
            df.iloc[-1, df.columns.get_loc('Close')] = px
        except Exception:
            pass
    return df

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
    # ✅ 07031936 清理：只留最新7個日期key，避免notified無限長大
    try:
        if isinstance(data, dict) and len(data) > 7:
            _keep = sorted(data.keys())[-7:]
            data = {k: data[k] for k in _keep}
    except Exception:
        pass
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
        # ✅08301905 容忍度改綁通道寬度（取兩制較嚴者）；原式：bb.iloc[-n:] * BUY_BOLL_TOLERANCE
        price_near_lower = (l.iloc[-n:] <= _gate_lower(bt.iloc[-n:], bb.iloc[-n:])).any()
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
        return False, False   # ✅Fix:統一回2元組防crash

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

        # ✅08301905 容忍度改綁通道寬度；原式：float(bt.iloc[-1]) * SELL_BOLL_TOLERANCE
        price_near_upper = float(h.iloc[-1])   >= _gate_upper(float(bt.iloc[-1]), float(bb.iloc[-1]))  # 引用第2-3章
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


def check_cover_condition(df):
    """做空回補獲利了結（check_sell_condition 的鏡像）：近下軌 AND RSI上升 AND MACD柱上升 → 觸底翻揚回補"""
    try:
        l   = df['Low']
        rsi = df['rsi14']
        bb  = df['boll_bot20']
        mh  = df['macd_hist']
        # ✅08301905 容忍度改綁通道寬度；原式：float(bb.iloc[-1]) * COVER_BOLL_TOLERANCE
        price_near_lower = float(l.iloc[-1])   <= _gate_lower(float(bt.iloc[-1]), float(bb.iloc[-1]), COVER_BOLL_TOLERANCE)
        rsi_rising       = float(rsi.iloc[-1]) >  float(rsi.iloc[-2])
        macd_rising      = float(mh.iloc[-1])  >  float(mh.iloc[-2])
        return price_near_lower and rsi_rising and macd_rising
    except:
        return False


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
        # ✅08301905 容忍度改綁通道寬度；原式：bt.iloc[-n:] * SELL_BOLL_TOLERANCE
        price_near_upper = (h.iloc[-n:] >= _gate_upper(bt.iloc[-n:], bb.iloc[-n:])).any()
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

        # 鐡像條件C（大盤強空頭放寬）：近N根任一最高價 >= 布林中軌 AND RSI↓ AND MACD柱↓  # ✅ 07010537
        try:
            high_touch_mid = (h.iloc[-n:] >= bm.iloc[-n:]).any()
            cond_C = high_touch_mid and rsi_falling and macd_falling
        except Exception:
            cond_C = False


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

        _abc_s = cond_A or cond_B or cond_C  # ✅ 07010537 補上 cond_C 鏡像（原只 cond_A or cond_B）
        return _abc_s or cond_D_short, cond_D_short  # (整體通過, 是否由條件D空頭觸發)
    except:
        return False, False   # ✅Fix:統一回2元組

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
def _mis_parse_symbol(sym):
    """由期交所契約代碼解析出（履約價, 買賣權）。解析不出回 (None, None)。
    ★格式：前綴 + 履約價 + 月份字母 + 年份末碼，例如 TXO44000H6、TX144000T6。
      月份字母慣例：A~L＝Call的1~12月；M~X＝Put的1~12月。
      前綴長度不固定（TXO 月選／TX1~TX5 週三選／TXU~TXZ 週五選，部分含數字），
      故【由字串尾端反向錨定】，避免前綴數字與履約價混淆。
    """
    try:
        import re as _re
        # ★前綴固定為3碼（TXO月選／TX1~TX5週三選／TXU~TXZ週五選），必須先錨定前綴，
        #   否則 TX1 的「1」會被併進履約價：TX144500H6 會被誤讀成履約價 144500（實為 44500）。
        #   ★此 bug 由 08090829 的真實契約代碼實測抓出，非事後補述。
        _m = _re.match(r'^([A-Z]{2}[A-Z0-9])(\d{3,6})([A-X])(\d)$',
                       str(sym).strip().upper())
        if not _m:
            return None, None
        _strike = float(_m.group(2))
        _mon = _m.group(3)
        # 合理性防呆：台指履約價為50點整數倍，且落在合理區間；不符即視為解析失敗
        if _strike % 50 != 0 or not (1000 <= _strike <= 200000):
            return None, None
        _is_put = _mon >= 'M'           # A~L=Call、M~X=Put
        return _strike, ('put' if _is_put else 'call')
    except Exception:
        return None, None


def _fetch_option_chain():
    """✅ (08090829) 取得台指選擇權【即時快照】權利金（供履約價≤OPT_PREMIUM_MAX 篩選）。
    來源：期交所行情資訊網 MIS（官方、免費、免 token）——取代 FinMind 付費牆（R-01）。
    ★主帥定案之容錯原則（沿用，未變）：
      ・只抓【一次】，失敗即放棄、回傳 None，由呼叫端降級為距離推估；【不重抓】。
      ・同一分鐘內重用快取，避免重複拉取 2MB 回應。
      ・訊號本身照常發送，取不到報價【絕不】影響通知。
    ★回傳格式刻意沿用舊有欄位名（strike_price／call_put／close／contract_date），
      使 _pick_strikes_by_premium() 完全不必修改（降低改動風險）。
    """
    global _opt_chain_cache
    try:
        import time as _t
        if _opt_chain_cache.get('rows') is not None and (_t.time() - _opt_chain_cache.get('ts', 0)) < OPT_CHAIN_CACHE_SEC:
            return _opt_chain_cache['rows']
        import requests as _req
        _payload = {"MarketType": "0", "SymbolType": "O", "KindID": "1", "CID": "TXO",
                    "ExpireMonth": "", "RowSize": "全部", "PageNo": "",
                    "SortColumn": "", "AscDesc": "A"}
        _r = _req.post(OPT_MIS_URL, json=_payload, timeout=OPT_MIS_TIMEOUT,
                       headers={'Referer': OPT_MIS_REFERER,
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        if _r.status_code != 200:
            print(f"  ⚠️ 期交所選擇權報價 HTTP {_r.status_code} → 降級為距離推估（不重抓）")
            return None
        _ql = ((_r.json() or {}).get('RtData') or {}).get('QuoteList') or []
        if not _ql:
            print("  ⚠️ 期交所選擇權報價回傳空清單 → 降級為距離推估（不重抓）")
            return None

        # ★自動回報：第一次取得時把原始欄位名印進 log，日後對方改欄位可立即看出
        if not _opt_chain_cache.get('logged_keys'):
            try:
                print(f"  ℹ️ MIS 原始欄位名（僅首次回報）：{sorted(list(_ql[0].keys()))[:18]}")
            except Exception:
                pass

        # ── 正規化為舊有欄位名，並過濾掉無法解析／無成交價者 ──
        _rows, _bad = [], 0
        for _q in _ql:
            if not isinstance(_q, dict):
                continue
            _sym = _q.get('SymbolID') or _q.get('Symbol') or _q.get('ProdID') or ''
            _strike, _cp = _mis_parse_symbol(_sym)
            if _strike is None:
                # ★備援：欄位名若與預期不同，掃描本筆所有字串值找出契約代碼。
                #   理由：主帥實測只證實 DispCName/CLastPrice 存在，代碼欄位名【未經實測】，
                #   寫死單一欄位名風險過高（鐵律R-05：不可用臆測當成已驗證）。
                for _v in _q.values():
                    if isinstance(_v, str) and len(_v) <= 20 and _v[:2].upper() == 'TX':
                        _strike, _cp = _mis_parse_symbol(_v)
                        if _strike is not None:
                            break
            if _strike is None:
                _bad += 1
                continue
            try:
                _px = float(str(_q.get('CLastPrice', '')).replace(',', '').strip())
            except Exception:
                continue
            if _px <= 0:
                continue                       # 無成交價＝無流動性，排除
            _rows.append({'strike_price': _strike, 'call_put': _cp, 'close': _px,
                          'contract_date': str(_q.get('DispCName', '')).strip()})
        _opt_chain_cache = {'ts': _t.time(), 'rows': _rows,
                            'logged_keys': True}
        if not _rows:
            print(f"  ⚠️ 期交所選擇權：取得 {len(_ql)} 檔但【0 檔可解析】→ 降級為距離推估。"
                  f"　★請把上面那行「MIS 原始欄位名」回報給AI，以校正代碼欄位。")
        else:
            _feat('opt_chain', f'{len(_ql)} 檔契約 → 可用 {len(_rows)} 檔')
            print(f"  ✅ 期交所選擇權即時報價：{len(_ql)} 檔契約 → 可用 {len(_rows)} 檔"
                  f"（無法解析代碼 {_bad} 檔已略過）")
        return _rows
    except Exception as _e:
        print(f"  ⚠️ 期交所選擇權報價失敗（{str(_e)[:40]}）→ 降級為距離推估（不重抓）")
        return None


def _pick_strikes_by_premium(rows, want_put, spot):
    """✅ (08032126) 從快照挑出【價外 且 權利金≤OPT_PREMIUM_MAX】的履約價。
    回傳 [(履約價, 權利金, 契約代碼)]，最多3檔。
    ・價外定義：PUT 履約價 < 現價；CALL 履約價 > 現價（以小博大，價外才便宜）。
    ・排序：權利金由高至低＝【最接近價平者優先】，結算落入價內機率較高。
      7/17 實證：43000P 進場約12元→結算71元(約6倍)；43100P 進場18.5元已>16 故不符。
    ・同一履約價若有多個到期契約，取權利金最低者（＝最近到期、時間價值最少）。
    ・防禦式解析：FinMind 欄位名若異動，取不到即回空清單→自動降級，不會拋錯。
    """
    try:
        _S = ('strike_price', 'strike', 'StrikePrice')
        _T = ('call_put', 'type', 'CallPut', 'option_type')
        _P = ('close', 'price', 'last_price', 'deal_price', 'LastPrice')
        _C = ('contract_date', 'due_date', 'option_id', 'data_id', 'contract')
        _tmp = []
        for _r in (rows or []):
            if not isinstance(_r, dict):
                continue
            _sv = next((_r[k] for k in _S if _r.get(k) is not None), None)
            _tv = next((_r[k] for k in _T if _r.get(k) is not None), None)
            _pv = next((_r[k] for k in _P if _r.get(k) is not None), None)
            _cv = next((_r[k] for k in _C if _r.get(k) is not None), '')
            if _sv is None or _tv is None or _pv is None:
                continue
            _is_put = str(_tv).strip().lower().startswith('p')
            if _is_put != bool(want_put):
                continue
            _s = float(_sv); _p = float(_pv)
            if (_is_put and _s >= spot) or ((not _is_put) and _s <= spot):
                continue                      # 只要價外
            if not (0 < _p <= OPT_PREMIUM_MAX):
                continue                      # 只要 ≤16 元
            _tmp.append((_s, _p, str(_cv)))
        _best = {}
        for _s, _p, _c in _tmp:
            if _s not in _best or _p < _best[_s][1]:
                _best[_s] = (_s, _p, _c)
        return sorted(_best.values(), key=lambda x: -x[1])[:3]
    except Exception as _e:
        print(f"  ⚠️ 選擇權鏈解析失敗（{str(_e)[:40]}）→ 降級為距離推估")
        return []


# ✅08160731【F-15】台股現貨指數標的「日盤時段守門」
# ┌─ 決策註記：為何需要這道防護（凍結清單條目 F-15）──────────────────────
# │ 真實事故：2026/08/11(週二) 13:38 發出「期貨15分K買進」、13:43 發出
# │   「期貨15分K做空」——★同一根15分K(13:30-13:45)、方向相反、相隔5分鐘。
# │   兩封信的「前一根」數值完全相同(RSI 64.1／MACD +13.76)，
# │   只有「當根」不同(收盤 45190.86→45138.36、RSI 64.5↑→60.7↓)，
# │   證明讀的是【同一根未完成K棒】，且標的 ^TWII 在 13:30 已收盤，
# │   價格變動來自 yfinance 對最後一根的【回填修正】，不是真實行情。
# │ ★為何既有防護擋不住：F-07 FUT_BAR_MAX_AGE_MIN=15 判定「陳舊」，
# │   但 13:30~13:45 這段的K棒年齡只有 0~15 分鐘，【剛好躲過】該門檻。
# │   夜盤(年齡數小時)本來就被 F-07 擋住，故本守門【不改變夜盤行為】，
# │   淨效果＝只補上 13:30~13:45 這個盲區。
# │ ★另一個矛盾：_opt_hint_if_window 的守門是 09:05~13:30，13:30後回傳空字串，
# │   所以那兩封信【沒有履約價建議】——履約價模組知道收盤了，
# │   發信模組卻不知道。兩個閘門的時間判斷不一致（鐵律AF4 的同型問題）。
# │ ★適用範圍：僅限【台股現貨指數】類標的（^TW 開頭，如 ^TWII）。
# │   若日後 FUTURES_5MK_TARGETS 加入真正的期貨代碼(如 TXFF)，
# │   該標的夜盤有真實行情，【不受本守門限制】。
# └──────────────────────────────────────────────────────────────
TW_SPOT_SESSION_GUARD = True        # ★凍結開關 F-15：關閉將使 13:30~13:45 假訊號重現
TW_SPOT_SESSION_START = 9 * 60      # 09:00（台股現貨開盤）
TW_SPOT_SESSION_END   = 13 * 60 + 30  # 13:30（台股現貨收盤）★與 _opt_hint_if_window 對齊


def _tw_spot_session_ok(ticker):
    """台股現貨指數標的是否處於「有真實行情」的時段。
    ・回傳 True  ＝ 可正常判斷訊號
    ・回傳 False ＝ 該標的此刻沒有真實行情，呼叫端必須 continue（不得發信）
    ★只約束 ^TW 開頭的現貨指數；其他標的一律放行，不影響未來接入真期貨。
    """
    if not TW_SPOT_SESSION_GUARD:
        return True
    try:
        if not str(ticker).upper().startswith('^TW'):
            return True                      # 非台股現貨指數（如 TXFF）→ 不受限
        _n = datetime.now(pytz.timezone('Asia/Taipei'))
        if _n.weekday() >= 5:                # 週六日無盤
            return False
        _m = _n.hour * 60 + _n.minute
        return TW_SPOT_SESSION_START <= _m <= TW_SPOT_SESSION_END
    except Exception:
        return True                          # 判斷失敗時不擋，避免守門本身變成故障點


def _opt_hint_if_window(current_price, signal_type):
    """✅ (08032126) 週選擇權推薦【時窗守門】——取代原分散於各訊號點的重複判斷。
    ・涵蓋週二~週五：週三選(週三結算)、週五選(週五結算) 及其【前一日】(週二/週四)。
    ・日盤 09:05 ~ 13:30。★起始維持 09:05、嚴禁改 09:00：
      剛開盤成交量暴增易造成報價 lag，此為主帥當初指定，不得擅改。
    ・原設定僅 (週三,週五) 09:05~10:45 → 2026/07/17(週五) 中午大跌3000點時
      訊號落在時窗外而漏發，本次擴展即為修正此缺口。
    """
    try:
        _n = datetime.now(pytz.timezone('Asia/Taipei'))
        if _n.weekday() not in (1, 2, 3, 4):      # 1=週二 2=週三 3=週四 4=週五
            return ""
        _m = _n.hour * 60 + _n.minute
        if not (9 * 60 + 5 <= _m <= 13 * 60 + 30):
            return ""
        return get_weekly_option_hint(current_price, signal_type)
    except Exception:
        return ""


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

        # 到期日說明（✅08032126：補齊週二/週四＝結算日前一日）
        if _wd == 2:
            _expiry = f"本週三（{_date_str}）到期週選擇權（週三選）"
        elif _wd == 4:
            _expiry = f"本週五（{_date_str}）到期週選擇權（週五選）"
        elif _wd == 1:
            _expiry = f"明日週三到期週選擇權（今日{_date_str}為結算前一日）"
        elif _wd == 3:
            _expiry = f"明日週五到期週選擇權（今日{_date_str}為結算前一日）"
        else:
            _expiry = f"當日（{_date_str}）週選擇權"

        _is_put    = (signal_type == 'sell')
        _opt_type  = 'PUT' if _is_put else 'CALL'
        _direction = '做空（buy PUT）' if _is_put else '做多（buy CALL）'

        # ── ✅(08032126) 優先以【實際權利金 ≤16元】篩選（以小博大）──
        _rows  = _fetch_option_chain()
        _picks = _pick_strikes_by_premium(_rows, _is_put, current_price) if _rows else []

        _hint = (
            f"\n\n{'='*40}\n"
            f"📋 週選擇權建議（{_direction}）\n"
            f"{'='*40}\n"
            f"到期日：{_expiry}\n"
            f"台指現價：{current_price:.0f}\n"
            f"建議標的：{_opt_type}（價外，以小博大）\n"
        )
        if _picks:
            _hint += (f"\n★依【期交所即時權利金】篩選（上限 {OPT_PREMIUM_MAX} 元）：\n")
            for _s, _p, _c in _picks:
                _hint += f"  → {int(_s)} {_opt_type}　權利金 {_p:g} 元" + (f"　[{_c}]" if _c else "") + "\n"
            _hint += "（排序：最接近價平者優先＝結算落入價內機率較高）\n"
        else:
            _atm = round(current_price / 50) * 50
            _strikes = ([_atm - 50, _atm - 100, _atm - 150] if _is_put
                        else [_atm + 50, _atm + 100, _atm + 150])
            _hint += (f"\n⚠️【未取得即時權利金報價】以下為【距離推估】，\n"
                      f"　　尚【未驗證】是否 ≤{OPT_PREMIUM_MAX} 元，下單前請自行確認市價：\n")
            for _s in _strikes:
                _hint += f"  → {int(_s)} {_opt_type}（價外，權利金待確認）\n"
        _hint += (
            f"\n⚠️ 進場原則（主帥定案）：\n"
            f"  ✅ 進場權利金 ≤ {OPT_PREMIUM_MAX} 元；超過即不進場（歸零時損失過大）\n"
            f"  ✅ 取價外選項，以小博大（便宜、行情到位可暴賺數十倍）\n"
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
        # ✅ (點1) 個股即時現價：盤中用５m最後一根，補更月K/週K/日K最後一根收盤
        # ✅08061155【(點1) 即時補更恢復】改由批次預抓表取值（零額外API呼叫）：
        #   06240532 曾因負荷而恆設 None，使第一關實際使用收盤價而非盤中即時價。
        _rt_px = _rt_price_map.get(ticker)
        df_w = get_stock_data(ticker, period='5y', interval='1mo', cache=weekly_cache)  # ✅ v05170856 長期投資改月K
        if df_w is None or len(df_w) < 30: return None
        _patch_last_close(df_w, _rt_px)   # ✅ (點1) 月K最後一根即時補更
        df_w = calc_indicators(df_w)   # ✅ 07010000 月K根治：改算全指標(macd/ma_c_20)，復活月K長期投資買進/做空/賣出第一道
        if df_w is None or len(df_w) < 30: return None
        if 'boll_mid20' not in df_w.columns: df_w['boll_mid20'] = df_w['ma_c_20']

        # 🔴 [優先處理賣出]
        if is_holding:
            _is_short_hold = (ticker in HOLDINGS_SHORT) or (ticker.split('.')[0] in HOLDINGS_SHORT)  # ✅ 07010514 空單判別
            _df_mon = calc_indicators(df_w)
            _df_wk  = get_stock_data(ticker, period='2y', interval='1wk', cache=daily_cache)
            _df_wk  = calc_indicators(_df_wk) if (_df_wk is not None and len(_df_wk) >= 26) else None
            _df_day = get_stock_data(ticker, period='1y', interval='1d', cache=daily_cache)
            _df_day = calc_indicators(_df_day) if (_df_day is not None and len(_df_day) >= 26) else None
            for _lbl, _edf in [('月K', _df_mon), ('週K', _df_wk), ('日K', _df_day)]:
                if _edf is None or len(_edf) < 26:
                    continue
                if _is_short_hold:
                    # ✅ 做空回補（月/週/日任一），回補取下軌/最低價
                    _cD, _cMsg = check_cover_condD(_edf)
                    if _cD:
                        c_price = float(_edf['Close'].iloc[-1])
                        print(f'  🔔 {ticker} [{_lbl}] {_cMsg}')
                        return ('COVER', c_price, float(_edf['Low'].iloc[-1]), float(_edf['boll_bot20'].iloc[-1]),
                                float(_edf['rsi14'].iloc[-1]), float(_edf['rsi14'].iloc[-2]))
                    if check_cover_condition(_edf):
                        c_price = float(_edf['Close'].iloc[-1])
                        print(f'  🔔 {ticker} [{_lbl}] 做空回補/觸底翻揚觸發')
                        return ('COVER', c_price, float(_edf['Low'].iloc[-1]), float(_edf['boll_bot20'].iloc[-1]),
                                float(_edf['rsi14'].iloc[-1]), float(_edf['rsi14'].iloc[-2]))
                else:
                    _dD_exit, _dD_msg = check_sell_condD(_edf)
                    if _dD_exit:
                        c_price = float(_edf['Close'].iloc[-1])
                        print(f'  🔔 {ticker} [{_lbl}] {_dD_msg}')
                        return ('SELL', c_price, float(_edf['High'].iloc[-1]), float(_edf['boll_top20'].iloc[-1]),
                                float(_edf['rsi14'].iloc[-1]), float(_edf['rsi14'].iloc[-2]))
                    if check_sell_condition(_edf):
                        c_price = float(_edf['Close'].iloc[-1])
                        print(f'  🔔 {ticker} [{_lbl}] 獲利了結/反轉賣出觸發')
                        return ('SELL', c_price, float(_edf['High'].iloc[-1]), float(_edf['boll_top20'].iloc[-1]),
                                float(_edf['rsi14'].iloc[-1]), float(_edf['rsi14'].iloc[-2]))
            return None

        # ── 第一道：週K/日K統一用3根（條件A or B）────────────────
        # weekly：用週K 3根
        # daily ：用日K 3根（與週K根數相同，確認當下位階即可）
        if not TEST_MODE:
            if SCAN_MODE == 'daily':
                # 日K模式第一道：日K 3根，條件A or B
                _df1st = get_stock_data(ticker, period='2y', interval='1wk', cache=daily_cache)  # ✅ v05170856 中期投資改週K
                if _df1st is None or len(_df1st) < 50: return None
                _patch_last_close(_df1st, _rt_px)   # ✅ (點1) 週K最後一根即時補更
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
        _patch_last_close(df_d, _rt_px)   # ✅ (點1) 週K(參考)最後一根即時補更
        df_d = calc_indicators(df_d)
        if df_d is None: return None
        # ✅ v05191855：第二道改用日K（A/B/C/E OR F，符合先大後小原則）
        _df_1d = get_stock_data(ticker, period='1y', interval='1d', cache=daily_cache)
        _patch_last_close(_df_1d, _rt_px)   # ✅ (點1) 日K最後一根即時補更
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
            if LOAD_SENTINEL: _load_stats['fetch_total'] += 1
            df_5m = _normalize_df(yf.download(ticker, period='5d', interval='5m', progress=False))
            if LOAD_SENTINEL and (df_5m is None or getattr(df_5m, 'empty', True)): _load_stats['fetch_fail'] += 1
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
        cond_5m_buy     = check_buy_precondition(df_5m)[0]    # 買進：近3根5分K條件A or B
        cond_5m_short   = check_short_precondition(df_5m)[0]  # 做空：近3根5分K鏡像條件A or B

        if _is_long_ok:
            # ── 多頭第三道：（RSI↑ AND MACD↑）AND（5分K買進條件A or B）
            if not (rsi_5m_ok and macd_5m_ok and cond_5m_buy):
                return None
            c = float(df_5m['Close'].iloc[-1])
            ref_df = df_d if SCAN_MODE == 'daily' else df_w
            if REALTIME_LAST_BAR and df_5m is not None and len(df_5m) >= 2:
                _patch_ref_realtime(ref_df, float(df_5m['Close'].iloc[-2]))   # ✅(點1-B)穩定化即時化:用已收完前一根5m
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
            if REALTIME_LAST_BAR and df_5m is not None and len(df_5m) >= 2:
                _patch_ref_realtime(ref_df, float(df_5m['Close'].iloc[-2]))   # ✅(點1-B)穩定化即時化:用已收完前一根5m
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
    台股收盤後顯示，若當日^TWII漲跌超過750點 → Gmail通知
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
        if _twii is None or len(_twii) < 2: return   # ✅ v06160529止血：calc_indicators回None時防NoneType subscript
        _cur  = _safe_float(_twii['Close'].iloc[-1])
        _prev = _safe_float(_twii['Close'].iloc[-2])
        if _prev <= 0: return
        _chg_pts = _cur - _prev
        _chg_pct = _chg_pts / _prev * 100

        print(f"  📊 台股大盤今日：{_cur:.0f}（前收{_prev:.0f}，漲跌{_chg_pts:+.0f}點，{_chg_pct:+.2f}%）")
        if abs(_chg_pts) < 750: return  # ✅ 07031936 台1000點下修至750點(更早警覺,同夜盤幅度)
        # ✅08092144【主帥指示關閉】收盤確認信
        #   主帥原話：「我的設計理念是【收到通知信就3分鐘內迅速進場下單】。
        #   所以根本不需要【收盤確認】，請記得把它關掉。」
        #   ★收盤後才發的信，行情已經結束，無法據以進場，屬「事後告知」＝狼來了預備軍。
        #   ★盤中即時版 check_tw_intraday_extreme() 已完全涵蓋此需求。
        #   ★保留 log 輸出供日後對照，只是不發信。
        if not TW_CLOSE_CONFIRM_ENABLED:
            print("  🔕 台股收盤確認信已依主帥指示關閉（盤中即時版已涵蓋）")
            return

        _direction = 'DOWN' if _chg_pts < 0 else 'UP'
        _alert_key = f"TWII_DAYTIME_{_direction}_{_today_str}"
        _today_notified = notified.get(_today_str, [])
        if _today_notified.count(_alert_key) >= 2:
            print(f"  🔕 台股白天極端異動今日已通知（{_direction}），跳過")
            return

        _emoji = "🔻" if _direction == 'DOWN' else "🚀"
        _action = "暴跌！考慮台股期貨做空或buy put" if _direction == 'DOWN' else "急漲！考慮台股期貨做多或buy call"
        _arr = '↘' if _direction == 'DOWN' else '↗'
        # ✅08092108 與盤中即時版區隔：本封為收盤後確認，主旨加註「收盤確認」
        _subject = f'☁️【雲端】{_emoji}【收盤確認】台股白天極端異動！{_arr}{int(abs(_chg_pts))}點({_chg_pct:+.1f}%)'
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

        # 閾值：750點（≈ 1.67%）✅ 07030953 由 2.22%(1000點) 下修至 1.5~1.8% 區間，更早響
        _threshold_pts = 750
        _threshold_pct = _threshold_pts / _twii_base * 100  # ≈ 1.67%

        if abs(_chg_pct) < _threshold_pct:
            return  # 未達閾值，不通知

        # 確認今日此方向是否已通知過
        _direction = 'DOWN' if _chg_pct < 0 else 'UP'
        _alert_key = f"TWII_EXTREME_{_direction}_{_today_str}"

        # ✅ v06131103：Firebase原子佔位（樂觀並行控制，跨多台機器一勞永逸防重複）
        _claim = False   # ✅ 07040032 max-2：2-slot原子認領(同標的同方向每日最多2次)
        for _slot in (1, 2):
            _c = _claim_alert_firebase(f"{_alert_key}#{_slot}", _today_str)
            if _c is True or _c is None:
                _claim = _c; break
        if _claim is False:
            print(f"  🔕 台指夜盤極端異動已被其他機器佔位/今日已通知（{_direction}），跳過")
            return
        if _claim is None:
            # 後援：Firebase不可用時退回本地檢查（行為同舊版，至少不漏報）
            try:
                _fb_reload = load_notified_firebase()
                if _fb_reload: notified.update(_fb_reload)
            except: pass
            _today_notified = notified.get(_today_str, [])
            if _today_notified.count(_alert_key) >= 2:
                print(f"  🔕 台指夜盤極端異動今日已通知過（{_direction}），跳過")
                return
        # _claim is True → 已成功原子佔位，續發送

        # 發送警報
        _emoji = "🔻" if _direction == 'DOWN' else "🚀"
        _action = "暴跌！考慮買進Put選擇權" if _direction == 'DOWN' else "急漲！考慮買進Call選擇權"
        _arr = '↘' if _direction=='DOWN' else '↗'
        _subject = f'☁️【雲端】{_emoji}台指夜盤極端異動！估計{_arr}{int(_est_points)}點({_chg_pct:+.1f}%)'
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

        # 記錄已通知（記憶體同步；佔位成功時Firebase已寫入，僅後援模式才寫回）
        if _today_str not in notified:
            notified[_today_str] = []
        if notified[_today_str].count(_alert_key) < 2:
            notified[_today_str].append(_alert_key)
        if _claim is None:
            save_notified(notified)  # 僅後援模式需寫回，避免重複patch Firebase
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

        # ✅08061155【主帥指定】上限由50支改為 LIMIT_UP_MAX_SCAN(=20)，並改為【大型權值股優先】：
        #   預篩清單原本沒有市值排序，直接取前50支等於隨機取樣；
        #   改以 TW_LARGE_CAP_PRIORITY（0050成分股近似清單）排序，大型股先掃、其餘遞補。
        _pri = {c: i for i, c in enumerate(TW_LARGE_CAP_PRIORITY)}
        _tw_codes = sorted(_tw_codes, key=lambda t: _pri.get(str(t).split('.')[0], 9999))
        for _ticker in _tw_codes[:LIMIT_UP_MAX_SCAN]:  # 主帥指定先設20支觀察負荷
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
        # ✅ (08031611)【去重升級】原「讀→數→發→寫」非原子：並行/延遲的雲端run會同時
        #    讀到count=0而各自發信 → 一晚重複發3封。改用與夜盤極端/條件W【相同】的
        #    Firebase 原子佔位機制，且上限由每日2次改為【每日最多1次】(主帥指定)。
        _notif_key = f"LIMIT_UP_{_today_str}"
        _claim = _claim_alert_firebase(_notif_key, _today_str)
        if _claim is False:
            print("  🔕 今日漲停追蹤已被其他機器佔位／已通知過，跳過")
            return
        if _claim is None:
            # 後援：Firebase不可用時退回本地紀錄檢查（每日最多1次）
            if notified.get(_today_str, []).count(_notif_key) >= 1:
                print("  🔕 今日漲停追蹤已通知過，跳過")
                return

        _lines = [f"📈 今日漲停股票買進候選（{_now.strftime('%Y/%m/%d')}）", '='*35]
        for _t, _c, _p, _per, _dk in _buy_candidates:
            _dk_str = '日K✅' if _dk else '日K尚未符合'
            _lines.append(f"  {_t}  漲停+{_c:.1f}%  收{_p:.2f}  {_per}買進訊號  {_dk_str}")
        _lines += ['='*35,
                   '⚠️ 漲停股需確認隔日開盤是否繼續，請謹慎進場',
                   '⚠️ 嚴禁用於當沖或隔日沖']

        _subject = f"☁️【雲端】📈漲停追蹤：{len(_buy_candidates)}支符合月K/週K買進條件"
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
        if df_w is None or not check_buy_precondition(df_w, is_weekly=True)[0]:
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
            _today_f  = _now_tw().strftime("%Y-%m-%d")   # ✅08060719 時區修正（同漲停追蹤那型）
            if _today_f not in notified: notified[_today_f] = []
            if notified[_today_f].count(_fund_key) >= 2:
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
                send_gmail(f"☁️【雲端】🔔【{_get_period_label("月K")}】基金買進訊號：{fund_name}", msg_body)
                print(f"✅ {fund_name} 已發送觸發買進訊號，已加入今日彙整清單！")
        else:
            print(f"ℹ️ {fund_name}：目前尚未共振達標。")

    except Exception as e:
        print(f"❌ scan_synthetic_fund 異常：{e}")
# ============================================================
# 【１２．防止觸發條件時反覆收到gmail通知】
# ============================================================
notified = load_notified()
today = _now_tw().strftime("%Y-%m-%d")   # ✅08060719【高風險修正】notified去重主鍵，原在雲端取UTC日期

if today not in notified:
    notified[today] = []  # ✅ 05101039修正：只新增今天的key，不覆蓋整個字典

# ✅ v05231322：跨午夜保護（00:00-06:00視為前一個交易日，避免重複通知）
_cur_hour = _now_tw().hour   # ✅08060719【極高風險修正】原取UTC小時：UTC00-06＝台灣08:00-14:00(台股交易時段)，
                             #   會在早盤誤啟動『跨午夜保護』把昨日通知記錄併入今日，使當天真訊號被判『已通知過』而靜音
if _cur_hour < 6:
    from datetime import timedelta as _tdelta
    _prev_day = (_now_tw()-_tdelta(days=1)).strftime("%Y-%m-%d")   # ✅08060719 時區修正
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
        _date = _now_tw().strftime('%Y%m%d_%H%M')   # ✅08060719 時區修正
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
            _date_only = _now_tw().strftime('%Y%m%d')  # 日期不含時間 ✅08060719 時區修正
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
        print(f"\n📄 當日CSV：{'scan_buy_'+_now_tw().strftime('%Y%m%d')+'.csv' if _buy else '（無買進訊號）'}")
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
    now=_dt.now()  # ⏱️elapsed快取TTL用，非日期/星期判斷 → 時區無關（08090225 依自檢(5)逐筆確認）
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
    now = _dt.now()  # ⏱️elapsed快取TTL用，非日期/星期判斷 → 時區無關（08090225 依自檢(5)逐筆確認）
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
    _ck=f'tdcc_{stock_id}'; now=_dt.now()  # ⏱️elapsed快取TTL用，非日期/星期判斷 → 時區無關（08090225 依自檢(5)逐筆確認）
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
                
                ok = send_gmail(f"📱【網頁版】{signal} {ticker} - {time_str}", msg, urgent=True)
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


def _condw_current_window():
    """條件W時間窗判定：回傳當前所屬窗ID；不在窗內回None。✅ 07011049 純新增。
    窗一：週二15:05 ~ 週三11:30；窗二：週四15:05 ~ 週五11:30（跨夜，含夜盤）。"""
    from datetime import timedelta as _td
    tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(tz)
    wd = now.weekday()               # 0=週一 … 4=週五
    tv = now.hour * 60 + now.minute
    d  = now.strftime('%Y%m%d')
    # 窗一：週二(1)15:05起
    if wd == 1 and tv >= 15*60+5:
        return f'{d}_W2'
    # 窗一延續：週三(2)11:30止（窗ID用前一日週二）
    # ✅09021415 由11:00延伸至11:30（主帥 2026/09/02 14:00 明示新指令）
    #   ★★★AI 於 09021330 曾擅自改成 13:30，★主帥【未授權】，★★已回退並具名認錯。
    #   ★沿革：10:45（06/30 主帥定案）→ 11:00（08/09 主帥指定，R-12）→ 11:30（09/02 主帥指定）
    if wd == 2 and tv <= 11*60+30:
        return f"{(now - _td(days=1)).strftime('%Y%m%d')}_W2"
    # 窗二：週四(3)15:05起
    if wd == 3 and tv >= 15*60+5:
        return f'{d}_W4'
    # 窗二延續：週五(4)11:30止（窗ID用前一日週四）
    # ✅09021415 與週三完全對稱（★結算日一對，★★邊界值嚴禁不同．自檢第(22)項機器擋）
    if wd == 4 and tv <= 11*60+30:
        return f"{(now - _td(days=1)).strftime('%Y%m%d')}_W4"
    return None



# ══════════════════════════════════════════════════════════════════════
# ✅09022055【持倉狀態跨執行保存】主帥 2026/09/02 15:02 質問所引出的根本問題
# ----------------------------------------------------------------------
#   ★主帥原話：「我在09:05~11:30建倉，難道11:31後到13:45，就不再觸發出場
#     （平倉）訊號通知了嗎？那這樣與放著等歸零（等破產），有什麼不同！！！」
#
#   ★★★查證結果比主帥想的更嚴重：
#     ・`_futures_is_holding` 原本只是【模組層級變數】（第225行），
#       ★每次 GitHub Actions 啟動都從 False 重新開始。
#     ・★★雲端是 cron 每5分鐘【一次性執行】，行程結束狀態就消失。
#     ・★★★所以「有持倉就繼續掃平倉」這個保護，★在雲端版【從來沒有生效過】，
#       ★它只在本機版同一個行程的 while 迴圈內有意義。
#     ・★我在 09021415 說「這個保護本來就存在，被我限縮了」——★★那句話只對一半，
#       ★★★正確說法是：它在雲端版本來就形同虛設，★而我又把它進一步縮小。
#
#   ★解法：把持倉狀態寫進 Firestore（與 futures_status 同一個 public 路徑家族），
#     ★★每次執行開頭先讀回來，★★★這樣跨行程、跨班次都記得住。
#   ★權限：public 路徑的 read 為 `allow read: if true`（W-2 規則，09/01 已部署），
#     ★★故讀取【不需憑證】；★寫入需要 service account 憑證。
# ══════════════════════════════════════════════════════════════════════
FUTURES_POS_PERSIST = True    # 持倉狀態跨執行保存總開關（凍結清單 F-19）

def _load_futures_position():
    """讀回持倉狀態。★不需憑證（public read）。★★失敗一律回 (False, False)，
    ★★★寧可漏掃平倉也不可憑空捏造一個不存在的持倉（會發出假平倉訊號）。"""
    global _futures_is_holding, _futures_is_short
    if not FUTURES_POS_PERSIST:
        return (False, False)
    try:
        import requests as _req
        _url = (f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
                f"/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}"
                f"/public/futures_position")
        _r = _req.get(_url, timeout=10)
        if _r.status_code != 200:
            print(f"  ℹ️ 持倉狀態讀取：無紀錄或無法讀取（HTTP {_r.status_code}），視為空手")
            return (False, False)
        _f = _r.json().get('fields', {})
        _long  = _f.get('is_long',  {}).get('booleanValue', False) is True
        _short = _f.get('is_short', {}).get('booleanValue', False) is True
        _at    = _f.get('updated_at', {}).get('stringValue', '?')
        _futures_is_holding = _long
        _futures_is_short   = _short
        print(f"  📥 持倉狀態已還原：多倉={_long} 空倉={_short}（更新於 {_at}）")
        return (_long, _short)
    except Exception as _e:
        print(f"  ⚠️ 持倉狀態讀取失敗（視為空手）：{str(_e)[:60]}")
        return (False, False)

def _save_futures_position(is_long, is_short, note=''):
    """寫入持倉狀態（需憑證）。★每一次改變 _futures_is_holding/_short 都必須同步呼叫。"""
    if not FUTURES_POS_PERSIST:
        return False
    try:
        import json, os, requests as _req
        from datetime import datetime as _dt
        import pytz as _tz
        cred_json = os.environ.get(FIREBASE_CRED_ENV)
        if not cred_json:
            _cf = os.path.join(os.path.dirname(os.path.abspath(__file__)), FIREBASE_CRED_FILE)
            if os.path.exists(_cf):
                with open(_cf, 'r', encoding='utf-8') as f:
                    cred_json = f.read()
        if not cred_json:
            print("  ⚠️ 持倉狀態寫入略過：Firebase 憑證未設定")
            return False
        import google.oauth2.service_account as _sa, google.auth.transport.requests as _gtr
        _c = _sa.Credentials.from_service_account_info(json.loads(cred_json),
             scopes=['https://www.googleapis.com/auth/datastore'])
        _c.refresh(_gtr.Request())
        _now = _dt.now(_tz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M')
        _url = (f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
                f"/databases/(default)/documents/artifacts/{FIREBASE_PROJECT_ID}"
                f"/public/futures_position")
        _r = _req.patch(_url, timeout=15,
            headers={"Authorization": f"Bearer {_c.token}", "Content-Type": "application/json"},
            json={"fields": {
                "is_long":    {"booleanValue": bool(is_long)},
                "is_short":   {"booleanValue": bool(is_short)},
                "note":       {"stringValue": str(note)[:120]},
                "updated_at": {"stringValue": _now},
                "source":     {"stringValue": "python"}
            }})
        _ok = _r.status_code in (200, 201)
        print(f"  {'📤' if _ok else '⚠️'} 持倉狀態已寫入：多倉={is_long} 空倉={is_short}"
              f"{'' if _ok else f'（失敗 HTTP {_r.status_code}）'}")
        return _ok
    except Exception as _e:
        print(f"  ⚠️ 持倉狀態寫入失敗：{str(_e)[:60]}")
        return False

def _txf_data_window(wd, tv):
    """★資料源可用時段（★不是台指期真實交易時段）。
    ★★條件W 與期貨5分K 目前抓 ^TWII（加權指數），★只有 09:00~13:30 有新K棒。
    ★★★台指期真實日盤到 13:45、夜盤 15:00~次日05:00，★但 ^TWII 沒有那些資料，
      ★這一段【做不到】，★★需待辦B（台指期即時報價源）解決，★★★不假裝有解。"""
    #   ★起始 09:05 與 cron '5/5 1-5 * * 1-5' 對齊（R-02：★不可改 09:00）
    return (0 <= wd <= 4) and ((9*60+5) <= tv <= (13*60+30))



def _futures_close_alert(now_tw):
    """✅09022155【期貨未平倉·收盤前提醒信】主帥 2026/09/02 21:20 指令。
    ★★★本函式【不需要任何行情資料】——★只看「時間」與「持倉狀態」，
      ★★所以不受 ^TWII 只到 13:30 的限制，★★★可涵蓋到台指期日盤收盤 13:45。
    ★每日最多一封（Firebase 認領槽防重複）。★★空手則不寄（ＡＭ１：不觸發不寄）。"""
    if not FUTURES_CLOSE_ALERT_ENABLED:
        return
    _wd = now_tw.weekday()
    _tv = now_tw.hour * 60 + now_tw.minute
    if not (0 <= _wd <= 4):
        return
    if not (FUTURES_CLOSE_ALERT_START <= _tv <= FUTURES_CLOSE_ALERT_END):
        return
    if not (_futures_is_holding or _futures_is_short):
        return   # ★空手 → 不寄（★嚴禁每日固定發信，狼來了效應）
    _today = now_tw.strftime('%Y-%m-%d')
    _claim = _claim_alert_firebase(f'futures_close_alert_{_today}', _today)
    if _claim is False:
        print("  ⏭️  未平倉提醒信今日已寄過，靜音")
        return
    _dir = '多倉🔴（買進在手）' if _futures_is_holding else '空倉🔵（放空在手）'
    _act = '賣出平倉' if _futures_is_holding else '買進回補'
    _msg = (f"☁️【雲端】⚠️【期貨未平倉·收盤前提醒】⚠️\n"
            f"目前持倉：{_dir}\n"
            f"建議動作：★{_act}\n"
            f"台指期日盤收盤時間：13:45\n"
            f"現在時間：{now_tw.strftime('%Y/%m/%d %H:%M')}\n\n"
            f"★本信只在【仍有未平倉部位】時寄出，★★空手不會收到。\n"
            f"★★若您已在券商端平倉，請忽略本信；\n"
            f"★★★系統的持倉狀態以【本程式發出的進出場訊號】為準，\n"
            f"　　不會自動得知您在元大 eLeader 手動下的單。")
    _ok = send_gmail(f"☁️【雲端】⚠️期貨未平倉提醒 - {now_tw.strftime('%m/%d %H:%M')}",
                     _msg, urgent=True)
    print(f"  {'✅' if _ok else '❌'} 期貨未平倉提醒信{'已發送' if _ok else '發送失敗'}")


def _condw_gate3(df, nbars, label):
    """✅09021330【條件W 第三道關卡】★只跑這一道，★不看第一道、不看第二道。
    ★抽出為共用函式，★★供 5分K 與 15分K 共用（ＡＫ１８：同型邏輯單一實作，
      ★★★避免兩份會漂移的複製品）。
    ★多空雙向：V轉觸底翻揚→buy CALL；★Λ轉觸頂翻落→buy PUT。
    回傳 dict 或 None（資料不足／指標失敗）。"""
    if df is None or df.empty or len(df) < nbars + 2:
        print(f'  \u26a0\ufe0f 條件W：{label} 資料不足（需 {nbars+2} 根），跳過')
        return None
    df = calc_indicators(df)
    if df is None:
        print(f'  \u26a0\ufe0f 條件W：{label} 指標計算失敗，跳過')
        return None
    n  = nbars
    lo = df['Low']; hi = df['High']
    bb = df['boll_bot20']; bt = df['boll_top20']
    bm = df['ma_c_20']; mh = df['macd_hist']; rsi = df['rsi14']
    rsi_now  = float(rsi.iloc[-1]); rsi_prev = float(rsi.iloc[-2])
    mh_now   = float(mh.iloc[-1]);  mh_prev  = float(mh.iloc[-2])
    close    = float(df['Close'].iloc[-1])
    boll_bot = float(bb.iloc[-1]);  boll_top = float(bt.iloc[-1])
    rsi_up = rsi_now > rsi_prev; rsi_dn = rsi_now < rsi_prev
    mac_up = mh_now  > mh_prev;  mac_dn = mh_now  < mh_prev

    # ── 多方：V轉觸底翻揚（條件A／B／E 任一）──
    _cA = (lo.iloc[-n:] <= _gate_lower(bt.iloc[-n:], bb.iloc[-n:])).any() and rsi_up and mac_up
    _cB = ((lo.iloc[-n:] < bm.iloc[-n:]).all() and (hi.iloc[-n:] < bt.iloc[-n:]).all()
           and len(mh) >= n + 1
           and all(float(mh.iloc[-n-1+j]) > float(mh.iloc[-n+j]) for j in range(n-1))
           and mac_up)
    try:
        _cE = bool(check_condE_long(df))
    except Exception:
        _cE = False
    near_lower = close <= _gate_lower(boll_top, boll_bot)
    _buy = ((_cA or _cB or _cE) and rsi_up and mac_up
            and near_lower and rsi_now > BUY_RSI_MIN)

    # ── 空方鏡像：Λ轉觸頂翻落（★R-08 已於 08/19 撤銷，★★做空為主帥要求）──
    _short = False
    if CONDW_ENABLE_SHORT:
        _sA = (hi.iloc[-n:] >= _gate_upper(bt.iloc[-n:], bb.iloc[-n:])).any() and rsi_dn and mac_dn
        _sB = ((hi.iloc[-n:] > bm.iloc[-n:]).all() and (lo.iloc[-n:] > bb.iloc[-n:]).all()
               and len(mh) >= n + 1
               and all(float(mh.iloc[-n-1+j]) < float(mh.iloc[-n+j]) for j in range(n-1))
               and mac_dn)
        try:
            _sE = bool(check_condE_short(df))
        except Exception:
            _sE = False
        near_upper = close >= _gate_upper(boll_top, boll_bot)
        _short = ((_sA or _sB or _sE) and rsi_dn and mac_dn
                  and near_upper and rsi_now < SHORT_RSI_MAX)

    print(f"  \u2139\ufe0f 條件W {label}（{n}根）："
          f"RSI={rsi_prev:.1f}→{rsi_now:.1f}({'\u2191' if rsi_up else '\u2193'})  "
          f"MACD柱={'\u2191' if mac_up else '\u2193'}  "
          f"多方={'\u2713' if _buy else '\u2717'}  空方={'\u2713' if _short else '\u2717'}")
    return {'buy': _buy, 'short': _short, 'close': close, 'label': label,
            'boll_bot': boll_bot, 'boll_top': boll_top,
            'rsi_prev': rsi_prev, 'rsi_now': rsi_now}


def scan_condition_w():
    # ✅08250451【修正·與 08102047 同型】★台指期K棒累積移到【所有關卡之前】無條件執行。
    #   ★真實事故：08102047 已為 futures-scan 修過完全一樣的問題，
    #     ★當時的結論是「累積快照是記錄行情，不該受任何策略關卡影響」，
    #     ★★但那次只改了 futures-scan，★條件W 這條路徑【沒有一起改】。
    #   ★後果：condw_scan 每 15 分鐘跑一次，但只要不在進場時間窗就 return，
    #     ★★累積器一次都沒被呼叫到（08242325 截圖為證：只印「跳過」）。
    #   ★這是 ＡＫ１８（單一修正必須推及所有同型位置）的違反。
    try:
        accumulate_txf_bar()
        # ✅09022155 收盤前提醒信放在【所有策略關卡之前】無條件執行。
        #   ★理由同 08102047 累積器事故：★★放在關卡後面會被 return/continue 跳掉。
        _futures_close_alert(datetime.now(pytz.timezone('Asia/Taipei')))
    except Exception as _e:
        print(f'  ⚠️ 台指期K棒累積異常（{str(_e)[:50]}）→ 不影響條件W')

    """條件W：週選擇權做多進場（雲端專用）。跳過第一二道，只跑第三道5分K V轉觸底翻揚→buy call。
    同窗同向最多2次（Firebase 2-slot 原子認領跨cron行程去重）。✅ 07011049 純新增，不影響既有掃描。"""
    wid = _condw_current_window()
    now_str_f = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M')
    # ✅09021330【測試開關】★不看時間窗、★★不看行情、★★★不佔 Firebase 認領槽。
    #   ★用途：驗證「Firestore 讀取權限 ＋ Gmail 寄信管線」是否活著。
    #   ★★主帥操作方式：把上方 CONDW_FORCE_TEST 改成 True，手動觸發 condw-scan
    #     workflow 一次，★★★收到信＝管線正常；沒收到＝管線壞了（與行情無關）。
    #   ★測完務必改回 False（已納入凍結清單 F-16 機器擋）。
    if CONDW_FORCE_TEST:
        print('  🧪 條件W：CONDW_FORCE_TEST=True → 強制寄出測試信（不看時間窗與行情）')
        try:
            _t5 = _normalize_df(yf.download(CONDW_TARGET, period='5d', interval='5m', progress=False))
            _tp = float(_t5['Close'].iloc[-1]) if (_t5 is not None and not _t5.empty) else 0.0
            _tb = str(_t5.index[-1])[:16] if (_t5 is not None and not _t5.empty) else '無資料'
        except Exception as _te:
            _tp, _tb = 0.0, f'抓取失敗({str(_te)[:30]})'
        _tmsg = ('🧪【測試信．非進場訊號】\n'
                 '本信由 CONDW_FORCE_TEST=True 強制寄出，★與行情無關。\n'
                 '收到本信＝Firestore 讀取權限與 Gmail 寄信管線【正常】。\n\n'
                 f'目前時間窗：{wid if wid else "不在窗內（測試模式不受此限）"}\n'
                 f'台指最新收盤：{_tp:.2f}\n'
                 f'最後一根5分K：{_tb}\n'
                 f'時間：{now_str_f}\n\n'
                 '★測試完成後請把 CONDW_FORCE_TEST 改回 False。')
        _tok = send_gmail(f'🧪【測試】條件W 管線測試 - {now_str_f}', _tmsg, urgent=True)
        print(f"  {'✅' if _tok else '❌'} 測試信{'已發送' if _tok else '發送失敗'}")
        return
    if wid is None:
        print('  ℹ️ 條件W：目前不在進場時間窗（週二15:05~週三11:30／週四15:05~週五11:30），跳過')   # ✅08241013 訊息與判定同步（原誤植10:45）
        return
    print(f'\n📊 條件W 週選擇權雙向掃描：{CONDW_TARGET}（窗 {wid}）'
          f'　★5分K OR 15分K、★★只跑第三道關卡')
    try:
        # ══ 抓 5分K（必跑）與 15分K（CONDW_ENABLE_15MK 開啟時）══
        _res = []
        df5 = _normalize_df(yf.download(CONDW_TARGET, period='5d', interval='5m', progress=False))
        if df5 is not None and not df5.empty and _bar_too_old(df5, '條件W-5分K'):
            _h = _taifex_hint_text()
            if _h:
                print(f'  ℹ️ 供參考：{_h.strip()}')
            return
        _r5 = _condw_gate3(df5, CONDW_LOOKBACK, '5分K')   # ✅09022155 54→5（定稿規格②）
        if _r5:
            _res.append(_r5)
        if CONDW_ENABLE_15MK:
            try:
                df15 = _normalize_df(yf.download(CONDW_TARGET, period='5d', interval='15m', progress=False))
                _r15 = _condw_gate3(df15, CONDW_LOOKBACK_15MK, '15分K')
                if _r15:
                    _res.append(_r15)
            except Exception as _e15:
                print(f'  ⚠️ 條件W：15分K 取得失敗（{str(_e15)[:40]}），★不影響5分K')
        if not _res:
            print('  ⚠️ 條件W：5分K 與 15分K 均無可用資料，跳過'); return

        # ══ ★★★OR 合併（★主帥 09/02 明示：兩個週期是 OR，不是 AND）══
        _buy_hits   = [r for r in _res if r['buy']]
        _short_hits = [r for r in _res if r['short']]
        if not _buy_hits and not _short_hits:
            print('  ❌ 條件W：5分K 與 15分K 第三道關卡均未成立，不進場'); return
        # ★同時成立時以【15分K 優先】：週期較長、雜訊較少（無主帥明示，★列為假設）
        def _pick(hits):
            for r in hits:
                if r['label'] == '15分K':
                    return r
            return hits[0]

        for _dir, _hits in (('buy', _buy_hits), ('sell', _short_hits)):
            if not _hits:
                continue
            r = _pick(_hits)
            _now_tw = datetime.now(pytz.timezone('Asia/Taipei'))
            _today  = _now_tw.strftime('%Y-%m-%d')
            _tv_w   = _now_tw.hour * 60 + _now_tw.minute
            _sess   = 'day' if (9*60+5 <= _tv_w <= 13*60+30) else 'night'
            _max_slot = CONDW_MAX_DAY if _sess == 'day' else CONDW_MAX_PER_WINDOW
            # ✅09022155【★★★回正：多空【共用】同一組配額】
            #   ★交接文件(09020802) 第六章⑧白紙黑字：
            #     「⑧配額　★buy call 與 buy put【共用同一組】；
            #       ★★日盤（3 次）與夜盤（2 次）★必須分開計算，不互相扣抵」
            #   ★★而我在 09021330 寫成【多空各自獨立槽位】，
            #     ★★★日盤會變成最多 6 封（多3＋空3），★超出主帥定稿的 3 封。
            #   ★我當時還在註解裡寫「同 08060105 日夜盤分離的教訓」——
            #     ★★那條教訓講的是【日盤與夜盤】要分離，★★★不是【多方與空方】要分離。
            #   ★這是把一條真實的教訓，★★套用到它管不到的地方。
            #   ★★★保留的分離：★日盤／夜盤仍各自獨立（這才是 08060105 的教訓）。
            _sent_slot = None
            for _slot in range(1, _max_slot + 1):
                _claim = _claim_alert_firebase(f'condW_{wid}_{_sess}#{_slot}', _today)
                if _claim is True or _claim is None:
                    _sent_slot = _slot; break
            if _sent_slot is None:
                print(f'  ⚠️ 條件W：本{_sess}時段已達{_max_slot}次上限（★多空共用配額），靜音'); continue
            _opt_hint = get_weekly_option_hint(r['close'], _dir)
            _title = '做多進場（buy CALL）' if _dir == 'buy' else '做空進場（buy PUT）'
            _band  = f"布林下緣：{r['boll_bot']:.2f}" if _dir == 'buy' else f"布林上緣：{r['boll_top']:.2f}"
            _arrow = '↑' if _dir == 'buy' else '↓'
            msg = (f"☁️【雲端】⭐【條件W 週選擇權{_title}】⭐（本窗第{_sent_slot}次）\n"
                   f"標的：{CONDW_TARGET}（台指）\n"
                   f"觸發週期：{r['label']}　★5分K OR 15分K，只看第三道關卡\n"
                   f"收盤：{r['close']:.2f}　{_band}\n"
                   f"RSI：{r['rsi_prev']:.1f} → {r['rsi_now']:.1f}（{_arrow}）　MACD柱：{_arrow}\n"
                   f"進場窗：{wid}\n"
                   f"時間：{now_str_f}"
                   + _opt_hint)
            _ok = send_gmail(f"☁️【雲端】⭐條件W週選{'買進' if _dir=='buy' else '做空'} "
                             f"{CONDW_TARGET} - {now_str_f}", msg, urgent=True)
            print(f"  {'✅' if _ok else '❌'} 條件W {_title}／{r['label']}"
                  f"（本窗第{_sent_slot}次）{'已發送' if _ok else '發送失敗'}")
    except Exception as _e:
        print(f'  ⚠️ 條件W掃描異常：{str(_e)[:80]}')


def _kline_cache_path():
    import os
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), KLINE_CACHE_FILE)

def load_cross_run_cache():
    """✅ 方案②(07061319) 程序啟動時載入『今日』跨輪月K/週K快取；任何錯誤自動回退(不影響掃描)。"""
    if not USE_CROSS_RUN_CACHE:
        return
    global weekly_cache, daily_cache
    try:
        import pickle, os
        p = _kline_cache_path()
        if not os.path.exists(p):
            print('♻️ [方案②] 無跨輪快取檔，本輪重新抓取並建立'); return
        with open(p, 'rb') as f:
            data = pickle.load(f)
        today = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y-%m-%d')
        if not isinstance(data, dict) or data.get('date') != today:
            _dd = data.get('date') if isinstance(data, dict) else 'NA'
            print(f'♻️ [方案②] 跨輪快取非今日({_dd})，忽略、重新抓取'); return
        w = data.get('weekly', {}); d = data.get('daily', {})
        _wok = {k: v for k, v in w.items() if hasattr(v, 'empty') and not v.empty and len(v) >= 30}
        _dok = {k: v for k, v in d.items() if hasattr(v, 'empty') and not v.empty}
        weekly_cache.update(_wok); daily_cache.update(_dok)
        print(f'✅ [方案②] 已載入今日跨輪快取：月K {len(_wok)} 檔、週K {len(_dok)} 檔（免重抓）')
    except Exception as _e:
        print(f'⚠️ [方案②] 載入跨輪快取失敗，自動回退為重新抓取：{_e}')

def save_cross_run_cache(prev_w=0, prev_d=0):
    """✅ 方案②(07061319) 掃描結束保存今日快取（僅在快取有成長時才寫檔，省IO）；失敗不影響掃描。"""
    if not USE_CROSS_RUN_CACHE:
        return
    try:
        import pickle
        if len(weekly_cache) <= prev_w and len(daily_cache) <= prev_d:
            return
        today = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y-%m-%d')
        with open(_kline_cache_path(), 'wb') as f:
            pickle.dump({'date': today, 'weekly': weekly_cache, 'daily': daily_cache}, f)
        print(f'💾 [方案②] 已保存今日跨輪快取：月K {len(weekly_cache)} 檔、週K {len(daily_cache)} 檔')
    except Exception as _e:
        print(f'⚠️ [方案②] 保存跨輪快取失敗（不影響本輪掃描）：{_e}')

def _hh_snapshot(_d, _label):
    """✅ (07130626) 持股健檢：輸出單一週期的 RSI／MACD柱／布林位置 摘要行"""
    _r  = float(_d['rsi14'].iloc[-1]);      _rp = float(_d['rsi14'].iloc[-2])
    _mh = float(_d['macd_hist'].iloc[-1]);  _mp = float(_d['macd_hist'].iloc[-2])
    _bt = float(_d['boll_top20'].iloc[-1])
    _bb = float(_d['boll_bot20'].iloc[-1])
    # ✅ (08031611)【bug修復】calc_indicators 主路徑(806行起)只建 boll_top20/boll_bot20、
    #    並【未建 boll_mid20】(僅少量資料的備援路徑738行才有) → 原寫法必 KeyError，
    #    致每檔健檢皆失敗。改由上下軌取中值（數學上等同 ma_c_20），兩條路徑都安全。
    _bm = (_bt + _bb) / 2
    _c  = float(_d['Close'].iloc[-1])
    if   _c >= _bt: _pos = '觸及上軌(過熱區)'
    elif _c <= _bb: _pos = '觸及下軌(超跌區)'
    elif _c >= _bm: _pos = '中軌之上(偏多)'
    else:           _pos = '中軌之下(偏弱)'
    return (f"　　{_label}：RSI {_r:.1f}（{'↑上升' if _r > _rp else '↓下降'}）｜"
            f"MACD柱 {_mh:+.3f}（{'↑上升' if _mh > _mp else '↓下降'}）｜布林 {_pos}")

def check_holdings_health():
    """✅ (07130626)【持股每日健檢通知】主帥指定功能
    ・對所有持股（台股/美股/虛擬幣/外匯）計算【長期＝月K】與【中期＝週K】的
      布林／RSI／MACD，並依【混合模式＝OR】（長期 or 中期任一觸發出場即示警）
      給出「續抱持有」或「建議評估賣出/回補」，彙整成【一封】Gmail。
    ・出場判斷完全沿用系統既有策略函式（check_sell_condition／check_sell_condD；
      空單走鏡像 check_cover_condition／check_cover_condD）→ 與掃描策略完全一致。
    ・雲端版由 GitHub Actions 觸發 → 筆電未開機也收得到；本機版策略相同。
    """
    print("\n📋 【持股每日健檢】啟動...")
    # ✅08241728【主帥指定】tuple 加入第4欄【市場別】，
    #   ★原本主旨只寫「N 檔觸發出場條件」，★完全看不出是台股還是美股。
    _items = []   # (顯示名, 代號, 是否空單, 市場別)
    for _c in HOLDINGS_TW:
        _items.append((_c, _c + '.TW', (_c in HOLDINGS_SHORT), '台股'))
    for _c in HOLDINGS_US:
        _items.append((_c, _c, (_c in HOLDINGS_SHORT), '美股'))
    for _c in HOLDINGS_CRYPTO:
        _items.append((_c, _c, (_c in HOLDINGS_SHORT), '虛擬幣'))
    for _c in HOLDINGS_FX:
        _items.append((_c, _c, (_c in HOLDINGS_SHORT), '外匯'))

    if not _items:
        print("📋 目前無持股，略過健檢")
        return

    _lines, _alert_cnt, _ok_cnt, _fail_cnt = [], 0, 0, 0
    _fail_names = []   # ✅ (08031611) 失敗標的名稱（僅在有示警而發信時，於信末附註）
    _alert_mkt = {}   # ✅08241728 各市場別的示警檔數，供主旨組裝
    for _name, _tk, _is_short, _mkt in _items:
        try:
            # 長期＝月K(5y/1mo)；台股 .TW 無資料時自動改試 .TWO（上櫃）
            _dm = get_stock_data(_tk, period='5y', interval='1mo')
            if (_dm is None or len(_dm) < 26) and _tk.endswith('.TW'):
                _tk2 = _tk.replace('.TW', '.TWO')
                _dm2 = get_stock_data(_tk2, period='5y', interval='1mo')
                if _dm2 is not None and len(_dm2) >= 26:
                    _tk, _dm = _tk2, _dm2
            # 中期＝週K(2y/1wk)
            _dw = get_stock_data(_tk, period='2y', interval='1wk')
            if _dm is None or _dw is None or len(_dm) < 26 or len(_dw) < 26:
                _lines.append(f"・{_name}：⚠️ 取得資料失敗或資料不足，本次略過")
                _fail_cnt += 1
                continue

            _im = calc_indicators(_dm)
            _iw = calc_indicators(_dw)
            if _im is None or _iw is None:
                _lines.append(f"・{_name}：⚠️ 指標計算失敗，本次略過")
                _fail_cnt += 1
                continue

            _px = float(_im['Close'].iloc[-1])

            # 出場判斷：沿用系統既有策略（做多＝賣出；做空＝回補），混合模式 OR
            if _is_short:
                _s_m = check_cover_condition(_im); _sd_m, _md_m = check_cover_condD(_im)
                _s_w = check_cover_condition(_iw); _sd_w, _md_w = check_cover_condD(_iw)
                _act = '建議評估【回補】(空單獲利了結)'
            else:
                _s_m = check_sell_condition(_im); _sd_m, _md_m = check_sell_condD(_im)
                _s_w = check_sell_condition(_iw); _sd_w, _md_w = check_sell_condD(_iw)
                _act = '建議評估【賣出】(獲利了結)'

            _hits = []
            if _s_m:  _hits.append('長期(月K) 出場訊號')
            if _sd_m: _hits.append(f'長期(月K) {_md_m}')
            if _s_w:  _hits.append('中期(週K) 出場訊號')
            if _sd_w: _hits.append(f'中期(週K) {_md_w}')

            if _hits:
                # ✅ (08031611)【防狼來了】只有【觸發出場條件】者才寫進信中
                # ✅ (08031637)【計數順序修正】先把整行組好再計數：原為「先計數→後呼叫
                #    _hh_snapshot」，若 _hh_snapshot 拋錯，計數已加卻又落入 except 計入失敗，
                #    造成主旨與內文不符（截圖實證：主旨「示警2檔／持有9檔」但內文全為健檢失敗）。
                _row = (
                    f"・[{_mkt}] {_name}{'（空單）' if _is_short else ''}　現價 {_px:,.2f}\n"
                    f"{_hh_snapshot(_im, '長期(月K)')}\n"
                    f"{_hh_snapshot(_iw, '中期(週K)')}\n"
                    f"　　👉 🔴 {_act}\n　　　　觸發：" + "；".join(_hits) + "\n"
                )
                _alert_cnt += 1
                _alert_mkt[_mkt] = _alert_mkt.get(_mkt, 0) + 1   # ✅08241728
                _lines.append(_row)
            else:
                _ok_cnt += 1   # ✅ (08031611) 未觸發者不列入信中，僅計數（避免無效資訊稀釋警覺）
            print(f"  ✅ {_name} 健檢完成（{'示警' if _hits else '持有'}）")
        except Exception as _e:
            _fail_cnt += 1
            _fail_names.append(str(_name))
            print(f"  ⚠️ {_name} 健檢失敗：{_e}")

    # ✅ (08031611)【防狼來了・主帥指定】零示警＝不發信，僅 console 記錄；
    #    唯有出現出場示警才寄信，避免每日固定一封造成警覺心鈍化。
    if _alert_cnt == 0:
        print(f"📋 持股健檢完成：無任何出場示警（續抱{_ok_cnt}檔／失敗{_fail_cnt}檔）→ 不發信")
        return

    _now = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M')
    # ✅08241728【主帥指定】主旨標明市場別，★一眼分辨台股／美股／虛擬幣／外匯。
    #   ・單一市場 → 「台股2檔」；★多市場 → 「台股2檔／美股1檔」
    #   ・順序固定為 台股→美股→虛擬幣→外匯，★不隨字典順序浮動
    _mk_order = ['台股', '美股', '虛擬幣', '外匯']
    _mk_txt = '／'.join(f'{_m}{_alert_mkt[_m]}檔' for _m in _mk_order if _alert_mkt.get(_m))
    _subject = f"☁️【雲端】📋 持股健檢示警！{_mk_txt} 觸發出場條件"
    _body = (
        f"【持股健檢・出場示警】{_now}（台灣時間）\n"
        f"{'='*46}\n"
        f"判斷方式＝混合模式(OR)：長期(月K) 或 中期(週K) 任一觸發出場條件即示警；\n"
        f"出場條件與主掃描系統【完全相同】(賣出條件／條件D出場；空單為回補鏡像)。\n"
        f"※ 未觸發出場者不列入本信（僅統計）；零示警日【不發信】。\n"
        f"{'='*46}\n\n"
        + "\n".join(_lines)
        + f"\n{'='*46}\n"
        f"合計：示警 {_alert_cnt} 檔／續抱 {_ok_cnt} 檔／失敗 {_fail_cnt} 檔"
        + (f"（失敗：{'、'.join(_fail_names)}）" if _fail_names else "") + "\n"
        f"※ 本信為持股出場示警，非即時買賣訊號；實際進出仍請自行判斷。\n"
        f"※ 持股清單於程式 HOLDINGS_TW／HOLDINGS_US／HOLDINGS_CRYPTO／HOLDINGS_FX 設定。\n"
    )
    try:
        send_gmail(_subject, _body)
        print(f"📧 持股健檢示警已寄出（示警{_alert_cnt}／續抱{_ok_cnt}／失敗{_fail_cnt}）")
    except Exception as _e:
        print(f"⚠️ 持股健檢寄信失敗：{_e}")

def scan_futures_15mk():
    """✅ (08060105)【15分K 訊號】主帥指定新增（原系統只有5分K）。
    ・緣由：2026/08/05(週三)09:45 主帥截圖之15分K已達成做多條件（MACD柱觸底V轉＋RSI↑），
      但系統【根本沒有15分K這個資料來源】，結構上不可能觸發。
    ・多空雙向：做多→建議 buy CALL；做空→建議 buy PUT（沿用 _opt_hint_if_window 時窗守門）。
    ・回看根數【等比換算】(K棒等比換算原則)：5分K 54根 ＝ 4.5小時
      → 15分K 為 54÷3 ＝【18根】，涵蓋相同時間長度，不可直接沿用54。
    ・去重：同一根15分K棒、同方向【只發一次】，用 Firebase 原子佔位（跨cron行程安全）。
    ・完全獨立於5分K流程：本函式任何例外都被吃掉，【不影響】既有5分K掃描與訊號。
    """
    try:
        _n15 = max(3, int(round(BUY_LOOKBACK_5MK / 3)))    # 54根5分K → 18根15分K（等比換算）
        for _tk in FUTURES_5MK_TARGETS:
            try:
                # ✅08160731【F-15】收盤後不得再產生訊號（見 _tw_spot_session_ok 決策註記）
                if not _tw_spot_session_ok(_tk):
                    print(f'  🔕 15分K：{_tk} 非現貨交易時段（09:00~13:30），跳過')
                    continue
                _df = _normalize_df(yf.download(_tk, period='5d', interval='15m', progress=False))
                if _df is None or _df.empty or len(_df) < _n15 + 2:
                    print(f'  ⚠️ 15分K：{_tk} 資料不足，跳過'); continue
                _df = calc_indicators(_df)
                if _df is None:
                    print(f'  ⚠️ 15分K：{_tk} 指標計算失敗，跳過'); continue

                _l = _df['Low']; _h = _df['High']
                _bb = _df['boll_bot20']; _bt = _df['boll_top20']
                _mh = _df['macd_hist'];  _rsi = _df['rsi14']
                _close    = float(_df['Close'].iloc[-1])
                _boll_bot = float(_bb.iloc[-1]); _boll_top = float(_bt.iloc[-1])
                _r_now = float(_rsi.iloc[-1]); _r_prev = float(_rsi.iloc[-2])
                _m_now = float(_mh.iloc[-1]);  _m_prev = float(_mh.iloc[-2])
                _r_up   = _r_now > _r_prev;  _r_dn = _r_now < _r_prev
                _m_up   = _m_now > _m_prev;  _m_dn = _m_now < _m_prev

                # ✅08301755【15分K 容忍度改綁通道寬度】多空兩側同步（ＡＫ１８）
                #   半通道寬 = (上軌-下軌)/2 = 2σ；不依賴 boll_mid20 欄位（部分路徑未建）
                # ✅08301905 改用全檔統一的 _gate_lower/_gate_upper（取兩制較嚴者）
                _buy_gate  = _gate_lower(_boll_top, _boll_bot)
                _sell_gate = _gate_upper(_boll_top, _boll_bot)
                _bb_gate   = _gate_lower(_bt.iloc[-_n15:], _bb.iloc[-_n15:])
                _bt_gate   = _gate_upper(_bt.iloc[-_n15:], _bb.iloc[-_n15:])

                # 多方：近18根任一最低價觸及布林下軌帶 AND 當根RSI↑ AND MACD柱↑ AND 現價仍近下軌
                _near_low  = (_l.iloc[-_n15:] <= _bb_gate).any()
                _buy  = (_near_low and _r_up and _m_up
                         and _close <= _buy_gate and _r_now > BUY_RSI_MIN)
                # 空方鏡像：近18根任一最高價觸及布林上軌帶 AND RSI↓ AND MACD柱↓ AND 現價仍近上軌
                #   ✅08301755【補上空方RSI門檻】SHORT_RSI_MAX 原本【宣告了但全檔零使用】，
                #     導致 2026/08/26 10:08 在 RSI=69.5(>65) 的上漲趨勢中發出做空訊號。
                #     ★5分K路徑(第4875行)與日K路徑本來就有此門檻，★只有15分K漏掉 → ＡＫ１８。
                _near_high = (_h.iloc[-_n15:] >= _bt_gate).any()
                _sell = (_near_high and _r_dn and _m_dn
                         and _close >= _sell_gate and _r_now < SHORT_RSI_MAX)

                if not (_buy or _sell):
                    print(f"  ℹ️ {_tk} 15分K：RSI={_r_now:.1f}({'↑' if _r_up else '↓'})  "
                          f"MACD柱={'↑' if _m_up else '↓'}  未達進出場條件")
                    continue

                _dir = 'buy' if _buy else 'sell'
                # ── 去重：同一根15分K棒、同方向只發一次（Firebase 原子佔位）──
                _bar = str(_df.index[-1])[:16].replace(' ', 'T')
                _tw  = datetime.now(pytz.timezone('Asia/Taipei'))
                _day = _tw.strftime('%Y-%m-%d')
                _claim = _claim_alert_firebase(f'f15mk_{_tk}_{_dir}_{_bar}', _day)
                if _claim is False:
                    print(f'  🔕 15分K：{_tk} 本根({_bar}) {_dir} 已通知過，跳過'); continue

                _now_str = _tw.strftime('%Y/%m/%d %H:%M')
                _opt = _opt_hint_if_window(_close, _dir)
                if _buy:
                    _title = f"☁️【雲端】⭐【期貨15分K買進訊號】⭐"
                    _body  = (f"{_title}\n標的：{_tk}\n"
                              f"收盤：{_close:.2f}　布林下軌：{_boll_bot:.2f}\n"
                              f"RSI：{_r_prev:.1f} → {_r_now:.1f}（↑）　MACD柱：{_m_prev:+.2f} → {_m_now:+.2f}（↑）\n"
                              f"回看根數：{_n15} 根15分K（＝54根5分K等比換算）\n"
                              f"時間：{_now_str}" + _opt)
                    _sub = f"☁️【雲端】⭐期貨15分K買進 {_tk} - {_now_str}"
                else:
                    _title = f"☁️【雲端】🔻【期貨15分K做空訊號】🔻"
                    _body  = (f"{_title}\n標的：{_tk}\n"
                              f"收盤：{_close:.2f}　布林上軌：{_boll_top:.2f}\n"
                              f"RSI：{_r_prev:.1f} → {_r_now:.1f}（↓）　MACD柱：{_m_prev:+.2f} → {_m_now:+.2f}（↓）\n"
                              f"回看根數：{_n15} 根15分K（＝54根5分K等比換算）\n"
                              f"時間：{_now_str}" + _opt)
                    _sub = f"☁️【雲端】🔻期貨15分K做空 {_tk} - {_now_str}"
                _ok = send_gmail(_sub, _body, urgent=True)
                print(f"  {'✅' if _ok else '❌'} {_tk} 15分K {_dir} 訊號{'已發送' if _ok else '發送失敗'}")
            except Exception as _e:
                print(f'  ⚠️ 15分K：{_tk} 掃描異常（{str(_e)[:60]}）')
    except Exception as _e:
        print(f'  ⚠️ 15分K掃描整體異常（{str(_e)[:60]}）→ 不影響5分K掃描')


def main_task():

    # 🔥 宣告 global 確保全域共用
    global weekly_cache, daily_cache, buy_signals, sell_signals, delist_signals, _futures_is_holding, _futures_is_short  # ✅ 07031936 加入期貧持倬全域
    global _holdings_sent   # ✅ (07130626) 持股每日健檢

    # ✅ (07130626)【持股每日健檢模式】SCAN_TYPE='holdings'：只做持股健檢並寄一封報告，不跑全市場掃描
    if SCAN_TYPE == 'holdings':
        if _holdings_sent:
            print("📋 本次執行已完成持股健檢，跳過重複執行")
            return
        check_holdings_health()
        _holdings_sent = True
        return

    # ✅ [修正: 每次掃描開始前必須清空當次訊號, 避免重複累加發信]
    buy_signals   = []
    sell_signals  = []
    delist_signals = []
    if LOAD_SENTINEL:
        _load_stats['fetch_total'] = 0; _load_stats['fetch_fail'] = 0; _load_stats['scan_start'] = time.time()

    tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(tz)
    now_str = now.strftime('%Y/%m/%d %H:%M')
    now_hour = now.hour

    # ✅ 每天早上 8 點且快取有東西時，執行清空快取
    if now_hour == WEEKLY_REFRESH_HOUR and len(weekly_cache) > 0:
        print(f"♻️ [{now_str}] 偵測到新的一天, 正在清除快取資料以獲取最新K線...")
        weekly_cache.clear()
        daily_cache.clear()

    # ✅ 方案②(07061319) 記憶體快取為空(新程序或剛清空)時，載入今日跨輪快取以免重抓
    if USE_CROSS_RUN_CACHE and len(weekly_cache) == 0 and len(daily_cache) == 0:
        load_cross_run_cache()
    _xrun_prev_w = len(weekly_cache); _xrun_prev_d = len(daily_cache)   # 記錄載入後基準，供結束時判斷是否成長

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
        _is_saturday_scan = (_now_tw().weekday() == 5) or (SCAN_TYPE == 'tw_full')   # ✅08060719 時區修正：原取UTC星期
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

        prefetch_realtime_prices(tw_list, '台股')   # ✅08061155 批次預抓即時價
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
                    elif result[0] == 'COVER':
                        sell_signals.append(('台股回補', code, *result[1:]))
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
        prefetch_realtime_prices(US_STOCKS, '美股')   # ✅08061155 批次預抓即時價
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
                elif result[0] == 'COVER':
                    sell_signals.append(('美股回補', ticker, *result[1:]))
                elif result[0] in ('DELIST_HOLD', 'DELIST_WATCH'):
                    delist_signals.append(('美股', ticker, result[0], result[1]))
            time.sleep(0.1)

        # ✅ v06081833：台股收盤後執行漲停追蹤（14:00後，每日一次）
        # ✅ (08031611)【時區bug修復】原 datetime.now() 在 GitHub Actions 為 UTC，
        #    台灣00:45=UTC16:45 亦 >=14 → 漲停追蹤誤在【台灣深夜】觸發發信。
        #    改用台灣時間，確保只在台股收盤後(14:00~)執行，符合原設計。
        _scan_hour = datetime.now(pytz.timezone('Asia/Taipei')).hour
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
        prefetch_realtime_prices(CRYPTO_LIST, '虛擬幣')   # ✅08061155 批次預抓即時價
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
                elif result[0] == 'COVER':
                    sell_signals.append(('虛擬幣回補', ticker, *result[1:]))
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
        prefetch_realtime_prices(FX_LIST, '外匯')   # ✅08061155 批次預抓即時價
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
                elif result[0] == 'COVER':
                    sell_signals.append(('外匯回補', ticker, *result[1:]))
                elif result[0] == 'SHORT':
                    sell_signals.append(('外匯做空', ticker, *result[1:]))
            time.sleep(0.1)

    # ── 期貨5分K掃描（TEST_MODE='5mk'且FUTURES在active_markets時執行）──
    if 'FUTURES' in active_markets and TEST_MODE == '5mk':
        # ✅ 持倉狀態：多倉/空倉分開追蹤
        # ✅ 07031936 _futures_is_holding/_futures_is_short 已於模組頂層正式宣告為全域，不再用dir()守門
        _pos_str = '多倉🔴' if _futures_is_holding else ('空倉🔵' if _futures_is_short else '空手⬜')
        _mode_str = f'SCAN_MODE={SCAN_MODE}' if 'SCAN_MODE' in dir() else SCAN_MODE
        print(f'\n📊 期貨5分K掃描：{FUTURES_5MK_TARGETS}')
        # ✅08102047【修正·問題A】台指期K棒累積移到【所有關卡之前】無條件執行。
        #   真實事故：08091843 起把它放在「下載df5之後、陳舊判定之前」，
        #   但5分K路徑在更早的關卡就 return/continue，根本走不到那一行
        #   → 主帥 08/10 15:08 實跑 futures-scan，自報顯示「未執行到」，
        #     代表 🔴B(a) 階段二的資料【一根都沒累積到】。
        #   ★累積快照是「記錄行情」，不該受任何策略關卡影響，故必須放最前面。
        accumulate_txf_bar()
        print(f'   持倉狀態：{_pos_str}　掃描模式：{_mode_str}')
        # ✅ 08060105 新增：先跑15分K訊號（獨立流程，異常不影響下方5分K掃描）
        print(f'\n📊 期貨15分K掃描：{FUTURES_5MK_TARGETS}')
        scan_futures_15mk()
        for ticker in FUTURES_5MK_TARGETS:
            try:
                # ✅08160731【F-15】收盤後不得再產生訊號（見 _tw_spot_session_ok 決策註記）
                if not _tw_spot_session_ok(ticker):
                    print(f'  🔕 5分K：{ticker} 非現貨交易時段（09:00~13:30），跳過')
                    continue
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
                # ✅09022055【★★★修正：做空原本被做多的第二道 continue 綁架】
                #   ★原式：`if check_buy_eleader(df5_d) is None: continue`
                #     → ★★這是【做多】的 eLeader，★但 continue 會跳過【整支標的】。
                #   ★★★後果：大跌時做多 eLeader 幾乎必然不過 → 整支跳掉 →
                #     ★下面的 _5mk_short 根本執行不到 → ★★做空在結構上永遠不可能觸發。
                #   ★這是 2026/09/02 台股大跌900點沒有做空訊號的【第二個根因】
                #     （第一個是時窗，已於 09021415 修正）。
                #   ★★★主帥 09/02 明示：★期貨【不可以】拿掉關卡1、關卡2
                #     （「期貨放著等歸零，不是大賺就是破產」）。
                #   ★★故本次【一道關卡都沒有拿掉】，★只是讓多空各走各的第二道。
                _buy_2nd   = check_buy_eleader(df5_d)   is not None
                _short_2nd = check_short_eleader(df5_d) is not None
                if not _buy_2nd and not _short_2nd:
                    print(f'  ❌ {ticker} 第二道日K eLeader多空皆未通過，跳過')
                    continue
                print(f'  ✅ {ticker} 第二道日K eLeader通過（多:{_buy_2nd} 空:{_short_2nd}）')

                # ── 第三道：5分K 54根條件A/B ────────────────
                # ✅【夜盤保留】主力以夜盤為主戰場，不剔除夜盤
                # period='5d' 確保足夠近期夜盤+日盤K棒（約1000+根）
                df5 = _normalize_df(yf.download(ticker, period='5d', interval='5m', progress=False))
                if df5 is None or df5.empty or len(df5) < BUY_LOOKBACK_5MK + 2:
                    print(f'  ⚠️ {ticker} 5分K資料不足（需>={BUY_LOOKBACK_5MK+2}根），跳過')
                    continue
                check_tw_intraday_extreme()   # ✅08092108【🆕E】台股盤中極端異動即時偵測
                if _bar_too_old(df5, f'{ticker} 期貨5分K'):
                    _h = _taifex_hint_text()
                    if _h:
                        print(f'  ℹ️ 供參考：{_h.strip()}')
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
                # ✅08301905 容忍度改綁通道寬度；原式：bb5.iloc[-n5:] * BUY_BOLL_TOLERANCE
                _5mk_cond_A = (l5.iloc[-n5:] <= _gate_lower(bt5.iloc[-n5:], bb5.iloc[-n5:])).any() and rsi_rising and macd_rising
                # ── 條件B：近54根最低均<布林中軌 AND 最高均<布林上軌 AND 前N根MACD縮 AND 當根MACD放大
                _5mk_low_mid  = (l5.iloc[-n5:] < bm5.iloc[-n5:]).all()
                _5mk_high_top = (h5.iloc[-n5:] < bt5.iloc[-n5:]).all()
                _5mk_macd_shr = len(mh5) >= n5+1 and all(float(mh5.iloc[-n5-1+j]) > float(mh5.iloc[-n5+j]) for j in range(n5-1))
                _5mk_cond_B   = _5mk_low_mid and _5mk_high_top and _5mk_macd_shr and macd_rising
                # ✅ v06160503修復：near_lower/near_upper 原定義在使用之後(use-before-def)，上移至此
                near_lower   = close  <= _gate_lower(boll_top, boll_bot)   # ✅08301905 原式：boll_bot * BUY_BOLL_TOLERANCE
                near_upper   = close  >= _gate_upper(boll_top, boll_bot)   # ✅08301905 原式：boll_top * SELL_BOLL_TOLERANCE
                # ✅ v05192313：5分K進場加入條件D（追高/追空）
                _5mk_cond_D_long  = getattr(df5.iloc[-1],'rsi14',0) > getattr(df5.iloc[-2],'rsi14',0) and near_upper
                _5mk_cond_D_short = getattr(df5.iloc[-1],'rsi14',0) < getattr(df5.iloc[-2],'rsi14',0) and near_lower
                # ✅ v05192327：5分K進場加E和F
                _5mk_cond_E_long = check_condE_long(df5) if df5 is not None else False
                _5mk_cond_F_long = check_buy_eleader(df5) is not None if df5 is not None else False
                # ✅09022055 第一道、第二道改為【顯式帶入】。
                #   ★原式靠上方的 continue 隱含保證，★★拆開多空後 continue 不再等價，
                #   ★★★若不顯式帶入，會出現「只有空方過第一道、多方卻發買進訊號」的錯誤。
                _5mk_buy = (_1st_long and _buy_2nd
                            and (_5mk_cond_A or _5mk_cond_B or _5mk_cond_D_long or
                                 _5mk_cond_E_long or _5mk_cond_F_long)
                            and rsi_now > BUY_RSI_MIN)
                now_str_f = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y/%m/%d %H:%M')
                # ── 日K位階參考（印出但不擋住掃描）──────────────
                try:
                    df_d5 = _normalize_df(yf.download(ticker, period='10d', interval='1d', progress=False))
                    if df_d5 is not None and not df_d5.empty and len(df_d5) >= 5:
                        df_d5 = calc_indicators(df_d5)
                        if df_d5 is not None:
                            _daily_buy  = check_buy_precondition(df_d5)[0]
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
                # ✅09022055 _short_2nd 已於第二道統一算好（原本在此重算，且被 continue 擋住）
                # 第三道做空條件A/B（鏡像多方）
                _5mk_short_A = (h5.iloc[-n5:] >= bt5.iloc[-n5:] * 1.00).any() and rsi_falling and macd_falling
                _5mk_high_mid  = (h5.iloc[-n5:] > bm5.iloc[-n5:]).all()
                _5mk_low_bot   = (l5.iloc[-n5:] > bb5.iloc[-n5:]).all()
                _5mk_macd_exp  = len(mh5) >= n5+1 and all(float(mh5.iloc[-n5-1+j]) < float(mh5.iloc[-n5+j]) for j in range(n5-1))
                _5mk_short_B   = _5mk_high_mid and _5mk_low_bot and _5mk_macd_exp and macd_falling
                _5mk_short = _short_1st and _short_2nd and (_5mk_short_A or _5mk_short_B) and rsi_now < (100 - BUY_RSI_MIN)

                _is_night_now = (now_str_f[11:16] >= '01:00' and now_str_f[11:16] < '05:00')
                # ✅ 深夜01~05有持倉：只掃平倉，跳過買進
                # ✅09022055 不在條件W窗內時【只掃平倉】，★嚴禁在窗外開新倉
                _entry_allowed = in_futures
                if (_5mk_buy and near_lower and _entry_allowed
                        and not (_is_night_now and _futures_is_holding)):
                    # ── 5分鐘內最多2封上限 ──────────────────────
                    _now_ts = time.time()
                    if not hasattr(send_gmail, '_futures_log'): send_gmail._futures_log = []
                    send_gmail._futures_log = [t for t in send_gmail._futures_log if _now_ts - t < 300]
                    if len(send_gmail._futures_log) >= 2:
                        print(f"  ⚠️ {ticker} 5分鐘內已發2封，跳過（防吵機制）"); pass
                    else:
                        send_gmail._futures_log.append(_now_ts)
                        # ✅ 05101133：週三/週五加入週選擇權推薦
                        # ✅ 08032126：時窗由「週三/五 09:05~10:45」擴展為
                        #    「週二~週五 09:05~13:30」，統一改用 _opt_hint_if_window 守門
                        _opt_hint = _opt_hint_if_window(close, 'buy')
                        msg = (
                            f"☁️【雲端】⭐【期貨5分K買進訊號】⭐\n"
                            f"標的：{ticker}\n"
                            f"收盤：{close:.2f}　布林下緣：{boll_bot:.2f}\n"
                            f"RSI：{rsi_prev:.1f} → {rsi_now:.1f}（↑）\n"
                            f"時間：{now_str_f}"
                            + _opt_hint
                        )
                        _ok = send_gmail(f"☁️【雲端】⭐期貨5分K買進 {ticker} - {now_str_f}", msg, urgent=True)
                        print(f"  {'✅' if _ok else '❌'} {ticker} 5分K買進訊號{'已發送' if _ok else '發送失敗'}")
                        # ✅ 方案A：買進訊號發出 → 記錄持倉狀態
                        _futures_is_holding = True
                        _futures_is_short   = False
                        print(f"  📌 持倉狀態已標記：is_futures_holding=True")
                        _save_futures_position(True, False, f'5mk買進 {ticker}')  # ✅09022055

                elif rsi_falling and macd_falling and near_upper:
                    # ── 5分鐘內最多2封上限 ──────────────────────
                    _now_ts = time.time()
                    if not hasattr(send_gmail, '_futures_log'): send_gmail._futures_log = []
                    send_gmail._futures_log = [t for t in send_gmail._futures_log if _now_ts - t < 300]
                    if len(send_gmail._futures_log) >= 2:
                        print(f"  ⚠️ {ticker} 5分鐘內已發2封，跳過（防吵機制）"); pass
                    else:
                        send_gmail._futures_log.append(_now_ts)
                        # ✅ 08032126：多方轉弱出場＝盤勢偏空 → 同步建議 buy PUT
                        _opt_hint2 = _opt_hint_if_window(close, 'sell')
                        msg = (
                            f"☁️【雲端】🔔【期貨5分K平倉訊號】🔔\n"
                            f"標的：{ticker}\n"
                            f"收盤：{close:.2f}　布林上緣：{boll_top:.2f}\n"
                            f"RSI：{rsi_prev:.1f} → {rsi_now:.1f}（↓）\n"
                            f"時間：{now_str_f}"
                            + _opt_hint2
                        )
                        _ok = send_gmail(f"☁️【雲端】🔔期貨5分K平倉 {ticker} - {now_str_f}", msg, urgent=True)
                        print(f"  {'✅' if _ok else '❌'} {ticker} 5分K平倉訊號{'已發送' if _ok else '發送失敗'}")
                        # ✅ 方案A：平倉訊號發出 → 清除持倉狀態
                        _futures_is_holding = False
                        _save_futures_position(False, _futures_is_short, '5mk平倉')  # ✅09022055
                        print(f"  📌 持倉狀態已清除：is_futures_holding=False")
                # ✅【做空訊號】三道關卡通過且接近布林上軌，且深夜無空倉
                # ✅09022055 窗外禁開新空倉（同買進，★出場才是窗外允許的動作）
                elif (_5mk_short and near_upper and _entry_allowed
                      and not (_is_night_now and _futures_is_short)):
                    _now_ts = time.time()
                    if not hasattr(send_gmail, '_futures_log'): send_gmail._futures_log = []
                    send_gmail._futures_log = [t for t in send_gmail._futures_log if _now_ts - t < 300]
                    if len(send_gmail._futures_log) >= 2:
                        print(f"  ⚠️ {ticker} 5分鐘內已發2封，跳過（防吵機制）")
                    else:
                        send_gmail._futures_log.append(_now_ts)
                        # ✅ 08032126【重大補漏】做空進場原本【完全沒有】週選擇權推薦，
                        #    PUT 提示只掛在「平倉」訊號上 → 2026/07/17 大跌3000點當日
                        #    即使在時窗內也不會建議 buy PUT。本次補上。
                        _opt_hint3 = _opt_hint_if_window(close, 'sell')
                        msg = (
                            f"☁️【雲端】🔻【期貨5分K做空訊號】🔻\n"
                            f"標的：{ticker}\n"
                            f"收盤：{close:.2f}　布林上軌：{boll_top:.2f}\n"
                            f"RSI：{rsi_prev:.1f} → {rsi_now:.1f}（↓）\n"
                            f"時間：{now_str_f}"
                            + _opt_hint3
                        )
                        _ok = send_gmail(f"☁️【雲端】🔻期貨5分K做空 {ticker} - {now_str_f}", msg, urgent=True)
                        print(f"  {'✅' if _ok else '❌'} {ticker} 做空訊號{'已發送' if _ok else '發送失敗'}")
                        _futures_is_short   = True
                        _futures_is_holding = False   # 做空時清除多倉
                        _save_futures_position(False, True, f'5mk做空 {ticker}')  # ✅09022055
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
                        # ✅ 08032126【補漏】空方回補＝盤勢轉強 → 同步建議 buy CALL（多空雙向對稱）
                        _opt_hint4 = _opt_hint_if_window(close, 'buy')
                        msg = (
                            f"☁️【雲端】🟢【期貨5分K平空回補】🟢\n"
                            f"標的：{ticker}\n"
                            f"收盤：{close:.2f}　布林下軌：{boll_bot:.2f}\n"
                            f"RSI：{rsi_prev:.1f} → {rsi_now:.1f}（↑）\n"
                            f"時間：{now_str_f}"
                            + _opt_hint4
                        )
                        _ok = send_gmail(f"☁️【雲端】🟢期貨5分K平空回補 {ticker} - {now_str_f}", msg, urgent=True)
                        print(f"  {'✅' if _ok else '❌'} {ticker} 平空回補訊號{'已發送' if _ok else '發送失敗'}")
                        _futures_is_short = False
                        _save_futures_position(_futures_is_holding, False, '5mk平空回補')  # ✅09022055
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
    # ✅08100036【⚪F′】補掃「全市場掃描範圍外」的觀察清單標的
    #   ★_scanned_tickers_all 必須在此就地組出：
    #     本輪實測抓到，原先寫在下方通知區會造成 undefined name（用在定義之前）。
    # ✅08102047【修正·問題B】觀察清單只在【全市場掃描模式】執行。
    #   真實事故：期貨5分K模式每5分鐘跑一次，原本會連帶每5分鐘讀一次 Firebase
    #   並對14檔補掃標的重抓行情 → 一天數百次無謂呼叫，恐觸發 yfinance 限流。
    if TEST_MODE in ('5mk', 'condW'):
        print('  ⏭️ 觀察清單：期貨/條件W 模式不執行（只在全市場掃描執行，避免每5分鐘重抓）')
    else:
        try:
            _scanned_tickers_all = [str(x[1]) for x in (buy_signals + sell_signals)]
            try:
                _scanned_tickers_all += list(US_STOCKS) + list(CRYPTO_LIST) + list(FX_LIST)
            except Exception:
                pass
            _wl_extra = scan_watchlist_extras(_scanned_tickers_all)
            if _wl_extra:
                buy_signals.extend(_wl_extra)
        except Exception as _e:
            print(f'  ⚠️ 觀察清單補掃呼叫失敗（{str(_e)[:40]}），不影響主掃描')

    print(f"  掃描完成！買進訊號：{len(buy_signals)}支 / 賣出訊號：{len(sell_signals)}支 / 下市警報：{len(delist_signals)}支")
    print(f"{'='*55}")
    # ✅ 方案②(07061319) 掃描結束保存今日跨輪快取（成長才存）
    save_cross_run_cache(_xrun_prev_w, _xrun_prev_d)
    # ✅ v05170940：寫入掃描狀態到Firebase供網頁版「上次掃描時間」顯示
    if LOAD_SENTINEL and _load_stats.get('scan_start'):
        _dur_min = (time.time() - _load_stats['scan_start']) / 60.0
        _ft = _load_stats['fetch_total']; _ff = _load_stats['fetch_fail']
        _fr = (_ff / _ft) if _ft > 0 else 0.0
        if _dur_min > LOAD_MAX_MIN or _fr > LOAD_FAIL_PCT:
            try:
                send_gmail("⚠️【負荷哨兵】掃描負荷過重警報",
                    f"本輪掃描耗時 {_dur_min:.1f} 分、5m抓取 {_ft} 次失敗 {_ff} 次（失敗率 {_fr*100:.0f}%）。\n"
                    f"門檻：耗時>{LOAD_MAX_MIN}分 或 失敗率>{LOAD_FAIL_PCT*100:.0f}%。\n"
                    f"建議：若持續，將 REALTIME_LAST_BAR 設False，或降低掃描頻率。",
                    urgent=True)
                print(f"⚠️ 負荷哨兵已寄發Gmail警報（耗時{_dur_min:.1f}分/失敗率{_fr*100:.0f}%）")
            except Exception as _e:
                print(f"負荷哨兵寄信失敗：{_e}")
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
            if notified[today].count(key) < 2:
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
    # ✅ v06160503修復：補定義 _signal_label（原版未定義→買賣通知主旨會NameError）
    #    依SCAN_MODE決定K別，與內文_ml預設邏輯一致；_get_period_label再轉長期/中期投資
    _signal_label = '週K' if SCAN_MODE == 'daily' else '月K'
    # ✅08091843 同一輪只判定一次時段，避免掃描跨越 15:00 時買賣被分到不同時段
    _sess_now = _signal_session()
    _sess_max = SIGNAL_MAX_DAY if _sess_now == 'day' else SIGNAL_MAX_NIGHT   # ✅08092144
    _feat('session', f"{'日盤' if _sess_now=='day' else '夜盤'}，每標的每方向上限 {_sess_max} 次")
    print(f"  🕒 本輪通知時段判定：{'日盤' if _sess_now=='day' else '夜盤'}"
          f"（每標的每方向上限 {_sess_max} 次）")

    if buy_signals:
        filtered = []

        for s in buy_signals:
            market, code, *_ = s
            # ✅08091843 額度改為【日盤/夜盤各自獨立】：key 帶時段標記，
            #   夜盤用掉的槽位不會排擠日盤（主帥 08/09 14:37 定案）。
            key = f"{market}_{code}_BUY_{_sess_now}"

            # ✅08091324【🔴A】改用 Firebase 原子佔位（跨雲端並行行程安全）
            #   原寫法只看本地 4_notified_today.json，雲端多個 job 各持一份，
            #   彼此看不見 → 同一支會被重複寄出。
            if _claim_notify_slot(key, today, notified, _sess_max):
                filtered.append(s)

        if filtered:
            body = f"掃描時間：{now_str}\n\n"

            for s in filtered:
                market, code, c, l, bb, r, rp = s[:7]
                _ml = s[7] if len(s) > 7 and isinstance(s[7], str) and s[7] in ('長期投資','中期投資') else ('中期投資' if SCAN_MODE=='daily' else '長期投資')
                body += (
                    f"{_wl_mark(code)}"          # ✅08100036【⚪F′】觀察清單命中標記
                    f"⭐【做多進場】⭐\n"
                    f"市場：{market}　代碼：{code}\n"
                    f"投資模式：{_ml}\n"
                    f"最低價：{l:.2f}　布林下緣：{bb:.2f}\n"
                    f"收盤價：{c:.2f}\n"
                    f"RSI：{rp:.1f} → {r:.1f}（↑上升）\n"
                    f"⚠️ 嚴禁用於當沖或隔日沖\n"
                    f"{'─'*30}\n"
                )

            send_gmail(f"☁️【雲端】⭐【{_get_period_label(_signal_label)}】做多進場 {_mk_summary(filtered)} - {now_str}", body)
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
            # ✅08091843 賣出同步改為【日盤/夜盤各自獨立】（與買進對稱）
            key = f"{market}_{code}_SELL_{_sess_now}"

            # ✅08091324【🔴A 對稱處理】賣出訊號有完全相同的雲端並行重複風險，
            #   只修買進會留半套，故一併改用原子佔位。
            if _claim_notify_slot(key, today, notified, _sess_max):
                filtered.append(s)

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

            send_gmail(f"☁️【雲端】🔔【{_get_period_label(_signal_label)}】出場訊號 {_mk_summary(filtered)} - {now_str}", body)
            save_notified(notified)
        else:
            print(f"🔕 賣出訊號 {len(sell_signals)-len(filtered)} 支已通知過")

# =====================
# 無訊號（只印Console，不發Gmail，避免通知疲乏）
# =====================
    if not buy_signals and not sell_signals and not delist_signals:
        print(f"📊 [{now_str}] 本次掃描無符合條件的股票，不發送通知。")

    # ✅08101455【修正】自報區塊改到【所有通知處理完之後】才印。
    #   真實事故：08100100 版把它掛在「掃描完成」那行後面，但通知處理（時段判定）
    #   是再之後才跑，導致自報區塊印出「通知時段判定：未執行到」，
    #   而 29 行之後它其實執行了 → ★自報區塊講了假話。
    #   一個「回報用」的功能若報錯，比沒有這個功能更危險（主帥 08/10 實測 log 發現）。
    print_feature_report()   # ✅08100100 新增／08101455 移位（鐵律AE3）

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

    # === [條件W 週選擇權做多模式]：TEST_MODE = 'condW' ✅ 07011049 純新增分支 ===

    if TEST_MODE == 'condW':
        print(f"🚀 條件W 週選擇權做多模式啟動（週二15:05~週三11:00／週四15:05~週五11:00）")
        scan_condition_w()
        time.sleep(3)
        exit()


    # === [期貨5分K模式]：TEST_MODE = '5mk' ===
    if TEST_MODE == '5mk':
        # 檢查是否在期貨交易時段
        tz_f = pytz.timezone('Asia/Taipei')
        now_f = datetime.now(tz_f)
        wd_f  = now_f.weekday()
        tv_f  = now_f.hour * 60 + now_f.minute

        # ══ ✅09021415【★★★期貨時窗與條件W 完全綁定】主帥 2026/09/02 14:00 指令 ══
        #   ★主帥原話：「條件W的週選擇權，既然都延長到11:30，期貨也要改成到11:30。
        #     而且觸發日也要改成和條件W一樣，★★週一不再觸發。
        #     ★週二和週四，和條件W一樣，15:05開始。★週三和週五，也和條件W一樣，到11:30。」
        #
        #   ★★★實作採【零分歧設計】（ＡＭ１９）：★不再自己寫一份時間判斷式，
        #     ★直接呼叫 _condw_current_window()。★★兩者結構上【不可能再漂移】。
        #   ★★這是今天整起事件的根本解：2026/09/02 之所以出現
        #     「條件W 週三11:00／期貨週三11:30／期貨週五11:00」三個不同的邊界，
        #     ★★★正是因為【同一個時窗被抄成三份】，改一份忘了改另外兩份。
        #
        #   ★★【重要副作用，已對主帥完整揭露】週一全天、週二/週四日盤、
        #     週三/週五 11:30 之後，期貨5分K與15分K【不再掃描】。★這是主帥的明確指令。
        _condw_win = _condw_current_window()
        in_futures = _condw_win is not None

        # ══ ✅09022055【★★★出場閘門：只受「有沒有持倉」管，不受進場時窗管】══
        #   ★主帥 2026/09/02 15:02 指令的核心：
        #     「09:05~11:30建倉，11:31後就不再觸發平倉訊號？那與放著等破產有何不同！」
        #   ★★設計原則（已立為凍結清單 R-14）：
        #     ★進場受時窗管（條件W 窗）；★★出場【不受任何時窗管】。
        #   ★★★先讀回 Firestore 的持倉狀態，★否則每次執行都從空手開始（原缺陷）。
        _load_futures_position()
        _can_exit = ((_futures_is_holding or _futures_is_short)
                     and _txf_data_window(wd_f, tv_f))
        # ✅09022155 收盤前提醒信專屬時窗：★13:20~13:44，★★不需行情資料，
        #   ★★★故【不受 _txf_data_window 的 13:30 上限限制】。
        _can_alert = (FUTURES_CLOSE_ALERT_ENABLED
                      and (_futures_is_holding or _futures_is_short)
                      and 0 <= wd_f <= 4
                      and FUTURES_CLOSE_ALERT_START <= tv_f <= FUTURES_CLOSE_ALERT_END)
        if _can_exit and not in_futures:
            print(f"[{test_now}] 📌 不在條件W窗內，★但有持倉 → 【只掃平倉，不掃進場】")

        # ══ ✅09021415【台股盤中極端異動 獨立時窗】★★★不得被期貨時窗綁架 ══
        #   ★真實風險（本輪 ＡＭ７② 全域排程相容性檢查抓到）：
        #     check_tw_intraday_extreme()（F-12）原本【寄生在期貨5mk 分支裡】。
        #     ★★若期貨時窗依主帥指令縮成條件W 窗，★週一全天、週二/週四日盤、
        #     ★★★週三/週五11:30~13:30 的台股極端異動偵測會【一併消失】。
        #   ★F-12 是台股功能，與週選擇權無關，★★故必須有自己的時窗。
        #   ★★台股日盤 09:05~13:30，週一~週五（起始 09:05 不可改 09:00，R-02）。
        in_tw_extreme = (TW_INTRADAY_EXTREME_ENABLED
                         and 0 <= wd_f <= 4
                         and (9*60+5) <= tv_f <= (13*60+30))

        if not (in_futures or in_tw_extreme or _can_exit or _can_alert):
            print(f"[{test_now}] ❌ 非條件W時窗、亦非台股日盤，直接結束"
                  f"（條件W窗：週二15:05~週三11:30／週四15:05~週五11:30）")
            time.sleep(5)
            exit()

        print(f"🚀 期貨5分K模式啟動"
              f"（條件W窗：{_condw_win if _condw_win else '不在窗內'}／台股極端異動：{'是' if in_tw_extreme else '否'}）")
        print(f"   標的：{FUTURES_5MK_TARGETS}　間隔：{FUTURES_5MK_INTERVAL}秒")
        print(f"   專屬帳號：{FUTURES_5MK_OWNER}")

        # 持續掃描直到週三11:30結束
        while True:
            now_loop = datetime.now(pytz.timezone('Asia/Taipei'))
            wd_l  = now_loop.weekday()
            tv_l  = now_loop.hour * 60 + now_loop.minute
            _is_night = (wd_l == 1 and 1*60 <= tv_l < 5*60)  # 週二深夜01~05
            # ✅09021415 迴圈閘門同樣改綁 _condw_current_window()（零分歧設計．ＡＭ１９）
            #   ★★★R-12 明列「改動時六處全要動，缺一即不完整」——本處即其一。
            _tw_ext_l = (TW_INTRADAY_EXTREME_ENABLED and 0 <= wd_l <= 4
                         and (9*60+5) <= tv_l <= (13*60+30))
            # ✅09022055 出場閘門併入：★有持倉 → 資料源可用時段內一律繼續掃平倉
            #   ★★★原式只保留「深夜有持倉」一種情形，★日盤 11:31~13:30 有持倉會斷線。
            _exit_l = ((_futures_is_holding or _futures_is_short)
                       and _txf_data_window(wd_l, tv_l))
            _alert_l = (FUTURES_CLOSE_ALERT_ENABLED
                        and (_futures_is_holding or _futures_is_short)
                        and 0 <= wd_l <= 4
                        and FUTURES_CLOSE_ALERT_START <= tv_l <= FUTURES_CLOSE_ALERT_END)
            in_f  = (
                (_condw_current_window() is not None) or
                _tw_ext_l or
                _exit_l or                           # ✅09022055 有持倉→繼續掃平倉
                _alert_l or                          # ✅09022155 收盤前未平倉提醒
                (_is_night and _futures_is_holding)  # ✅ 深夜有持倉→繼續掃平倉（原有，保留）
            )
            if not in_f:
                print(f"✅ 條件W時窗與台股日盤均已結束，監控結束")
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


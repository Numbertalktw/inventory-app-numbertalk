import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime, timedelta
import time

# ==========================================
# 1. 系統基礎設定
# ==========================================
PAGE_TITLE = "numbertalk 雲端庫存系統"
SPREADSHEET_NAME = "numbertalk-system"

WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]
CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品", "數字珠", "數字串", "香料", "手作設備"]
SERIES = ["原料", "半成品", "成品", "包材", "生命數字能量項鍊", "數字手鍊", "貼紙", "小卡", "火漆章", "能量蠟燭", "香包", "水晶", "魔法鹽"]
KEYERS = ["千畇", "James", "Imeng", "小幫手"]
SHIPPING_METHODS = ["郵局", "i郵箱", "全家", "7-11", "自取"]

ORDER_STATUSES = ["已確認", "未付款/未出貨", "已付款/未出貨", "未付款/已出貨", "已完成"]
ORDER_STATUS_COLORS = {
    "已確認": "🟡", "未付款/未出貨": "🔴", "已付款/未出貨": "🟠",
    "未付款/已出貨": "🔵", "已完成": "🟢",
    "已成立": "🟡", "待處理": "🟡", "處理中": "🔵", "已出貨": "🟠", "已取消": "⚫"
}

PREFIX_MAP = {
    "生命數字能量項鍊": "SN", "數字手鍊": "SB", "貼紙": "ST", "小卡": "CD",
    "火漆章": "FS", "能量蠟燭": "LA", "香包": "SB", "水晶": "CT", "魔法鹽": "MS",
    "天然石": "NS", "金屬配件": "MT", "線材": "WR", "包裝材料": "PK", "完成品": "PD"
}

# ==========================================
# 1.5 數字學計算工具（五階段數 / 流年）
# ==========================================
def _digit_sum(n):
    return sum(int(d) for d in str(n))

def _reduce_chain(n):
    chain = [n]
    while n >= 10:
        n = _digit_sum(n)
        chain.append(n)
    return chain

def calc_liunian(year, birth_month, birth_day):
    """流年 = 年份所有數字 + 生日月 + 生日日，縮減"""
    digits_str = str(year) + str(birth_month) + str(birth_day)
    total = sum(int(d) for d in digits_str)
    chain = _reduce_chain(total)
    return "/".join(str(x) for x in chain)

def calc_jieduan(birth_year, birth_month):
    """階段數 = 出生年所有數字 + 生日月，縮減"""
    digits_str = str(birth_year) + str(birth_month)
    total = sum(int(d) for d in digits_str)
    chain = _reduce_chain(total)
    return "/".join(str(x) for x in chain)

def personal_year_range(birth_month, birth_day, today=None):
    """依生日是否已過決定三個年份"""
    if today is None:
        today = datetime.now().date()
    birthday_passed = (today.month, today.day) >= (birth_month, birth_day)
    personal_year = today.year if birthday_passed else today.year - 1
    return [personal_year - 1, personal_year, personal_year + 1]

def parse_birthday(bday_str):
    """解析生日字串 YYYY/MM/DD 或 YYYY-MM-DD"""
    if not bday_str or not str(bday_str).strip():
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            d = datetime.strptime(str(bday_str).strip(), fmt)
            return d.year, d.month, d.day
        except ValueError:
            pass
    return None

def format_phone(phone_val):
    """確保電話號碼保留前導零（台灣手機 09xx）"""
    s = str(phone_val).strip()
    if not s or s in ('', 'nan', 'None', '0', '0.0'):
        return ''
    # 移除小數點（Google Sheets 可能存成 9.19515513E8 之類）
    if '.' in s:
        try:
            s = str(int(float(s)))
        except (ValueError, OverflowError):
            pass
    # 如果是純數字、長度 9、以 9 開頭 → 台灣手機號碼缺少前導 0
    digits = s.replace('-', '').replace(' ', '')
    if digits.isdigit() and len(digits) == 9 and digits.startswith('9'):
        return '0' + digits
    return s

def render_numerology_table(bday_str, lunar_bday_str="", key_prefix=""):
    """參考 IF Crystal 格式：顯示三年流年 x 階段數對照表（國曆 + 農曆並排）"""
    parsed = parse_birthday(bday_str)
    if not parsed:
        # 如果只有農曆生日，用農曆生日來計算
        lunar_only = parse_birthday(lunar_bday_str) if lunar_bday_str else None
        if not lunar_only:
            st.info("請輸入生日（格式: YYYY/MM/DD）以顯示數字能量")
            return False
        # 只有農曆 → 只顯示農曆階段數
        ly, lm, ld = lunar_only
        lunar_jieduan = calc_jieduan(ly, lm)
        lunar_jd_final = lunar_jieduan.split("/")[-1]
        st.markdown("##### 📊 流年 × 階段數 三年對照表")
        st.markdown(f"**🌙 農曆階段數：** `{lunar_jd_final}`　（{ly}年 + {lm}月 → {lunar_jieduan}）")
        st.markdown("*（未填國曆生日，僅顯示農曆階段數）*")
        return True
    by, bm, bd = parsed
    years = personal_year_range(bm, bd)
    labels = ["去年", "今年", "明年"]
    jieduan = calc_jieduan(by, bm)
    jd_final = jieduan.split("/")[-1]

    lunar_parsed = parse_birthday(lunar_bday_str) if lunar_bday_str else None
    lunar_jd_final = ""
    lunar_jieduan = ""
    if lunar_parsed:
        ly, lm, ld = lunar_parsed
        lunar_jieduan = calc_jieduan(ly, lm)
        lunar_jd_final = lunar_jieduan.split("/")[-1]

    st.markdown("##### 📊 流年 × 階段數 三年對照表")

    col_solar, col_lunar = st.columns(2)
    with col_solar:
        st.markdown(f"**🌞 國曆階段數：** `{jd_final}`　（{by}年 + {bm}月 → {jieduan}）")
    with col_lunar:
        if lunar_parsed:
            st.markdown(f"**🌙 農曆階段數：** `{lunar_jd_final}`　（{ly}年 + {lm}月 → {lunar_jieduan}）")
        else:
            st.markdown("**🌙 農曆階段數：** *未填寫農曆生日*")

    rows = []
    for yr, lbl in zip(years, labels):
        ln = calc_liunian(yr, bm, bd)
        ln_final = ln.split("/")[-1]
        row_data = {
            "年份": f"{yr}（{lbl}）",
            "流年計算": f"{yr}年 + {bm}月 + {bd}日 → {ln}",
            "流年數": ln_final,
            "國曆階段數計算": f"{by}年 + {bm}月 → {jieduan}",
            "國曆階段數": jd_final,
        }
        if lunar_parsed:
            row_data["農曆階段數計算"] = f"{ly}年 + {lm}月 → {lunar_jieduan}"
            row_data["農曆階段數"] = lunar_jd_final
        rows.append(row_data)
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    return True

# ==========================================
# 2. Google Sheet 連線核心
# ==========================================
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

@st.cache_resource
def get_client():
    try:
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
        return gspread.authorize(creds)
    except: return None

@st.cache_resource
def get_spreadsheet():
    client = get_client()
    if not client: return None
    try: return client.open(SPREADSHEET_NAME)
    except: return None

def get_worksheet(sheet_name):
    sh = get_spreadsheet()
    if not sh: return None
    try: return sh.worksheet(sheet_name)
    except: return None

def get_fresh_client():
    """建立全新 gspread 連線(不快取),確保寫入時 token 有效"""
    try:
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
        return gspread.authorize(creds)
    except:
        return None

def get_worksheet_for_write(sheet_name):
    """取得寫入專用 worksheet(全新連線,避免 token 過期問題)"""
    client = get_fresh_client()
    if not client:
        return None
    try:
        sh = client.open(SPREADSHEET_NAME)
        return sh.worksheet(sheet_name)
    except:
        return None

@st.cache_data(ttl=120)
def load_data(sheet_name):
    import time as _time
    ws = get_worksheet(sheet_name)
    if ws is None:
        return pd.DataFrame()
    for _attempt in range(3):
        try:
            data = ws.get_all_records()
            df = pd.DataFrame(data)
            for col in ['sku', 'name', 'category', 'series', 'spec', 'color', 'note', 'price']:
                if col not in df.columns:
                    df[col] = ""
            return df.fillna("")
        except Exception as _api_err:
            if '429' in str(_api_err) and _attempt < 2:
                _time.sleep(3 + _attempt * 3)
                continue
            elif '429' in str(_api_err):
                # 429 錯誤不要快取空結果，直接 raise 讓 Streamlit 不快取
                raise
            return pd.DataFrame()
    return pd.DataFrame()

def clear_cache():
    load_data.clear()
    load_product_prices.clear()
    get_spreadsheet.clear()
    get_client.clear()

# ==========================================
# 3. 核心功能函式
# ==========================================

def ensure_price_column():
    if st.session_state.get('_price_col_ok'):
        return
    ws = get_worksheet("Products")
    if not ws:
        return
    try:
        header = ws.row_values(1)
        if 'price' in header:
            st.session_state['_price_col_ok'] = True
            return
        expected = ['sku', 'series', 'category', 'name', 'spec', 'color', 'note']
        if header[:7] == expected:
            if len(header) < 8 or header[7] == '':
                ws.update_cell(1, 8, 'price')
            else:
                ws.update_cell(1, len(header) + 1, 'price')
        else:
            ws.update_cell(1, len(header) + 1, 'price')
        st.session_state['_price_col_ok'] = True
    except:
        pass

@st.cache_data(ttl=300)
def load_product_prices():
    ws = get_worksheet("Products")
    if not ws:
        return {}
    try:
        header = ws.row_values(1)
        if 'price' not in header:
            return {}
        price_col = header.index('price') + 1
        skus = ws.col_values(1)[1:]
        prices = ws.col_values(price_col)[1:]
        result = {}
        for i in range(len(skus)):
            p = prices[i] if i < len(prices) else ''
            try:
                result[str(skus[i])] = float(p) if p not in ['', None] else 0.0
            except (ValueError, TypeError):
                result[str(skus[i])] = 0.0
        return result
    except:
        return {}

def get_formatted_product_df():
    df = load_data("Products")
    if df.empty: return df
    try:
        price_map = load_product_prices()
        df['price'] = df['sku'].astype(str).map(price_map).fillna(0.0)
    except:
        df['price'] = 0.0
    df['sku'] = df['sku'].astype(str)
    df['name'] = df['name'].astype(str)
    df['label'] = df['sku'] + " | " + df['name'] + " (" + df['spec'].astype(str) + " / " + df['color'].astype(str) + ")"
    return df

def update_stock_qty(sku, warehouse, delta_qty):
    ws = get_worksheet_for_write("Stock")
    if not ws: return
    try:
        all_vals = ws.get_all_values()
        header = all_vals[0]
        sku_idx, wh_idx, qty_idx = header.index("sku"), header.index("warehouse"), header.index("qty")
        row_idx = -1
        for i, row in enumerate(all_vals[1:], 2):
            if str(row[sku_idx]) == str(sku) and str(row[wh_idx]) == str(warehouse):
                row_idx = i
                current_val = float(row[qty_idx]) if row[qty_idx] else 0.0
                break
        if row_idx > 0:
            ws.update_cell(row_idx, qty_idx + 1, current_val + delta_qty)
        else:
            ws.append_row([str(sku), warehouse, delta_qty])
    except: pass

def add_transaction(doc_type, date_str, sku, wh, qty, user, note, ship_method="", ship_no="", cost=0):
    ws_hist = get_worksheet_for_write("History")
    if not ws_hist:
        return False
    df_p = load_data("Products")
    p_name = ""
    if not df_p.empty:
        match = df_p[df_p['sku'].astype(str) == str(sku)]
        if not match.empty: p_name = match.iloc[0]['name']

    prefix = {"進貨":"IN", "銷售出貨":"OUT", "製造領料":"MO", "製造入庫":"PD", "移庫(撥出)":"TR-O", "移庫(撥入)":"TR-I"}.get(doc_type, "ADJ")
    doc_no = f"{prefix}-{int(time.time())}"
    try:
        ws_hist.append_row([
            doc_type, doc_no, str(date_str), str(sku), wh, float(qty),
            user, note, float(cost), str(datetime.now()), p_name, ship_method, ship_no
        ])
        factor = -1 if doc_type in ['銷售出貨', '製造領料', '移庫(撥出)'] else 1
        update_stock_qty(sku, wh, float(qty) * factor)
        clear_cache()
        return True
    except: return False

def delete_transaction(doc_no):
    ws_hist = get_worksheet_for_write("History")
    if not ws_hist:
        return False
    try:
        cells = ws_hist.findall(str(doc_no))
        if not cells: return False
        for cell in reversed(cells):
            row_num = cell.row
            record = ws_hist.row_values(row_num)
            r_type, r_sku, r_wh, r_qty = record[0], record[3], record[4], float(record[5])
            reverse_factor = 1 if r_type in ['銷售出貨', '製造領料', '移庫(撥出)'] else -1
            update_stock_qty(r_sku, r_wh, r_qty * reverse_factor)
            ws_hist.delete_rows(row_num)
        clear_cache()
        return True
    except: return False

def generate_auto_sku(series, category, existing_skus_set):
    prefix = PREFIX_MAP.get(series, PREFIX_MAP.get(category, "XX"))
    count = 1
    while True:
        candidate = f"{prefix}-{count:03d}"
        if candidate not in existing_skus_set: return candidate
        count += 1
        if count > 999: return f"{prefix}-{int(time.time())}"

def add_product(sku, name, category, series, spec, note, color, price=0):
    ensure_price_column()
    ws = get_worksheet_for_write("Products")
    if not ws:
        return False, "連線錯誤"
    try:
        header = ws.row_values(1)
        row_data = [''] * len(header)
        col_map = {h: i for i, h in enumerate(header)}
        for key, val in [('sku', str(sku)), ('series', series), ('category', category),
                         ('name', name), ('spec', spec), ('color', color),
                         ('note', note), ('price', float(price))]:
            if key in col_map:
                row_data[col_map[key]] = val
        ws.append_row(row_data)
        ws_stock = get_worksheet_for_write("Stock")
        if ws_stock:
            ws_stock.append_rows([[str(sku), wh, 0.0] for wh in WAREHOUSES])
        clear_cache()
        return True, "新增成功"
    except: return False, "連線錯誤"

def update_product(sku, new_data):
    ensure_price_column()
    ws = get_worksheet_for_write("Products")
    if not ws:
        return False
    try:
        header = ws.row_values(1)
        col_map = {h: i + 1 for i, h in enumerate(header)}
        cell = ws.find(str(sku))
        row = cell.row
        for key in ['name', 'spec', 'color', 'note', 'price']:
            if key in new_data and key in col_map:
                val = float(new_data[key]) if key == 'price' else new_data[key]
                ws.update_cell(row, col_map[key], val)
        clear_cache()
        return True
    except: return False

def get_stock_overview():
    df_prod = load_data("Products")
    df_stock = load_data("Stock")
    if df_prod.empty: return pd.DataFrame()
    df_prod['sku'] = df_prod['sku'].astype(str)
    if df_stock.empty:
        result = df_prod.copy()
        for wh in WAREHOUSES: result[wh] = 0.0
        result['總庫存'] = 0.0
    else:
        df_stock['sku'] = df_stock['sku'].astype(str)
        df_stock['qty'] = pd.to_numeric(df_stock['qty'], errors='coerce').fillna(0)
        pivot = df_stock.pivot_table(index='sku', columns='warehouse', values='qty', aggfunc='sum').fillna(0)
        for wh in WAREHOUSES:
            if wh not in pivot.columns: pivot[wh] = 0.0
        pivot['總庫存'] = pivot[WAREHOUSES].sum(axis=1)
        result = pd.merge(df_prod, pivot, on='sku', how='left').fillna(0)
    target_cols = ['sku', 'series', 'category', 'name', 'spec', 'color', 'price', 'note', '總庫存'] + WAREHOUSES
    return result[[c for c in target_cols if c in result.columns]]

# ==========================================
# 3.5 訂單系統核心功能
# ==========================================

def ensure_order_sheets():
    if st.session_state.get('_order_sheets_ok'):
        return
    sh = get_spreadsheet()
    if not sh:
        return
    try:
        existing = [ws.title for ws in sh.worksheets()]
        if "Orders" not in existing:
            ws = sh.add_worksheet(title="Orders", rows=1000, cols=20)
            ws.append_row(["order_no", "order_date", "customer_name",
                           "birthday", "lunar_birthday", "birth_time",
                           "customer_phone", "customer_email", "shipping_address",
                           "status", "items_detail", "note",
                           "total_amount", "items_total", "discount", "shipping_fee",
                           "created_by", "created_at"])
        if "OrderItems" not in existing:
            ws = sh.add_worksheet(title="OrderItems", rows=5000, cols=8)
            ws.append_row(["order_no", "sku", "product_name", "qty", "unit_price",
                           "subtotal", "warehouse"])
        st.session_state['_order_sheets_ok'] = True
    except Exception:
        pass

def ensure_extra_columns():
    """確保既有的 Orders / Members 工作表含有 birthday, lunar_birthday, birth_time 欄位"""
    if st.session_state.get('_extra_cols_ok'):
        return
    try:
        for sheet_name in ["Orders", "Members"]:
            ws_w = get_worksheet_for_write(sheet_name)
            if not ws_w:
                continue
            cur_header = ws_w.row_values(1)
            extra = ["birthday", "lunar_birthday", "birth_time"]
            if sheet_name == "Orders":
                extra.append("items_detail")
            missing = [c for c in extra if c not in cur_header]
            if missing:
                needed_total = len(cur_header) + len(missing)
                if ws_w.col_count < needed_total:
                    ws_w.resize(cols=needed_total + 5)
                for col_name in missing:
                    ws_w.update_cell(1, len(cur_header) + 1, col_name)
                    cur_header.append(col_name)

        st.session_state['_extra_cols_ok'] = True
    except Exception:
        pass

DESIRED_ORDER_COLUMNS = [
    "order_no", "order_date", "customer_name",
    "birthday", "lunar_birthday", "birth_time",
    "customer_phone", "customer_email", "shipping_address",
    "status", "items_detail", "note",
    "total_amount", "items_total", "discount", "shipping_fee",
    "created_by", "created_at"
]

# 中文→英文 欄位名稱對應（用於識別使用者手動改名的重複欄位）
_CN_TO_EN_COL = {
    "優惠折扣": "discount",
    "運費": "shipping_fee",
    "應付總額": "total_amount",
    "商品小計": "items_total",
    "訂單編號": "order_no",
    "訂單日期": "order_date",
    "客戶名稱": "customer_name",
    "生日": "birthday",
    "農曆生日": "lunar_birthday",
    "出生時間": "birth_time",
    "電話": "customer_phone",
    "信箱": "customer_email",
    "地址": "shipping_address",
    "狀態": "status",
    "品項明細": "items_detail",
    "備註": "note",
    "建立者": "created_by",
    "建立時間": "created_at",
}


def reorganize_orders_columns():
    """一次性遷移：重整 Orders 工作表欄位順序、移除重複欄位"""
    client = get_fresh_client()
    if not client:
        return False, "Google 連線失敗"
    try:
        sh = client.open(SPREADSHEET_NAME)
        ws = sh.worksheet("Orders")
    except Exception as e:
        return False, f"無法開啟工作表: {e}"

    all_data = ws.get_all_values()
    if not all_data or len(all_data) < 2:
        return False, "工作表為空或只有標題列"

    header = all_data[0]
    rows = all_data[1:]

    # ── 1. 將每個欄位標題對應到標準英文名稱 ──
    col_groups = {}
    for idx, name in enumerate(header):
        canonical = _CN_TO_EN_COL.get(name, name)
        if canonical not in col_groups:
            col_groups[canonical] = []
        col_groups[canonical].append((idx, name))

    # ── 2. 重複欄位只保留資料最多的那一欄 ──
    best_col = {}
    for canonical, positions in col_groups.items():
        if len(positions) == 1:
            best_col[canonical] = positions[0][0]
        else:
            best_idx, best_count = positions[0][0], -1
            for pos_idx, _ in positions:
                count = sum(1 for row in rows if pos_idx < len(row) and str(row[pos_idx]).strip())
                if count > best_count:
                    best_count = count
                    best_idx = pos_idx
            best_col[canonical] = best_idx

    # ── 3. 組裝新資料（依 DESIRED_ORDER_COLUMNS 排列，不交換值） ──
    new_header = list(DESIRED_ORDER_COLUMNS)
    new_rows = []
    for row in rows:
        new_row = []
        for col_name in DESIRED_ORDER_COLUMNS:
            src_idx = best_col.get(col_name)
            if src_idx is not None and src_idx < len(row):
                new_row.append(row[src_idx])
            else:
                new_row.append("")
        new_rows.append(new_row)

    # ── 4. 清除並重寫工作表 ──
    ws.clear()
    ws.update('A1', [new_header] + new_rows, value_input_option='RAW')

    # 縮減多餘欄位
    if ws.col_count > len(new_header) + 2:
        ws.resize(cols=len(new_header))

    clear_cache()
    return True, f"完成！共整理 {len(new_rows)} 筆訂單，欄位數 {len(new_header)}"


def batch_recalc_all_orders(progress_cb=None):
    """批次重算所有訂單金額（從 OrderItems 重新加總，搭配 discount / shipping_fee 算出應付總額）

    total_amount = 應付總額（items_total − discount + shipping_fee），items_total = 商品小計
    同時刷新 items_detail 摘要欄位。
    使用單次讀 + 單次寫，最大限度減少 API 呼叫。

    Args:
        progress_cb: 可選的回呼函式 progress_cb(current, total) 用於更新進度條
    Returns:
        (ok: bool, message: str, updated_count: int)
    """
    import time as _bt

    client = get_fresh_client()
    if not client:
        return False, "Google 連線失敗", 0
    try:
        sh = client.open(SPREADSHEET_NAME)
        ws_orders = sh.worksheet("Orders")
        ws_items = sh.worksheet("OrderItems")
    except Exception as e:
        return False, f"無法開啟工作表: {e}", 0

    # ── 1. 一次讀取所有資料 ──
    orders_data = ws_orders.get_all_values()
    items_data = ws_items.get_all_values()

    if not orders_data or len(orders_data) < 2:
        return False, "沒有訂單資料", 0

    o_header = orders_data[0]
    o_rows = [list(r) for r in orders_data[1:]]  # 深拷貝以便修改

    o_col = {h: i for i, h in enumerate(o_header)}
    required = ['order_no', 'total_amount', 'items_total', 'discount', 'shipping_fee']
    missing = [c for c in required if c not in o_col]
    if missing:
        return False, f"缺少欄位: {', '.join(missing)}\n請先執行「重整訂單欄位順序」", 0

    # ── 2. 從 OrderItems 建立品項對照表 ──
    i_header = items_data[0] if items_data else []
    i_rows = items_data[1:] if len(items_data) > 1 else []
    i_col = {h: i for i, h in enumerate(i_header)}

    def _sf(val):
        """safe float"""
        try:
            return float(val) if val not in ('', None) else 0.0
        except (ValueError, TypeError):
            return 0.0

    # order_no → [{ product_name, qty, subtotal }, ...]
    order_items_map = {}
    for ir in i_rows:
        ono = ir[i_col.get('order_no', 0)] if i_col.get('order_no', 0) < len(ir) else ''
        if not ono:
            continue
        pn_i = i_col.get('product_name', 2)
        qt_i = i_col.get('qty', 3)
        sb_i = i_col.get('subtotal', 5)
        pname = ir[pn_i] if pn_i < len(ir) else ''
        qty = _sf(ir[qt_i] if qt_i < len(ir) else 0)
        sub = _sf(ir[sb_i] if sb_i < len(ir) else 0)
        order_items_map.setdefault(ono, []).append({
            'product_name': pname, 'qty': qty, 'subtotal': sub
        })

    # ── 3. 逐筆重算 ──
    # total_amount = 應付總額（最終金額），items_total = 商品小計（品項原始加總）
    ta_i = o_col['total_amount']       # → 應付總額
    it_i = o_col['items_total']        # → 商品小計
    disc_i = o_col['discount']
    ship_i = o_col['shipping_fee']
    detail_i = o_col.get('items_detail')

    updated = 0
    total_orders = len(o_rows)

    for idx, row in enumerate(o_rows):
        # 確保 row 長度足夠
        while len(row) < len(o_header):
            row.append('')

        ono = row[o_col['order_no']]
        if not ono:
            continue

        discount = _sf(row[disc_i])
        shipping = _sf(row[ship_i])

        items = order_items_map.get(ono, [])
        items_sum = sum(it['subtotal'] for it in items)

        # 沒有 OrderItems 但已有金額 → 保留現有值（可能是手動建立的訂單）
        if not items and (_sf(row[ta_i]) > 0 or _sf(row[it_i]) > 0):
            if progress_cb:
                progress_cb(idx + 1, total_orders)
            continue

        final_total = items_sum - discount + shipping

        # total_amount = 應付總額，items_total = 商品小計
        changed = False
        if abs(_sf(row[ta_i]) - final_total) > 0.01:
            row[ta_i] = final_total
            changed = True
        if abs(_sf(row[it_i]) - items_sum) > 0.01:
            row[it_i] = items_sum
            changed = True

        # 更新 items_detail 摘要
        if detail_i is not None and items:
            summary = ", ".join(
                [f"{it['product_name']} x{int(it['qty'])}" for it in items if it['product_name']]
            )
            if summary and row[detail_i] != summary:
                row[detail_i] = summary
                changed = True

        if changed:
            updated += 1

        if progress_cb:
            progress_cb(idx + 1, total_orders)

    # ── 4. 整批寫回（單次 API 呼叫） ──
    write_data = [o_header]
    for row in o_rows:
        write_data.append(row[:len(o_header)])

    ws_orders.update('A1', write_data, value_input_option='RAW')
    clear_cache()

    return True, f"批次重算完成！已更新 {updated}/{total_orders} 筆訂單", updated


def generate_order_no():
    now = datetime.now()
    return f"ORD-{now.strftime('%Y%m%d')}-{int(time.time()) % 100000:05d}"

def create_order(order_no, order_date, customer_name, customer_phone,
                 customer_email, shipping_address, items, note, created_by,
                 discount=0, shipping_fee=0, birthday="", lunar_birthday="", birth_time=""):
    ensure_order_sheets()
    ensure_extra_columns()
    ws_orders = get_worksheet_for_write("Orders")
    ws_items = get_worksheet_for_write("OrderItems")
    if not ws_orders or not ws_items:
        return False, "無法連線到工作表"
    try:
        items_total = sum(item['subtotal'] for item in items)
        total = items_total - float(discount) + float(shipping_fee)
        header = ws_orders.row_values(1)
        row_data = [''] * len(header)
        col_map = {h: i for i, h in enumerate(header)}
        field_vals = {
            'order_no': order_no, 'order_date': str(order_date),
            'customer_name': customer_name, 'customer_phone': customer_phone,
            'customer_email': customer_email, 'shipping_address': shipping_address,
            'status': "已確認", 'total_amount': float(total),
            'note': note, 'created_by': created_by, 'created_at': str(datetime.now()),
            'discount': float(discount), 'shipping_fee': float(shipping_fee),
            'items_total': float(items_total),
            'birthday': str(birthday), 'lunar_birthday': str(lunar_birthday),
            'birth_time': str(birth_time),
            'items_detail': ", ".join([f"{it['product_name']} x{int(it['qty'])}" for it in items])
        }
        for k, v in field_vals.items():
            if k in col_map:
                row_data[col_map[k]] = v
        ws_orders.append_row(row_data, value_input_option='RAW')
        for item in items:
            ws_items.append_row([
                order_no, item['sku'], item['product_name'],
                float(item['qty']), float(item['unit_price']),
                float(item['subtotal']), item['warehouse']
            ])
        clear_cache()
        save_member(customer_name, customer_phone, customer_email, shipping_address,
                    birthday=birthday, lunar_birthday=lunar_birthday, birth_time=birth_time)
        return True, f"訂單 {order_no} 建立成功"
    except Exception as e:
        return False, f"建立失敗: {e}"

def load_orders():
    ensure_order_sheets()
    ensure_extra_columns()
    df = load_data("Orders")
    if df.empty:
        return pd.DataFrame(columns=["order_no", "order_date", "customer_name",
                                      "customer_phone", "customer_email",
                                      "shipping_address", "status", "total_amount",
                                      "note", "created_by", "created_at"])
    df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce').fillna(0)
    for col in ['discount', 'shipping_fee', 'items_total']:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    for col in ['birthday', 'lunar_birthday', 'birth_time']:
        if col not in df.columns:
            df[col] = ""
    if 'customer_phone' in df.columns:
        df['customer_phone'] = df['customer_phone'].apply(format_phone)
    return df

def load_order_items(order_no=None):
    ensure_order_sheets()
    df = load_data("OrderItems")
    if df.empty:
        return pd.DataFrame(columns=["order_no", "sku", "product_name",
                                      "qty", "unit_price", "subtotal", "warehouse"])
    df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0)
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce').fillna(0)
    df['subtotal'] = pd.to_numeric(df['subtotal'], errors='coerce').fillna(0)
    if order_no:
        df = df[df['order_no'].astype(str) == str(order_no)]
    return df

def update_order_status(order_no, new_status):
    """更新訂單狀態 - 使用全新連線確保寫入成功"""
    ws = get_worksheet_for_write("Orders")
    if not ws:
        st.error("無法連線到 Orders 工作表")
        return False
    try:
        all_vals = ws.get_all_values()
        header = all_vals[0]
        no_idx = header.index("order_no")
        st_idx = header.index("status")
        for i, row in enumerate(all_vals[1:], 2):
            if str(row[no_idx]) == str(order_no):
                ws.update_cell(i, st_idx + 1, new_status)
                clear_cache()
                return True
        st.error(f"找不到訂單 {order_no}")
        return False
    except Exception as e:
        st.error(f"狀態更新失敗: {e}")
        return False

def update_order_note(order_no, new_note):
    """更新訂單備註"""
    ws = get_worksheet_for_write("Orders")
    if not ws:
        st.error("無法連線到 Orders 工作表")
        return False
    try:
        all_vals = ws.get_all_values()
        header = all_vals[0]
        no_idx = header.index("order_no")
        note_idx = header.index("note")
        for i, row in enumerate(all_vals[1:], 2):
            if str(row[no_idx]) == str(order_no):
                ws.update_cell(i, note_idx + 1, new_note)
                clear_cache()
                return True
        st.error(f"找不到訂單 {order_no}")
        return False
    except Exception as e:
        st.error(f"備註更新失敗: {e}")
        return False

def update_order_fields(order_no, fields_dict):
    """更新訂單的多個欄位(客戶資訊、折扣、運費等)"""
    ensure_extra_columns()
    ws = get_worksheet_for_write("Orders")
    if not ws:
        st.error("無法連線到 Orders 工作表")
        return False
    try:
        all_vals = ws.get_all_values()
        header = all_vals[0]
        no_idx = header.index("order_no")
        # 如果欄位不在 header，動態新增（先擴展欄數）
        missing_fields = [f for f in fields_dict.keys() if f not in header]
        if missing_fields:
            needed_total = len(header) + len(missing_fields)
            if ws.col_count < needed_total:
                ws.resize(cols=needed_total + 5)
            for field_name in missing_fields:
                ws.update_cell(1, len(header) + 1, field_name)
                header.append(field_name)
        for i, row in enumerate(all_vals[1:], 2):
            if str(row[no_idx]) == str(order_no):
                for field_name, field_value in fields_dict.items():
                    if field_name in header:
                        col_idx = header.index(field_name)
                        ws.update_cell(i, col_idx + 1, field_value)
                clear_cache()
                return True
        st.error(f"找不到訂單 {order_no}")
        return False
    except Exception as e:
        st.error(f"訂單更新失敗: {e}")
        return False

def add_order_item(order_no, sku, product_name, qty, unit_price, warehouse):
    """新增訂單品項"""
    ws = get_worksheet_for_write("OrderItems")
    if not ws:
        st.error("無法連線到 OrderItems 工作表")
        return False
    try:
        subtotal = float(qty) * float(unit_price)
        ws.append_row([order_no, sku, product_name, float(qty),
                       float(unit_price), subtotal, warehouse])
        clear_cache()
        return True
    except Exception as e:
        st.error(f"新增品項失敗: {e}")
        return False

def delete_order_item(order_no, sku, warehouse):
    """刪除訂單中的某個品項"""
    ws = get_worksheet_for_write("OrderItems")
    if not ws:
        st.error("無法連線到 OrderItems 工作表")
        return False
    try:
        all_vals = ws.get_all_values()
        header = all_vals[0]
        no_idx = header.index("order_no")
        sku_idx = header.index("sku")
        wh_idx = header.index("warehouse")
        for i, row in enumerate(all_vals[1:], 2):
            if (str(row[no_idx]) == str(order_no) and
                str(row[sku_idx]) == str(sku) and
                str(row[wh_idx]) == str(warehouse)):
                ws.delete_rows(i)
                clear_cache()
                return True
        st.error("找不到該品項")
        return False
    except Exception as e:
        st.error(f"刪除品項失敗: {e}")
        return False

def recalc_order_total(order_no, discount=0, shipping_fee=0):
    """重新計算訂單總額並更新（含品項明細摘要）"""
    items = load_order_items(order_no)
    items_total = float(items['subtotal'].sum()) if not items.empty else 0.0
    total = items_total - float(discount) + float(shipping_fee)
    items_summary = ", ".join(
        [f"{row['product_name']} x{int(row['qty'])}" for _, row in items.iterrows()]
    ) if not items.empty else ""
    return update_order_fields(order_no, {
        'items_total': float(items_total),
        'total_amount': float(total),
        'discount': float(discount),
        'shipping_fee': float(shipping_fee),
        'items_detail': items_summary
    })

def backfill_items_detail():
    """回填所有訂單的 items_detail 欄位"""
    df_orders = load_orders()
    if df_orders.empty:
        return 0
    count = 0
    import time as _bt
    for _, order in df_orders.iterrows():
        detail = str(order.get('items_detail', '')).strip()
        if detail and detail not in ('', 'nan', 'None'):
            continue
        ono = str(order.get('order_no', ''))
        items = load_order_items(ono)
        if items.empty:
            continue
        summary = ", ".join(
            [f"{row['product_name']} x{int(row['qty'])}" for _, row in items.iterrows()]
        )
        if summary:
            update_order_fields(ono, {'items_detail': summary})
            count += 1
            _bt.sleep(1)
    return count

def ship_order(order_no, keyer, ship_method="", ship_no="", target_status="未付款/已出貨"):
    items = load_order_items(order_no)
    if items.empty:
        return False, "找不到訂單品項"
    for _, item in items.iterrows():
        ok = add_transaction(
            "銷售出貨", date.today(),
            str(item['sku']), str(item['warehouse']),
            float(item['qty']), keyer,
            f"訂單出貨: {order_no}", ship_method, ship_no
        )
        if not ok:
            return False, f"品項 {item['sku']} 出貨失敗"
    if update_order_status(order_no, target_status):
        return True, "出貨完成，庫存已扣除"
    return False, "出貨紀錄已建立但狀態更新失敗"

def delete_order(order_no):
    ws_orders = get_worksheet_for_write("Orders")
    ws_items = get_worksheet_for_write("OrderItems")
    if not ws_orders or not ws_items:
        st.error("無法連線到工作表")
        return False
    try:
        cells = ws_items.findall(str(order_no))
        for cell in sorted(cells, key=lambda c: c.row, reverse=True):
            ws_items.delete_rows(cell.row)
        cells = ws_orders.findall(str(order_no))
        for cell in sorted(cells, key=lambda c: c.row, reverse=True):
            ws_orders.delete_rows(cell.row)
        clear_cache()
        return True
    except Exception as e:
        st.error(f"刪除失敗: {e}")
        return False

# ==========================================
# 3.6 會員名單核心功能
# ==========================================

def ensure_members_sheet():
    if st.session_state.get('_members_sheet_ok'):
        return
    sh = get_spreadsheet()
    if not sh:
        return
    try:
        existing = [ws.title for ws in sh.worksheets()]
        if "Members" not in existing:
            ws = sh.add_worksheet(title="Members", rows=2000, cols=12)
            ws.append_row(["member_id", "name", "phone", "email", "address",
                           "note", "created_at", "last_order_date",
                           "birthday", "lunar_birthday", "birth_time"])
        st.session_state['_members_sheet_ok'] = True
    except Exception:
        pass

def load_members():
    ensure_members_sheet()
    ensure_extra_columns()
    df = load_data("Members")
    if df.empty:
        return pd.DataFrame(columns=["member_id", "name", "phone", "email",
                                      "address", "note", "created_at", "last_order_date",
                                      "birthday", "lunar_birthday", "birth_time"])
    for col in ['birthday', 'lunar_birthday', 'birth_time']:
        if col not in df.columns:
            df[col] = ""
    if 'phone' in df.columns:
        df['phone'] = df['phone'].apply(format_phone)
    return df

def find_member_by_name(name):
    df = load_members()
    if df.empty:
        return None
    match = df[df['name'].astype(str) == str(name)]
    return match.iloc[0] if not match.empty else None

def save_member(name, phone, email, address, note="", birthday="", lunar_birthday="", birth_time=""):
    ensure_members_sheet()
    try:
        client = get_fresh_client()
        if not client:
            st.error("Google 連線失敗，請點左側「刷新資料」後重試")
            return False
        sh = client.open(SPREADSHEET_NAME)
        ws = sh.worksheet("Members")
    except Exception as e:
        st.error(f"無法開啟 Members 工作表: {e}")
        return False
    try:
        # 一次讀取所有資料（只用 1 次 API）
        all_vals = ws.get_all_values()
        header = all_vals[0] if all_vals else []

        # 確保欄位存在（只在缺少時才寫入）
        cols_added = False
        missing = [c for c in ["birthday", "lunar_birthday", "birth_time"] if c not in header]
        if missing:
            needed_total = len(header) + len(missing)
            if ws.col_count < needed_total:
                ws.resize(cols=needed_total + 5)
            for needed_col in missing:
                ws.update_cell(1, len(header) + 1, needed_col)
                header.append(needed_col)
            cols_added = True
        if cols_added:
            all_vals = ws.get_all_values()
            header = all_vals[0]

        col_map = {h: idx for idx, h in enumerate(header)}  # 0-based index
        name_idx = col_map.get("name", 1)

        # 找會員
        found_row = -1
        found_data = None
        for i in range(1, len(all_vals)):
            row = all_vals[i]
            cell_val = row[name_idx] if len(row) > name_idx else ""
            if str(cell_val) == str(name):
                found_row = i + 1  # 1-based sheet row number
                found_data = list(row)
                break

        if found_row > 0:
            # === 更新既有會員（整行寫入，只用 1 次 API）===
            while len(found_data) < len(header):
                found_data.append('')
            field_updates = {
                'last_order_date': str(date.today()),
            }
            if phone:
                field_updates['phone'] = phone
            if email:
                field_updates['email'] = email
            if address:
                field_updates['address'] = address
            if birthday:
                field_updates['birthday'] = str(birthday)
            if lunar_birthday:
                field_updates['lunar_birthday'] = str(lunar_birthday)
            if birth_time:
                field_updates['birth_time'] = str(birth_time)
            for fname, fval in field_updates.items():
                if fname in col_map:
                    found_data[col_map[fname]] = fval
            ws.update(f'A{found_row}', [found_data], value_input_option='RAW')
            clear_cache()
            return True
        else:
            # === 新增會員 ===
            mid = f"M-{int(time.time()) % 100000:05d}"
            row_data = [''] * len(header)
            for k, v in [('member_id', mid), ('name', name), ('phone', phone),
                         ('email', email), ('address', address), ('note', note),
                         ('created_at', str(datetime.now())), ('last_order_date', str(date.today())),
                         ('birthday', str(birthday)), ('lunar_birthday', str(lunar_birthday)),
                         ('birth_time', str(birth_time))]:
                if k in col_map:
                    row_data[col_map[k]] = v
            ws.append_row(row_data, value_input_option='RAW')
            clear_cache()
            return True
    except Exception as e:
        st.error(f"會員儲存失敗: {e}")
        return False

def delete_member(name):
    ws = get_worksheet_for_write("Members")
    if not ws:
        return False
    try:
        all_vals = ws.get_all_values()
        header = all_vals[0]
        name_idx = header.index("name")
        for i, row in enumerate(all_vals[1:], 2):
            if str(row[name_idx]) == str(name):
                ws.delete_rows(i)
                clear_cache()
                return True
        return False
    except Exception:
        return False

def sync_members_from_orders():
    """從所有訂單中提取客戶資料，自動匯入會員名單（不覆蓋已有會員）"""
    df_orders = load_orders()
    if df_orders.empty:
        return 0
    existing_members = load_members()
    existing_names = set(existing_members['name'].astype(str).tolist()) if not existing_members.empty else set()
    count = 0
    seen = set()
    for _, row in df_orders.iterrows():
        name = str(row.get('customer_name', '')).strip()
        if not name or name in seen or name in existing_names:
            continue
        seen.add(name)
        phone = str(row.get('customer_phone', ''))
        email = str(row.get('customer_email', ''))
        addr = str(row.get('shipping_address', ''))
        bday = str(row.get('birthday', ''))
        lbday = str(row.get('lunar_birthday', ''))
        btime = str(row.get('birth_time', ''))
        save_member(name, phone, email, addr, birthday=bday, lunar_birthday=lbday, birth_time=btime)
        count += 1
    return count

# ==========================================
# 3.7 歷史紀錄顯示
# ==========================================

def render_history_table(doc_type_filter=None):
    st.markdown("#### 最近紀錄")
    df = load_data("History")
    if df.empty: return
    df_prod = load_data("Products")
    sku_map = dict(zip(df_prod['sku'].astype(str), df_prod['name'])) if not df_prod.empty else {}
    if doc_type_filter:
        df = df[df['doc_type'].isin(doc_type_filter)] if isinstance(doc_type_filter, list) else df[df['doc_type'] == doc_type_filter]
    df = df.sort_index(ascending=False).head(15)
    cols = st.columns([1.5, 1.5, 3, 1, 1, 1, 2, 1])
    for col, h in zip(cols, ["單號", "日期", "品名 / SKU", "倉庫", "數量", "經手", "備註", "操作"]): col.markdown(f"**{h}**")
    for idx, row in df.iterrows():
        c1, c2, c3, c4, c5, c6, c7, c8 = st.columns([1.5, 1.5, 3, 1, 1, 1, 2, 1])
        doc_no = str(row.get('doc_no', ''))
        c1.text(doc_no[-10:]); c2.text(row.get('date', ''))
        sku = str(row.get('sku',''))
        d_name = row.get('product_name', sku_map.get(sku, '未知'))
        c3.text(f"{d_name}\n({sku})")
        c4.text(row.get('warehouse', ''))
        c5.text(row.get('qty', 0)); c6.text(row.get('user', '')); c7.text(row.get('note', ''))
        if c8.button("刪除", key=f"del_{doc_no}_{idx}"):
            if delete_transaction(doc_no): st.rerun()
        st.divider()

# ==========================================
# 3.8 工資管理（整合自 wage-app）
# ==========================================

DEFAULT_WAGE_CATALOG = [
    {"name": "光之鹽語 - 光之鹽語禮盒",     "wageMake": 52,   "wagePack": 6,    "wageShip": 10, "wageSvc": 0},
    {"name": "艾草包10入",                    "wageMake": 0,    "wagePack": 10,   "wageShip": 10, "wageSvc": 0},
    {"name": "艾草包5入",                     "wageMake": 0,    "wagePack": 5,    "wageShip": 10, "wageSvc": 0},
    {"name": "脈輪淨化蠟燭組 - 9入",          "wageMake": 45,   "wagePack": 4.5,  "wageShip": 10, "wageSvc": 0},
    {"name": "光之鹽語 - 單購魔法鹽",          "wageMake": 24,   "wagePack": 4,    "wageShip": 10, "wageSvc": 0},
    {"name": "大淨化包",                       "wageMake": 24,   "wagePack": 3,    "wageShip": 10, "wageSvc": 0},
    {"name": "大淨化包｜三日快速顯化儀式 - 代點顯化蠟燭", "wageMake": 24, "wagePack": 3, "wageShip": 0, "wageSvc": 250},
    {"name": "顯化蠟燭2入",                   "wageMake": 10,   "wagePack": 1,    "wageShip": 10, "wageSvc": 0},
    {"name": "顯化蠟燭｜代點服務",             "wageMake": 10,   "wagePack": 1,    "wageShip": 0,  "wageSvc": 200},
    {"name": "2026 馬上成功・人財貴圓滿組",    "wageMake": 48,   "wagePack": 6,    "wageShip": 10, "wageSvc": 0},
    {"name": "28天脈輪能量日常守護組",          "wageMake": 152,  "wagePack": 16,   "wageShip": 10, "wageSvc": 0},
    {"name": "數字水晶手鍊(細)",               "wageMake": 50,   "wagePack": 0,    "wageShip": 10, "wageSvc": 0},
    {"name": "數字水晶手鍊(粗)",               "wageMake": 100,  "wagePack": 0,    "wageShip": 10, "wageSvc": 0},
    {"name": "生命數字能量項鍊(鈦鋼), 項鍊整組","wageMake": 200, "wagePack": 0,    "wageShip": 10, "wageSvc": 0},
    {"name": "銅鑼浴",                         "wageMake": 0,    "wagePack": 0,    "wageShip": 0,  "wageSvc": 450},
    {"name": "生命靈數解盤服務",               "wageMake": 0,    "wagePack": 0,    "wageShip": 0,  "wageSvc": 2520},
    {"name": "【清明節氣祈福組 】- 家族能量清理與內在小孩療癒 - 老師代點", "wageMake": 24, "wagePack": 3, "wageShip": 0, "wageSvc": 250},
]

WAGE_STAGES = ["製造", "包裝", "出貨", "服務費"]

def ensure_wage_sheets():
    """確保工資相關工作表存在，不存在則自動建立（使用新鮮連線避免 token 過期）"""
    if st.session_state.get('_wage_sheets_ok'):
        return
    import uuid as _uuid
    client = get_fresh_client()
    if not client:
        st.error("無法連接 Google Sheets，請確認設定")
        return
    try:
        sh = client.open(SPREADSHEET_NAME)
    except Exception as e:
        st.error(f"開啟試算表失敗：{e}")
        return

    existing_titles = [ws.title for ws in sh.worksheets()]

    # WageEmployees
    if "WageEmployees" not in existing_titles:
        ws_emp = sh.add_worksheet(title="WageEmployees", rows=200, cols=5)
        ws_emp.append_row(["id", "name", "multProd"])
        for emp_name, mult in [("James", 1), ("千畇", 1), ("Imeng", 1)]:
            ws_emp.append_row([str(_uuid.uuid4())[:8], emp_name, mult])
    else:
        ws_emp = sh.worksheet("WageEmployees")
        if not ws_emp.row_values(1):
            ws_emp.append_row(["id", "name", "multProd"])

    # WageCatalog（含每階段負責員工欄位）
    if "WageCatalog" not in existing_titles:
        ws_cat = sh.add_worksheet(title="WageCatalog", rows=200, cols=9)
        ws_cat.append_row(["name", "wageMake", "wagePack", "wageShip", "wageSvc", "empMake", "empPack", "empShip", "empSvc"])
        for p in DEFAULT_WAGE_CATALOG:
            ws_cat.append_row([p["name"], p["wageMake"], p["wagePack"], p["wageShip"], p["wageSvc"], "", "", "", ""])
    else:
        ws_cat = sh.worksheet("WageCatalog")
        header = ws_cat.row_values(1)
        if not header:
            ws_cat.append_row(["name", "wageMake", "wagePack", "wageShip", "wageSvc", "empMake", "empPack", "empShip", "empSvc"])
            for p in DEFAULT_WAGE_CATALOG:
                ws_cat.append_row([p["name"], p["wageMake"], p["wagePack"], p["wageShip"], p["wageSvc"], "", "", "", ""])
        # 若既有工作表缺少員工欄位，save_wage_product 儲存時會自動補上

    # WageEntries
    if "WageEntries" not in existing_titles:
        ws_ent = sh.add_worksheet(title="WageEntries", rows=2000, cols=14)
        ws_ent.append_row(["id", "date", "employee_name", "category", "stage", "item", "qty", "price", "amount", "note", "created_by", "created_at", "paid"])
    else:
        ws_ent = sh.worksheet("WageEntries")
        header = ws_ent.row_values(1)
        if not header:
            ws_ent.append_row(["id", "date", "employee_name", "category", "stage", "item", "qty", "price", "amount", "note", "created_by", "created_at", "paid"])
        elif "paid" not in header:
            # 既有工作表補上 paid 欄位
            new_col = len(header) + 1
            if ws_ent.col_count < new_col:
                ws_ent.resize(cols=new_col + 2)
            ws_ent.update_cell(1, new_col, "paid")

    # WageSettlements
    if "WageSettlements" not in existing_titles:
        ws_set = sh.add_worksheet(title="WageSettlements", rows=100, cols=4)
        ws_set.append_row(["year_month", "settled_at", "total"])
    else:
        ws_set = sh.worksheet("WageSettlements")
        if not ws_set.row_values(1):
            ws_set.append_row(["year_month", "settled_at", "total"])

    st.session_state['_wage_sheets_ok'] = True

def load_wage_employees():
    df = load_data("WageEmployees")
    if df.empty or 'name' not in df.columns:
        return pd.DataFrame(columns=["id", "name", "multProd"])
    return df

def load_wage_catalog():
    df = load_data("WageCatalog")
    if df.empty:
        return pd.DataFrame(DEFAULT_WAGE_CATALOG)

    # 自動對應 Google Sheets 的各種欄位名稱（下底線 / 駝峰式皆支援）
    col_map = {
        'product_name': 'name',
        'wage_make': 'wageMake',
        'wage_pack': 'wagePack',
        'wage_ship': 'wageShip',
        'wage_svc': 'wageSvc',
        'emp_make': 'empMake',
        'emp_pack': 'empPack',
        'emp_ship': 'empShip',
        'emp_svc': 'empSvc',
    }
    # load_data() 會自動補空的 'name' 欄位給所有 sheets；
    # 若 WageCatalog 實際使用 product_name，改名前必須先刪除這個空佔位欄，
    # 否則會出現兩個 'name' 欄導致 df['name'] 回傳 DataFrame 而非 Series。
    if 'product_name' in df.columns and 'name' in df.columns:
        df = df.drop(columns=['name'])
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    if 'name' not in df.columns:
        return pd.DataFrame(DEFAULT_WAGE_CATALOG)

    for col in ["wageMake", "wagePack", "wageShip", "wageSvc"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0
    for col in ["empMake", "empPack", "empShip", "empSvc"]:
        if col not in df.columns:
            df[col] = ""
    return df

def load_wage_entries(year_month=None):
    df = load_data("WageEntries")
    if df.empty or 'date' not in df.columns:
        return pd.DataFrame(columns=["id", "date", "employee_name", "category", "stage", "item", "qty", "price", "amount", "note", "paid"])
    # 相容使用者在 Google Sheets 改過的欄位名稱
    _rename_map = {}
    if 'entry_id' in df.columns and 'id' not in df.columns:
        _rename_map['entry_id'] = 'id'
    if 'item_name' in df.columns and 'item' not in df.columns:
        _rename_map['item_name'] = 'item'
    if _rename_map:
        df = df.rename(columns=_rename_map)
    for col in ["qty", "price", "amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    if 'paid' not in df.columns:
        df['paid'] = ""
    if year_month:
        df = df[df['date'].astype(str).str.startswith(year_month)]
    return df

def load_wage_settlements():
    df = load_data("WageSettlements")
    if df.empty or 'year_month' not in df.columns:
        return {}
    return dict(zip(df['year_month'].astype(str), df['settled_at'].astype(str)))

def add_wage_entry(date_str, employee_name, category, stage, item, qty, price, amount, note, created_by=""):
    import uuid, datetime as dt
    entry_id = str(uuid.uuid4())[:8]
    created_at = dt.datetime.now().isoformat()
    ws = get_worksheet_for_write("WageEntries")
    if not ws:
        return False
    import time as _t
    for _retry in range(3):
        try:
            ws.append_row([entry_id, date_str, employee_name, category, stage or "", item, qty or "", price or "", amount, note, created_by, created_at, ""])
            break
        except Exception as _e:
            if '429' in str(_e) and _retry < 2:
                _t.sleep(3 + _retry * 3)
                continue
            raise
    clear_cache()
    return True

def mark_wage_entry_paid(entry_id, paid=True):
    """標記單筆工資為已發放或未發放"""
    ws = get_worksheet_for_write("WageEntries")
    if not ws:
        return False
    data = ws.get_all_values()
    if not data:
        return False
    header = data[0]
    id_col = None
    paid_col = None
    for i, h in enumerate(header):
        if h.strip().lower() in ('id', 'entry_id'):
            id_col = i
        if h.strip().lower() == 'paid':
            paid_col = i
    if id_col is None:
        return False
    if paid_col is None:
        # 自動補 paid 欄位
        paid_col = len(header)
        if ws.col_count <= paid_col:
            ws.resize(cols=paid_col + 2)
        ws.update_cell(1, paid_col + 1, "paid")
    for row_idx, row in enumerate(data[1:], start=2):
        cell_id = row[id_col] if id_col < len(row) else ""
        if str(cell_id).strip() == str(entry_id).strip():
            ws.update_cell(row_idx, paid_col + 1, "Y" if paid else "")
            clear_cache()
            return True
    return False

def delete_wage_entry(entry_id):
    ws = get_worksheet_for_write("WageEntries")
    data = ws.get_all_values()
    for i, row in enumerate(data[1:], start=2):
        if row and row[0] == entry_id:
            ws.delete_rows(i)
            clear_cache()
            return True
    return False

def save_wage_employee(name, mult_prod=1):
    import uuid
    ws = get_worksheet_for_write("WageEmployees")
    data = ws.get_all_values()
    for i, row in enumerate(data[1:], start=2):
        if row and row[1] == name:
            ws.update(f"C{i}", [[mult_prod]])
            clear_cache()
            return True
    emp_id = str(uuid.uuid4())[:8]
    ws.append_row([emp_id, name, mult_prod])
    clear_cache()
    return True

def delete_wage_employee(name):
    ws = get_worksheet_for_write("WageEmployees")
    data = ws.get_all_values()
    for i, row in enumerate(data[1:], start=2):
        if row and row[1] == name:
            ws.delete_rows(i)
            clear_cache()
            return True
    return False

def save_wage_product(product_name, wage_make=0, wage_pack=0, wage_ship=0, wage_svc=0, emp_make="", emp_pack="", emp_ship="", emp_svc="", original_name=""):
    ws = get_worksheet_for_write("WageCatalog")
    if not ws:
        return False
    data = ws.get_all_values()
    header = list(data[0]) if data else []

    # 支援新舊兩種欄位名稱（Google Sheets 可能用 wage_make 或 wageMake）
    _col_aliases = {
        "name":     ["name", "product_name"],
        "wageMake": ["wageMake", "wage_make"],
        "wagePack": ["wagePack", "wage_pack"],
        "wageShip": ["wageShip", "wage_ship"],
        "wageSvc":  ["wageSvc",  "wage_svc"],
        "empMake":  ["empMake",  "emp_make"],
        "empPack":  ["empPack",  "emp_pack"],
        "empShip":  ["empShip",  "emp_ship"],
        "empSvc":   ["empSvc",   "emp_svc"],
    }
    col_idx = {h: i + 1 for i, h in enumerate(header)}

    def _find_col(field):
        """依照新舊別名尋找實際欄位索引，找不到回傳 None"""
        for alias in _col_aliases.get(field, [field]):
            if alias in col_idx:
                return col_idx[alias]
        return None

    # 若缺少員工欄位（新舊名稱都沒有），自動補上（先擴展欄數避免超出 grid limits）
    missing_emp = [n for n, o in [("empMake", "emp_make"), ("empPack", "emp_pack"), ("empShip", "emp_ship"), ("empSvc", "emp_svc")]
                   if n not in header and o not in header]
    if missing_emp:
        needed = len(header) + len(missing_emp)
        if ws.col_count < needed:
            ws.resize(cols=needed + 3)
        for new_name in missing_emp:
            header.append(new_name)
            ws.update_cell(1, len(header), new_name)
            col_idx[new_name] = len(header)

    field_map = {
        "wageMake": wage_make, "wagePack": wage_pack,
        "wageShip": wage_ship, "wageSvc":  wage_svc,
        "empMake":  emp_make,  "empPack":  emp_pack, "empShip": emp_ship, "empSvc": emp_svc,
    }

    # 更新既有行（用 original_name 定位，若有改名則同步更新名稱欄）
    lookup_name = original_name if original_name else product_name
    for i, row in enumerate(data[1:], start=2):
        if row and row[0] == lookup_name:
            # 如果改了名稱，更新第一欄
            if product_name != lookup_name:
                ws.update_cell(i, 1, product_name)
            for field, val in field_map.items():
                c = _find_col(field)
                if c is not None:
                    ws.update_cell(i, c, val)
            clear_cache()
            return True

    # 新增一行
    new_row = [""] * len(header)
    name_col = _find_col("name")
    if name_col:
        new_row[name_col - 1] = product_name
    else:
        new_row[0] = product_name          # fallback：放第一欄
    for field, val in field_map.items():
        c = _find_col(field)
        if c is not None:
            new_row[c - 1] = val
    ws.append_row(new_row)
    clear_cache()
    return True

def delete_wage_product(product_name):
    ws = get_worksheet_for_write("WageCatalog")
    data = ws.get_all_values()
    for i, row in enumerate(data[1:], start=2):
        if row and row[0] == product_name:
            ws.delete_rows(i)
            clear_cache()
            return True
    return False

def mark_wage_settlement(year_month, total):
    import datetime as dt
    ws = get_worksheet_for_write("WageSettlements")
    data = ws.get_all_values()
    settled_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    for i, row in enumerate(data[1:], start=2):
        if row and row[0] == year_month:
            ws.update(f"B{i}:C{i}", [[settled_at, total]])
            clear_cache()
            return True
    ws.append_row([year_month, settled_at, total])
    clear_cache()
    return True

def _match_wage_catalog(product_name, df_cat):
    """多層模糊比對產品目錄，回傳匹配的 Series 或 None"""
    if df_cat.empty:
        return None
    cm = df_cat[df_cat['name'] == product_name]
    if cm.empty:
        cm = df_cat[df_cat['name'].apply(lambda n: n in product_name if n else False)]
    if cm.empty and len(product_name) >= 8:
        cm = df_cat[df_cat['name'].str.contains(product_name[:8], na=False, regex=False)]
    if cm.empty and len(product_name) >= 4:
        cm = df_cat[df_cat['name'].str.contains(product_name[:4], na=False, regex=False)]
    if cm.empty:
        return None
    if len(cm) > 1:
        cm = cm.loc[[cm['name'].str.len().idxmax()]]
    return cm.iloc[0]

def _get_existing_wage_keys(order_no):
    """取得訂單已有的工資 (stage, item) 組合"""
    df_existing = load_wage_entries()
    keys = set()
    if not df_existing.empty and 'note' in df_existing.columns:
        mask = df_existing['note'].astype(str).str.contains(str(order_no), na=False)
        if mask.any():
            for _, er in df_existing[mask].iterrows():
                keys.add((str(er.get('stage', '')), str(er.get('item', ''))))
    return keys

def create_wage_for_stages(order_no, order_date, items_df, df_cat, stages_with_emp, keyer=""):
    """通用工資建立函式
    stages_with_emp: list of (stage_name, wage_col, employee_name)
    例: [("製造", "wageMake", "James"), ("出貨", "wageShip", "Imeng")]
    """
    if items_df.empty or df_cat.empty:
        return 0
    existing_keys = _get_existing_wage_keys(order_no)
    count = 0
    date_str = str(order_date)

    for _, item in items_df.iterrows():
        product_name = str(item.get('product_name', ''))
        qty = float(item.get('qty', 1) or 1)
        cat_row = _match_wage_catalog(product_name, df_cat)
        if cat_row is None:
            continue

        for stage, wage_col, emp in stages_with_emp:
            if (stage, product_name) in existing_keys:
                continue
            wage = float(cat_row.get(wage_col, 0) or 0)
            if wage <= 0:
                continue
            if not emp or emp.lower() == 'nan':
                continue
            amount = round(wage * qty, 2)
            add_wage_entry(
                date_str=date_str,
                employee_name=emp,
                category="產品",
                stage=stage,
                item=product_name,
                qty=qty,
                price=wage,
                amount=amount,
                note=f"自動帶入｜訂單 {order_no}",
                created_by=keyer
            )
            count += 1
    return count

def auto_create_wage_entries_for_order(order_no, order_date, keyer="", shipper="", service_person=""):
    """訂單完成時自動建立工資：
    - 製造/包裝：依每個品項的出庫倉庫（倉庫名 = 製造人）
    - 出貨：用 shipper 參數（出貨作業選的人）
    - 服務費：用 service_person 參數（製造作業選的人）
    """
    items = load_order_items(order_no)
    if items.empty:
        return 0
    df_cat = load_wage_catalog()
    if df_cat.empty:
        return 0

    existing_keys = _get_existing_wage_keys(order_no)
    count = 0
    date_str = str(order_date)

    for _, item in items.iterrows():
        product_name = str(item.get('product_name', ''))
        qty = float(item.get('qty', 1) or 1)
        warehouse = str(item.get('warehouse', '')).strip()

        cat_row = _match_wage_catalog(product_name, df_cat)
        if cat_row is None:
            continue

        # 製造 → 倉庫名（誰的庫存 = 誰製造的）
        # 包裝 → 同倉庫名
        # 出貨 → shipper 參數
        # 服務費 → service_person 參數
        stage_emp = [
            ("製造", "wageMake", warehouse),
            ("包裝", "wagePack", warehouse),
            ("出貨", "wageShip", shipper),
            ("服務費", "wageSvc", service_person),
        ]

        for stage, wage_col, emp in stage_emp:
            if (stage, product_name) in existing_keys:
                continue
            wage = float(cat_row.get(wage_col, 0) or 0)
            if wage <= 0:
                continue
            if not emp or emp.lower() == 'nan':
                emp = keyer if keyer else "未指定"
            amount = round(wage * qty, 2)
            add_wage_entry(
                date_str=date_str,
                employee_name=emp,
                category="產品",
                stage=stage,
                item=product_name,
                qty=qty,
                price=wage,
                amount=amount,
                note=f"自動帶入｜訂單 {order_no}",
                created_by=keyer
            )
            count += 1
    return count

def backfill_wage_entries_for_month(year_month):
    """補建指定月份所有已完成訂單的缺失工資紀錄，回傳 (訂單數, 建立數, 警告列表)"""
    df_orders = load_orders()
    if df_orders.empty:
        return 0, 0, []
    completed = df_orders[
        (df_orders['status'] == '已完成') &
        (df_orders['order_date'].astype(str).str.startswith(year_month))
    ]
    if completed.empty:
        return 0, 0, []
    total_created = 0
    order_count = 0
    warnings = []

    # 預先載入目錄做診斷
    df_cat = load_wage_catalog()

    for _, order in completed.iterrows():
        ono = str(order.get('order_no', ''))
        odate = str(order.get('order_date', ''))
        created_by = str(order.get('created_by', ''))

        # 診斷：檢查每個品項是否能匹配
        items = load_order_items(ono)
        if not items.empty and not df_cat.empty:
            for _, itm in items.iterrows():
                pname = str(itm.get('product_name', ''))
                wh = str(itm.get('warehouse', '')).strip()
                cr = _match_wage_catalog(pname, df_cat)
                if cr is None:
                    warnings.append(f"⚠️ {ono}: 「{pname}」在工資目錄找不到對應產品")
                else:
                    if not wh:
                        warnings.append(f"ℹ️ {ono}: 「{pname}」無出庫倉庫，製造/包裝負責人將使用訂單建立者")

        import time as _bt
        n = auto_create_wage_entries_for_order(ono, odate, keyer=created_by)
        if n > 0:
            _bt.sleep(2)  # 避免 API 限流
            total_created += n
            order_count += 1
    return order_count, total_created, warnings

# ==========================================
# 4. 主程式分頁
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="💎")
st.title(f"💎 {PAGE_TITLE}")
ensure_price_column()

with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["🛒 訂單管理", "👥 會員管理", "🔨 製造作業", "🚚 出貨作業", "📦 商品管理", "📥 進貨作業", "📦 移庫作業", "📊 報表查詢", "💰 工資管理"])
    if st.button("刷新資料"):
        clear_cache()
        st.rerun()
    with st.expander("🔧 系統工具"):
        st.caption("⚠️ 請依序執行：先 ① 再 ②")
        if st.button("① 📐 重整訂單欄位順序", help="重新排列欄位順序、移除重複的中文欄位"):
            with st.spinner("重整欄位中..."):
                ok, msg = reorganize_orders_columns()
            if ok:
                st.success(msg)
                time.sleep(2)
                st.rerun()
            else:
                st.error(msg)
        if st.button("② 🔄 批次重算所有訂單金額", help="從 OrderItems 重新加總，算出正確的商品小計與應付總額"):
            progress_bar = st.progress(0, text="準備中...")
            def _update_progress(current, total):
                progress_bar.progress(current / total, text=f"重算中... {current}/{total}")
            ok, msg, cnt = batch_recalc_all_orders(progress_cb=_update_progress)
            if ok:
                progress_bar.progress(1.0, text="完成！")
                st.success(msg)
                time.sleep(2)
                st.rerun()
            else:
                progress_bar.empty()
                st.error(msg)

# --- 📦 商品管理 ---
if page == "📦 商品管理":
    st.subheader("📦 商品資料維護")
    t1, t2 = st.tabs(["新增商品", "修改商品"])
    with t1:
        current_df = load_data("Products")
        existing_cats = sorted(list(set(current_df['category'].tolist()))) if not current_df.empty else []
        cat_list = sorted(list(set(CATEGORIES + existing_cats)))
        c_cat, c_ser = st.columns(2)
        cat_opt = c_cat.selectbox("1. 分類", cat_list + ["手動輸入新分類..."])
        final_cat = c_cat.text_input("新分類名稱") if cat_opt == "手動輸入新分類..." else cat_opt
        if cat_opt != "手動輸入新分類..." and not current_df.empty:
            filtered_sers = current_df[current_df['category'] == cat_opt]['series'].unique().tolist()
            final_ser_list = sorted(list(set(filtered_sers))) if filtered_sers else sorted(SERIES)
        else: final_ser_list = sorted(SERIES)
        ser_opt = c_ser.selectbox("2. 系列", final_ser_list + ["手動輸入新系列..."])
        final_ser = c_ser.text_input("新系列名稱") if ser_opt == "手動輸入新系列..." else ser_opt
        auto_sku = generate_auto_sku(final_ser, final_cat, set(current_df['sku'].astype(str)) if not current_df.empty else set())
        c1, c2 = st.columns(2)
        sku = c1.text_input("3. 貨號", value=auto_sku)
        name = c2.text_input("4. 品名 *必填")
        v_spec = st.text_input("5. 規格"); v_color = st.text_input("6. 顏色")
        pc1, pc2 = st.columns(2)
        v_price = pc1.number_input("7. 售價", min_value=0.0, value=0.0, step=10.0)
        note = pc2.text_input("8. 備註")
        if st.button("確認新增商品"):
            if sku and name:
                s, m = add_product(sku, name, final_cat, final_ser, v_spec, note, v_color, v_price)
                if s: st.success("新增成功"); time.sleep(1); st.rerun()
    with t2:
        df_p_f = get_formatted_product_df()
        if not df_p_f.empty:
            sel_l = st.selectbox("選擇商品", options=df_p_f['label'].tolist())
            if sel_l:
                sku_s = sel_l.split(" | ")[0]
                curr = df_p_f[df_p_f['sku'].astype(str) == sku_s].iloc[0]
                with st.form("edit_f"):
                    n_n = st.text_input("品名", value=str(curr['name']))
                    n_s = st.text_input("規格", value=str(curr['spec']))
                    n_c = st.text_input("顏色", value=str(curr['color']))
                    cur_price = float(curr['price']) if curr.get('price', '') not in ['', None] else 0.0
                    n_p = st.number_input("售價", min_value=0.0, value=cur_price, step=10.0)
                    n_nt = st.text_input("備註", value=str(curr['note']))
                    if st.form_submit_button("儲存修改"):
                        if update_product(sku_s, {'name': n_n, 'spec': n_s, 'color': n_c, 'note': n_nt, 'price': n_p}):
                            st.success("更新成功"); time.sleep(1); st.rerun()
        else: st.warning("資料庫為空，請先新增商品。")

# --- 📦 移庫作業 ---
elif page == "📦 移庫作業":
    st.subheader("📦 倉庫間移庫")
    prods = get_formatted_product_df()
    if not prods.empty:
        with st.form("tr_form"):
            sel_p = st.selectbox("選擇商品", prods['label'])
            user = st.selectbox("經手人", KEYERS)
            w1, w2, q = st.columns(3)
            f_wh = w1.selectbox("來源倉庫", WAREHOUSES, index=0)
            t_wh = w2.selectbox("目標倉庫", WAREHOUSES, index=1)
            qty = q.number_input("數量", min_value=0.1, value=1.0)
            if st.form_submit_button("執行移庫"):
                if f_wh == t_wh: st.error("來源與目標不可相同")
                else:
                    sku = sel_p.split(" | ")[0]
                    add_transaction("移庫(撥出)", date.today(), sku, f_wh, qty, user, f"移至 {t_wh}")
                    add_transaction("移庫(撥入)", date.today(), sku, t_wh, qty, user, f"來自 {f_wh}")
                    st.success("移庫完成"); time.sleep(1); st.rerun()
    render_history_table(["移庫(撥出)", "移庫(撥入)"])

# --- 📥 進貨作業 ---
elif page == "📥 進貨作業":
    st.subheader("📥 進貨入庫")
    prods = get_formatted_product_df()
    if not prods.empty:
        with st.form("in_form"):
            sel_p = st.selectbox("商品", prods['label'])
            in_c1, in_c2 = st.columns(2)
            wh = in_c1.selectbox("倉庫", WAREHOUSES)
            qty = in_c2.number_input("數量", min_value=1.0, value=1.0)
            in_c3, in_c4 = st.columns(2)
            in_cost = in_c3.number_input("成本 (總額)", min_value=0.0, value=0.0, step=10.0)
            user = in_c4.selectbox("經手人", KEYERS)
            in_note = st.text_input("備註 (成本明細)")
            if st.form_submit_button("執行進貨"):
                sku_only = sel_p.split(" | ")[0]
                if add_transaction("進貨", date.today(), sku_only, wh, qty, user, in_note, cost=in_cost):
                    st.success("進貨成功"); time.sleep(1); st.rerun()
    render_history_table("進貨")

# --- 🚚 出貨作業 ---
elif page == "🚚 出貨作業":
    st.subheader("🚚 出貨作業")
    if 'out_list' not in st.session_state: st.session_state['out_list'] = []

    tab_order_ship, tab_manual_ship = st.tabs(["📋 訂單出貨", "📦 散客出貨"])

    # ── 訂單出貨（新功能）──────────────────────────────────────
    with tab_order_ship:
        st.markdown("##### 選擇訂單 → 對應工資產品 → 出貨扣庫存 → 自動建立工資")
        df_orders_ship = load_orders()
        if df_orders_ship.empty:
            st.info("目前沒有任何訂單")
        else:
            shippable = df_orders_ship[df_orders_ship['status'].isin(
                ["已確認", "未付款/未出貨", "已付款/未出貨"])]
            if shippable.empty:
                st.info("沒有待出貨的訂單")
            else:
                order_labels_ship = (
                    shippable['order_no'].astype(str) + " | " +
                    shippable['customer_name'].astype(str) + " | $" +
                    shippable['total_amount'].astype(int).astype(str) + " | " +
                    shippable['status'].astype(str)
                ).tolist()
                sel_ship_order = st.selectbox("選擇訂單", order_labels_ship, key="ship_order_sel")
                sel_ship_ono = sel_ship_order.split(" | ")[0]
                sel_ship_row = shippable[shippable['order_no'].astype(str) == sel_ship_ono].iloc[0]

                # 顯示訂單摘要
                si1, si2, si3 = st.columns(3)
                si1.write(f"**客戶:** {sel_ship_row.get('customer_name', '')}")
                si2.write(f"**地址:** {sel_ship_row.get('shipping_address', '')}")
                si3.write(f"**總額:** ${sel_ship_row.get('total_amount', 0):,.0f}")

                items_ship = load_order_items(sel_ship_ono)
                df_cat_ship = load_wage_catalog()
                cat_names_s = df_cat_ship['name'].tolist() if not df_cat_ship.empty else []
                cat_options_s = ["（不計工資）"] + cat_names_s

                if not items_ship.empty:
                    st.markdown("##### 出貨品項 → 對應工資產品")
                    st.caption("請為每個品項選擇對應的工資產品")
                    matched_ship = []
                    for idx, (_, item) in enumerate(items_ship.iterrows()):
                        product_name = str(item.get('product_name', ''))
                        qty = float(item.get('qty', 1) or 1)
                        wh = str(item.get('warehouse', ''))
                        default_idx = 0
                        for ci, cn in enumerate(cat_names_s):
                            if product_name == cn:
                                default_idx = ci + 1; break
                            if product_name in cn or cn in product_name:
                                default_idx = ci + 1; break
                            if len(product_name) >= 4 and product_name[:4] in cn:
                                default_idx = ci + 1; break

                        sc1, sc2 = st.columns([2, 3])
                        sc1.write(f"**{product_name}** ×{qty:.0f}（{wh}）")
                        sel_cat_s = sc2.selectbox(
                            "對應工資產品", cat_options_s, index=default_idx,
                            key=f"ship_cat_{idx}_{sel_ship_ono}", label_visibility="collapsed")

                        if sel_cat_s != "（不計工資）":
                            cat_row = df_cat_ship[df_cat_ship['name'] == sel_cat_s].iloc[0]
                            w_ship_val = float(cat_row.get('wageShip', 0) or 0)
                            matched_ship.append({
                                'product_name': product_name, 'cat_name': sel_cat_s,
                                'qty': qty, 'w_ship': w_ship_val
                            })

                st.markdown("---")
                ship_c1, ship_c2, ship_c3 = st.columns(3)
                ship_method = ship_c1.selectbox("寄送方式", SHIPPING_METHODS, key="ship_o_method")
                ship_tracking = ship_c2.text_input("配送號碼", key="ship_o_tracking")
                ship_user = ship_c3.selectbox("出貨人員", KEYERS, key="ship_o_user")

                # 判斷目標狀態
                cur_status = str(sel_ship_row.get('status', ''))
                if cur_status == "已付款/未出貨":
                    target_st = "已完成"
                else:
                    target_st = "未付款/已出貨"

                if matched_ship:
                    total_ship_wage = sum(x['w_ship'] * x['qty'] for x in matched_ship)
                    st.info(f"💰 出貨工資合計: **${total_ship_wage:,.0f}**（{ship_user}）　｜　出貨後狀態：**{target_st}**")
                else:
                    st.info(f"出貨後訂單狀態將更新為：**{target_st}**（無工資產品對應）")

                if st.button("🚚 確認出貨", type="primary", use_container_width=True, key="ship_o_confirm"):
                    if items_ship.empty:
                        st.error("此訂單沒有品項")
                    else:
                        # 1. 執行出貨（扣庫存 + 更新訂單狀態）
                        ok, msg = ship_order(sel_ship_ono, ship_user, ship_method, ship_tracking, target_st)
                        if ok:
                            # 2. 建立出貨工資
                            wage_count = 0
                            for mi in matched_ship:
                                if mi['w_ship'] > 0:
                                    add_wage_entry(
                                        date.today().isoformat(), ship_user, "產品", "出貨",
                                        mi['cat_name'], mi['qty'], mi['w_ship'],
                                        round(mi['w_ship'] * mi['qty'], 2),
                                        f"訂單出貨｜{sel_ship_ono}", ship_user)
                                    wage_count += 1
                            st.success(f"✅ {msg}")
                            if wage_count > 0:
                                st.info(f"💰 已建立 {wage_count} 筆出貨工資紀錄（出貨人員：{ship_user}）")

                            # 3. 如果已完成，補建還沒建過的製造/包裝/服務費工資
                            if target_st == "已完成":
                                w_extra = auto_create_wage_entries_for_order(
                                    sel_ship_ono, sel_ship_row.get('order_date', date.today()),
                                    keyer=ship_user, shipper=ship_user)
                                if w_extra > 0:
                                    st.info(f"💰 另補建 {w_extra} 筆工資紀錄")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg)

        st.markdown("---")
        render_history_table("銷售出貨")

    # ── 散客出貨（原有功能）──────────────────────────────────────
    with tab_manual_ship:
        st.markdown("##### 非訂單出貨（手動選品）")
        col_a, col_b, col_c = st.columns(3)
        ship_opt = col_a.selectbox("寄送方式", SHIPPING_METHODS + ["手動輸入..."], key="ms_method")
        final_ship = col_a.text_input("自訂方式", key="ms_custom") if ship_opt == "手動輸入..." else ship_opt
        ship_no = col_b.text_input("配送號碼", key="ms_tracking")
        user = col_c.selectbox("經手人", KEYERS, index=3, key="ms_user")
        order_id = st.text_input("備註", key="ms_note")
        st.divider()
        prods = get_formatted_product_df()
        if not prods.empty:
            col1, col2, col3 = st.columns([3, 1, 1])
            sel_p = col1.selectbox("挑選商品", prods['label'], key="ms_prod")
            wh = col2.selectbox("倉庫", WAREHOUSES, index=3, key="ms_wh")
            qty = col3.number_input("數量", 1.0, key="ms_qty")
            if st.button("加入待出貨清單", key="ms_add"):
                st.session_state['out_list'].append({'sku': sel_p.split(" | ")[0], 'name': sel_p.split(" | ")[1], 'wh': wh, 'qty': qty})
                st.rerun()
        if st.session_state['out_list']:
            for i, item in enumerate(st.session_state['out_list']):
                c_l, c_d = st.columns([5, 1])
                c_l.write(f"**{item['name']}** - {item['wh']} x{item['qty']}")
                if c_d.button("移除", key=f"rm_o_{i}"): st.session_state['out_list'].pop(i); st.rerun()
            if st.button("確認出貨", type="primary", use_container_width=True, key="ms_confirm"):
                for x in st.session_state['out_list']:
                    add_transaction("銷售出貨", date.today(), x['sku'], x['wh'], x['qty'], user, order_id, final_ship, ship_no)
                st.session_state['out_list'] = []; st.success("出貨完成"); time.sleep(1); st.rerun()
        render_history_table("銷售出貨")

# --- 🛒 訂單管理 ---
elif page == "🛒 訂單管理":
    st.subheader("🛒 訂單管理系統")
    tab_new, tab_list, tab_detail = st.tabs(["📝 新增訂單", "📋 訂單列表", "🔍 訂單明細"])

    with tab_new:
        if 'order_items' not in st.session_state:
            st.session_state['order_items'] = []

        st.markdown("##### 客戶資訊")
        members_df = load_members()
        member_names = ["-- 手動輸入 --"] + members_df['name'].astype(str).tolist() if not members_df.empty else ["-- 手動輸入 --"]
        sel_member = st.selectbox("從會員名單帶入", member_names, key="o_member_sel")

        prev_sel = st.session_state.get('_prev_member_sel', "-- 手動輸入 --")
        if sel_member != prev_sel:
            st.session_state['_prev_member_sel'] = sel_member
            if sel_member != "-- 手動輸入 --":
                m = find_member_by_name(sel_member)
                if m is not None:
                    st.session_state["o_cname"] = str(m['name'])
                    st.session_state["o_cphone"] = str(m['phone'])
                    st.session_state["o_cemail"] = str(m['email'])
                    st.session_state["o_caddr"] = str(m['address'])
                    st.session_state["o_bday"] = str(m.get('birthday', ''))
                    st.session_state["o_lbday"] = str(m.get('lunar_birthday', ''))
                    st.session_state["o_btime"] = str(m.get('birth_time', ''))
            else:
                for k in ["o_cname", "o_cphone", "o_cemail", "o_caddr", "o_bday", "o_lbday", "o_btime"]:
                    st.session_state[k] = ""

        cc1, cc2 = st.columns(2)
        cust_name = cc1.text_input("客戶名稱 *必填", key="o_cname")
        cust_phone = cc2.text_input("聯絡電話", key="o_cphone")
        cc3, cc4 = st.columns(2)
        cust_email = cc3.text_input("Email", key="o_cemail")
        ship_addr = cc4.text_input("寄送地址", key="o_caddr")
        bd1, bd2, bd3 = st.columns(3)
        o_birthday = bd1.text_input("🌞 國曆生日 (YYYY/MM/DD)", key="o_bday")
        o_lunar_bday = bd2.text_input("🌙 農曆生日 (YYYY/MM/DD)", key="o_lbday")
        o_birth_time = bd3.text_input("🕐 出生時間 (HH:MM)", key="o_btime")
        if o_birthday or o_lunar_bday:
            render_numerology_table(o_birthday, o_lunar_bday, key_prefix="new_order")
        o_note = st.text_input("訂單備註", key="o_note")
        o_user = st.selectbox("建立人", ["James", "Imeng", "小幫手"], key="o_user")

        st.markdown("##### 加入商品")
        prods = get_formatted_product_df()
        if not prods.empty:
            oc1, oc2, oc3, oc4 = st.columns([3, 1, 1, 1])
            o_sel = oc1.selectbox("選擇商品", prods['label'], key="o_psel")
            o_wh = oc2.selectbox("出貨倉庫", WAREHOUSES, key="o_pwh")
            o_qty = oc3.number_input("數量", min_value=1.0, value=1.0, key="o_pqty")
            sel_sku = o_sel.split(" | ")[0]
            sel_row = prods[prods['sku'].astype(str) == sel_sku]
            try:
                raw_price = sel_row.iloc[0]['price'] if not sel_row.empty else 0
                default_price = float(raw_price) if raw_price not in ['', None] else 0.0
            except (ValueError, TypeError):
                default_price = 0.0
            o_price = oc4.number_input("單價", min_value=0.0, value=default_price, step=10.0, key=f"o_pprice_{sel_sku}")

            if st.button("加入訂單", key="o_add_item"):
                sku = o_sel.split(" | ")[0]
                pname = o_sel.split(" | ")[1] if " | " in o_sel else o_sel
                st.session_state['order_items'].append({
                    'sku': sku, 'product_name': pname,
                    'qty': o_qty, 'unit_price': o_price,
                    'subtotal': o_qty * o_price, 'warehouse': o_wh
                })
                st.rerun()

        if st.session_state['order_items']:
            st.markdown("##### 訂單品項")
            items_total = 0
            for i, item in enumerate(st.session_state['order_items']):
                ic1, ic2, ic3, ic4, ic5 = st.columns([3, 1, 1, 1, 0.5])
                ic1.write(f"**{item['product_name']}** ({item['sku']})")
                ic2.write(f"倉庫: {item['warehouse']}")
                ic3.write(f"x {item['qty']:.0f}")
                ic4.write(f"${item['subtotal']:,.0f}")
                if ic5.button("X", key=f"o_rm_{i}"):
                    st.session_state['order_items'].pop(i)
                    st.rerun()
                items_total += item['subtotal']

            st.markdown(f"**商品小計: ${items_total:,.0f}**")
            st.markdown("##### 優惠折扣 / 運費")
            df_c1, df_c2 = st.columns(2)
            o_discount = df_c1.number_input("優惠折扣", min_value=0.0, value=0.0, step=10.0, key="o_discount")
            o_ship_fee = df_c2.number_input("運費", min_value=0.0, value=0.0, step=10.0, key="o_ship_fee")
            final_total = items_total - o_discount + o_ship_fee
            st.markdown(f"### 應付總額: **${final_total:,.0f}**")
            if o_discount > 0 or o_ship_fee > 0:
                st.caption(f"商品 ${items_total:,.0f} - 折扣 ${o_discount:,.0f} + 運費 ${o_ship_fee:,.0f}")

            if st.button("確認建立訂單", type="primary", use_container_width=True):
                if not cust_name:
                    st.error("請填寫客戶名稱")
                else:
                    ono = generate_order_no()
                    ok, msg = create_order(
                        ono, date.today(), cust_name, cust_phone,
                        cust_email, ship_addr,
                        st.session_state['order_items'], o_note, o_user,
                        o_discount, o_ship_fee,
                        o_birthday, o_lunar_bday, o_birth_time
                    )
                    if ok:
                        st.session_state['order_items'] = []
                        st.success(msg)
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg)

    with tab_list:
        if st.button("🔄 回填訂單內容到 Google Sheets", key="backfill_detail", help="將所有缺少 items_detail 的訂單補上商品明細"):
            with st.spinner("回填中..."):
                bf_count = backfill_items_detail()
            if bf_count > 0:
                st.success(f"✅ 已為 {bf_count} 筆訂單回填商品明細")
                time.sleep(1.5)
                st.rerun()
            else:
                st.info("所有訂單的商品明細已齊全")
        df_orders = load_orders()
        if df_orders.empty:
            st.info("目前沒有任何訂單")
        else:
            # === 月份篩選 ===
            df_orders['order_date_parsed'] = pd.to_datetime(df_orders['order_date'], errors='coerce')
            valid_dates = df_orders['order_date_parsed'].dropna()
            if not valid_dates.empty:
                all_months = valid_dates.dt.to_period('M').unique().sort_values(ascending=False)
                month_options = ["全部月份"] + [str(m) for m in all_months]
            else:
                month_options = ["全部月份"]

            fc1, fc2 = st.columns([1, 2])
            sel_month = fc1.selectbox("📅 篩選月份", month_options, key="o_month_filter")
            search_q = fc2.text_input("🔍 搜尋 (訂單號/客戶名)", key="o_search")

            if sel_month != "全部月份":
                sel_period = pd.Period(sel_month, freq='M')
                df_orders = df_orders[df_orders['order_date_parsed'].dt.to_period('M') == sel_period]
                st.caption(f"顯示 {sel_month} 的訂單，共 {len(df_orders)} 筆")

            sub_pending, sub_done = st.tabs(["📋 未完成", "✅ 已完成"])

            def render_order_list(df_filtered, kp):
                if df_filtered.empty:
                    st.info("沒有符合的訂單")
                    return
                st.markdown(f"共 **{len(df_filtered)}** 筆")
                for _, row in df_filtered.iterrows():
                    ono = str(row.get('order_no', ''))
                    status = str(row.get('status', ''))
                    icon = ORDER_STATUS_COLORS.get(status, "⚪")
                    with st.expander(f"{icon} {ono} | {row.get('customer_name', '')} | ${row.get('total_amount', 0):,.0f} | {status}"):
                        dc1, dc2, dc3 = st.columns(3)
                        dc1.write(f"日期: {row.get('order_date', '')}")
                        dc2.write(f"電話: {row.get('customer_phone', '')}")
                        dc3.write(f"Email: {row.get('customer_email', '')}")
                        st.write(f"地址: {row.get('shipping_address', '')}")
                        o_bday = str(row.get('birthday', ''))
                        o_lbday = str(row.get('lunar_birthday', ''))
                        o_btime = str(row.get('birth_time', ''))
                        if o_bday or o_lbday or o_btime:
                            bd_txt = f"🌞 國曆: {o_bday}" if o_bday else ""
                            lb_txt = f"🌙 農曆: {o_lbday}" if o_lbday else ""
                            bt_txt = f"🕐 出生時間: {o_btime}" if o_btime else ""
                            st.write(f"{bd_txt}  {lb_txt}  {bt_txt}".strip())
                        if o_bday or o_lbday:
                            with st.container():
                                render_numerology_table(o_bday, o_lbday, key_prefix=f"{kp}_{ono}")
                        r_disc = float(row.get('discount', 0))
                        r_ship = float(row.get('shipping_fee', 0))
                        r_items = float(row.get('items_total', 0))
                        if r_disc > 0 or r_ship > 0:
                            st.write(f"商品 ${r_items:,.0f} - 折扣 ${r_disc:,.0f} + 運費 ${r_ship:,.0f} = **${row.get('total_amount', 0):,.0f}**")
                        items = load_order_items(ono)
                        if not items.empty:
                            st.markdown("##### 訂單品項")
                            for it_idx, it_row in items.iterrows():
                                ic1, ic2, ic3, ic4, ic5 = st.columns([3, 1, 1, 1, 0.5])
                                ic1.write(f"**{it_row['product_name']}** ({it_row['sku']})")
                                ic2.write(f"{it_row['warehouse']}")
                                ic3.write(f"x{it_row['qty']:.0f}")
                                ic4.write(f"${it_row['subtotal']:,.0f}")
                                if ic5.button("🗑️", key=f"{kp}_irm_{ono}_{it_idx}"):
                                    if delete_order_item(ono, str(it_row['sku']), str(it_row['warehouse'])):
                                        recalc_order_total(ono, r_disc, r_ship)
                                        st.success(f"已刪除 {it_row['product_name']}")
                                        time.sleep(1)
                                        st.rerun()

                        # === 新增品項 ===
                        st.markdown("---")
                        prods = get_formatted_product_df()
                        if not prods.empty:
                            st.markdown("##### ➕ 新增品項")
                            ai1, ai2, ai3, ai4 = st.columns([3, 1, 1, 1])
                            ai_sel = ai1.selectbox("商品", prods['label'], key=f"{kp}_ai_sel_{ono}")
                            ai_wh = ai2.selectbox("倉庫", WAREHOUSES, key=f"{kp}_ai_wh_{ono}")
                            ai_qty = ai3.number_input("數量", min_value=1.0, value=1.0, key=f"{kp}_ai_qty_{ono}")
                            ai_sku = ai_sel.split(" | ")[0]
                            ai_prod = prods[prods['sku'].astype(str) == ai_sku]
                            try:
                                ai_dp = float(ai_prod.iloc[0]['price']) if not ai_prod.empty else 0.0
                            except (ValueError, TypeError):
                                ai_dp = 0.0
                            ai_price = ai4.number_input("單價", min_value=0.0, value=ai_dp, step=10.0,
                                                         key=f"{kp}_ai_pr_{ono}_{ai_sku}")
                            if st.button("➕ 加入此品項", key=f"{kp}_ai_add_{ono}"):
                                ai_pname = ai_sel.split(" | ")[1] if " | " in ai_sel else ai_sel
                                if add_order_item(ono, ai_sku, ai_pname, ai_qty, ai_price, ai_wh):
                                    recalc_order_total(ono, r_disc, r_ship)
                                    st.success(f"已新增 {ai_pname}")
                                    time.sleep(1)
                                    st.rerun()

                        # === 修改客戶 / 金額 ===
                        st.markdown("---")
                        edit_info, edit_price = st.tabs(["✏️ 修改客戶資訊", "💰 修改金額"])
                        with edit_info:
                            with st.form(f"edit_info_{kp}_{ono}"):
                                ei1, ei2 = st.columns(2)
                                e_name = ei1.text_input("客戶名稱", value=str(row.get('customer_name', '')))
                                e_phone = ei2.text_input("電話", value=str(row.get('customer_phone', '')))
                                ei3, ei4 = st.columns(2)
                                e_email = ei3.text_input("Email", value=str(row.get('customer_email', '')))
                                e_addr = ei4.text_input("地址", value=str(row.get('shipping_address', '')))
                                ebd1, ebd2, ebd3 = st.columns(3)
                                e_bday = ebd1.text_input("🌞 國曆生日", value=str(row.get('birthday', '')))
                                e_lbday = ebd2.text_input("🌙 農曆生日", value=str(row.get('lunar_birthday', '')))
                                e_btime = ebd3.text_input("🕐 出生時間 (HH:MM)", value=str(row.get('birth_time', '')))
                                e_note = st.text_input("備註", value=str(row.get('note', '')))
                                if st.form_submit_button("💾 儲存客戶資訊", use_container_width=True):
                                    if update_order_fields(ono, {
                                        'customer_name': e_name, 'customer_phone': e_phone,
                                        'customer_email': e_email, 'shipping_address': e_addr,
                                        'note': e_note, 'birthday': e_bday,
                                        'lunar_birthday': e_lbday,
                                        'birth_time': e_btime
                                    }):
                                        save_member(e_name, e_phone, e_email, e_addr,
                                                    birthday=e_bday, lunar_birthday=e_lbday,
                                                    birth_time=e_btime)
                                        st.success("客戶資訊已更新（會員同步）")
                                        time.sleep(1)
                                        st.rerun()
                        with edit_price:
                            with st.form(f"edit_price_{kp}_{ono}"):
                                ep1, ep2 = st.columns(2)
                                e_disc = ep1.number_input("優惠折扣", min_value=0.0, value=float(r_disc), step=10.0)
                                e_shipf = ep2.number_input("運費", min_value=0.0, value=float(r_ship), step=10.0)
                                cur_it = float(items['subtotal'].sum()) if not items.empty else 0.0
                                new_tot = cur_it - e_disc + e_shipf
                                st.markdown(f"商品 ${cur_it:,.0f} - 折扣 ${e_disc:,.0f} + 運費 ${e_shipf:,.0f} = **${new_tot:,.0f}**")
                                if st.form_submit_button("💾 儲存金額", use_container_width=True):
                                    if recalc_order_total(ono, e_disc, e_shipf):
                                        st.success("金額已更新")
                                        time.sleep(1)
                                        st.rerun()

                        # === 此訂單的工資紀錄 ===
                        _df_all_wages = load_wage_entries()
                        _existing_wage = pd.DataFrame()
                        if not _df_all_wages.empty and 'note' in _df_all_wages.columns:
                            _mask = _df_all_wages['note'].astype(str).str.contains(ono, na=False)
                            if _mask.any():
                                _existing_wage = _df_all_wages[_mask]
                        if not _existing_wage.empty:
                            st.markdown("---")
                            st.markdown(f"##### 💰 工資紀錄（{len(_existing_wage)} 筆）")
                            _ew_cols = [c for c in ['stage', 'employee_name', 'item', 'amount', 'paid'] if c in _existing_wage.columns]
                            _ew_disp = _existing_wage[_ew_cols].rename(
                                columns={'stage': '階段', 'employee_name': '負責人', 'item': '品項', 'amount': '金額', 'paid': '已發'})
                            st.dataframe(_ew_disp, use_container_width=True, hide_index=True)

                        # === 下拉式狀態選單 ===
                        st.markdown("---")
                        all_statuses = ["已確認", "未付款/未出貨", "已付款/未出貨", "未付款/已出貨", "已完成"]
                        options = [s for s in all_statuses if s != status]
                        if options:
                            new_st = st.selectbox("變更狀態", options, key=f"{kp}_nst_{ono}")

                            need_ship = (status in ["未付款/未出貨", "處理中", "已付款/未出貨"]
                                         and new_st in ["未付款/已出貨", "已完成"])
                            if need_ship:
                                sc1, sc2, sc3 = st.columns(3)
                                ship_user = sc1.selectbox("出貨經手人", KEYERS, key=f"{kp}_su_{ono}")
                                s_method = sc2.selectbox("寄送方式", SHIPPING_METHODS, key=f"{kp}_sm_{ono}")
                                s_no = sc3.text_input("配送號碼", key=f"{kp}_sn_{ono}")

                            ac1, ac2 = st.columns([3, 1])
                            btn_label = "🚚 確認出貨" if need_ship else "✅ 確認變更"
                            if ac1.button(btn_label, key=f"{kp}_apply_{ono}", type="primary"):
                                if need_ship:
                                    ok, msg = ship_order(ono, ship_user, s_method, s_no, new_st)
                                    if ok:
                                        st.success(msg)
                                        if new_st == "已完成":
                                            w_cnt = auto_create_wage_entries_for_order(
                                                ono, row.get('order_date', date.today()),
                                                keyer=ship_user, shipper=ship_user)
                                            if w_cnt > 0:
                                                st.info(f"💰 已自動建立 {w_cnt} 筆工資紀錄")
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        st.error(msg)
                                else:
                                    if update_order_status(ono, new_st):
                                        st.success(f"狀態已更新為: {new_st}")
                                        if new_st == "已完成":
                                            w_cnt = auto_create_wage_entries_for_order(
                                                ono, row.get('order_date', date.today()))
                                            if w_cnt > 0:
                                                st.info(f"💰 已自動建立 {w_cnt} 筆工資紀錄")
                                        time.sleep(1)
                                        st.rerun()

                            if status in ["已確認", "已成立", "待處理"]:
                                if ac2.button("刪除", key=f"{kp}_del_{ono}"):
                                    if delete_order(ono):
                                        st.success("訂單已刪除")
                                        time.sleep(1)
                                        st.rerun()

                        # === 補建工資（已完成訂單專用）===
                        if status == "已完成":
                            st.markdown("---")
                            if st.button("💰 補建工資紀錄", key=f"{kp}_wage_{ono}",
                                         help="依出庫倉庫自動匹配製造/包裝負責人"):
                                w_cnt = auto_create_wage_entries_for_order(
                                    ono, row.get('order_date', date.today()))
                                if w_cnt > 0:
                                    st.success(f"✅ 已補建 {w_cnt} 筆工資紀錄（製造/包裝依出庫倉庫自動分配）")
                                else:
                                    st.info("此訂單的工資紀錄已齊全")
                                time.sleep(1)
                                st.rerun()

            def apply_search(df, q):
                if not q:
                    return df
                mask = (
                    df['order_no'].astype(str).str.contains(q, case=False, na=False) |
                    df['customer_name'].astype(str).str.contains(q, case=False, na=False)
                )
                return df[mask]

            with sub_pending:
                pending = df_orders[df_orders['status'] != "已完成"]
                render_order_list(apply_search(pending, search_q).sort_index(ascending=False), "p")

            with sub_done:
                done = df_orders[df_orders['status'] == "已完成"]
                render_order_list(apply_search(done, search_q).sort_index(ascending=False), "d")

    with tab_detail:
        df_orders = load_orders()
        if not df_orders.empty:
            order_labels = (df_orders['order_no'].astype(str) + " | " +
                            df_orders['customer_name'].astype(str) + " | " +
                            df_orders['status'].astype(str)).tolist()
            sel_order = st.selectbox("選擇訂單", order_labels, key="o_detail_sel")
            sel_ono = sel_order.split(" | ")[0]
            row = df_orders[df_orders['order_no'].astype(str) == sel_ono]
            if not row.empty:
                row = row.iloc[0]
                status = str(row.get('status', ''))
                icon = ORDER_STATUS_COLORS.get(status, "⚪")
                st.markdown(f"### {icon} 訂單 {sel_ono}")
                mc1, mc2, mc3, mc4 = st.columns(4)
                mc1.metric("狀態", status)
                mc2.metric("應付總額", f"${row.get('total_amount', 0):,.0f}")
                mc3.metric("客戶", row.get('customer_name', ''))
                mc4.metric("日期", row.get('order_date', ''))
                d_disc = float(row.get('discount', 0))
                d_ship = float(row.get('shipping_fee', 0))
                d_items = float(row.get('items_total', 0))
                if d_disc > 0 or d_ship > 0:
                    dc1, dc2, dc3 = st.columns(3)
                    dc1.metric("商品小計", f"${d_items:,.0f}")
                    dc2.metric("優惠折扣", f"-${d_disc:,.0f}")
                    dc3.metric("運費", f"${d_ship:,.0f}")

                # === 生日 / 出生時間 / 數字能量顯示 ===
                det_bday = str(row.get('birthday', ''))
                det_lbday = str(row.get('lunar_birthday', ''))
                det_btime = str(row.get('birth_time', ''))
                if det_bday or det_lbday or det_btime:
                    st.markdown("---")
                    bd_parts = []
                    if det_bday:
                        bd_parts.append(f"🌞 國曆: {det_bday}")
                    if det_lbday:
                        bd_parts.append(f"🌙 農曆: {det_lbday}")
                    if det_btime:
                        bd_parts.append(f"🕐 出生時間: {det_btime}")
                    st.write("　".join(bd_parts))
                if det_bday or det_lbday:
                    st.markdown(f"#### 🔢 數字能量")
                    render_numerology_table(det_bday, det_lbday, key_prefix="detail")

                # === 訂單品項列表 ===
                st.markdown("---")
                items = load_order_items(sel_ono)
                if not items.empty:
                    st.markdown("#### 訂單品項")
                    st.dataframe(
                        items[['sku', 'product_name', 'qty', 'unit_price', 'subtotal', 'warehouse']].rename(
                            columns={'sku': '貨號', 'product_name': '品名', 'qty': '數量',
                                     'unit_price': '單價', 'subtotal': '小計', 'warehouse': '倉庫'}
                        ),
                        use_container_width=True, hide_index=True
                    )

                # === 修改訂單區域 ===
                st.markdown("---")
                st.markdown("#### ✏️ 修改訂單")
                edit_tab_info, edit_tab_items, edit_tab_price = st.tabs(
                    ["👤 客戶資訊", "📦 品項管理", "💰 金額調整"])

                # --- 客戶資訊修改 ---
                with edit_tab_info:
                    with st.form("edit_order_info"):
                        ei_c1, ei_c2 = st.columns(2)
                        e_name = ei_c1.text_input("客戶名稱", value=str(row.get('customer_name', '')))
                        e_phone = ei_c2.text_input("聯絡電話", value=str(row.get('customer_phone', '')))
                        ei_c3, ei_c4 = st.columns(2)
                        e_email = ei_c3.text_input("Email", value=str(row.get('customer_email', '')))
                        e_addr = ei_c4.text_input("寄送地址", value=str(row.get('shipping_address', '')))
                        ei_b1, ei_b2, ei_b3 = st.columns(3)
                        e_bday = ei_b1.text_input("🌞 國曆生日", value=str(row.get('birthday', '')))
                        e_lbday = ei_b2.text_input("🌙 農曆生日", value=str(row.get('lunar_birthday', '')))
                        e_btime = ei_b3.text_input("🕐 出生時間 (HH:MM)", value=str(row.get('birth_time', '')))
                        e_note = st.text_input("備註", value=str(row.get('note', '')))
                        if st.form_submit_button("💾 儲存客戶資訊", use_container_width=True):
                            updates = {
                                'customer_name': e_name,
                                'customer_phone': e_phone,
                                'customer_email': e_email,
                                'shipping_address': e_addr,
                                'note': e_note,
                                'birthday': e_bday,
                                'lunar_birthday': e_lbday,
                                'birth_time': e_btime
                            }
                            if update_order_fields(sel_ono, updates):
                                save_member(e_name, e_phone, e_email, e_addr,
                                            birthday=e_bday, lunar_birthday=e_lbday,
                                            birth_time=e_btime)
                                st.success("客戶資訊已更新（會員同步）")
                                time.sleep(1)
                                st.rerun()

                # --- 品項管理 ---
                with edit_tab_items:
                    if not items.empty:
                        st.markdown("##### 刪除品項")
                        for item_idx, item_row in items.iterrows():
                            ic1, ic2, ic3, ic4 = st.columns([3, 1, 1, 0.5])
                            ic1.write(f"**{item_row['product_name']}** ({item_row['sku']})")
                            ic2.write(f"{item_row['warehouse']} x{item_row['qty']:.0f}")
                            ic3.write(f"${item_row['subtotal']:,.0f}")
                            if ic4.button("🗑️", key=f"d_rm_{item_idx}_{item_row['sku']}"):
                                if delete_order_item(sel_ono, str(item_row['sku']), str(item_row['warehouse'])):
                                    recalc_order_total(sel_ono, d_disc, d_ship)
                                    st.success(f"已刪除 {item_row['product_name']}")
                                    time.sleep(1)
                                    st.rerun()
                    else:
                        st.info("此訂單目前沒有品項")

                    st.markdown("##### 新增品項")
                    prods = get_formatted_product_df()
                    if not prods.empty:
                        ai_c1, ai_c2, ai_c3, ai_c4 = st.columns([3, 1, 1, 1])
                        ai_sel = ai_c1.selectbox("選擇商品", prods['label'], key="d_ai_sel")
                        ai_wh = ai_c2.selectbox("倉庫", WAREHOUSES, key="d_ai_wh")
                        ai_qty = ai_c3.number_input("數量", min_value=1.0, value=1.0, key="d_ai_qty")
                        ai_sku = ai_sel.split(" | ")[0]
                        ai_row = prods[prods['sku'].astype(str) == ai_sku]
                        try:
                            ai_default_price = float(ai_row.iloc[0]['price']) if not ai_row.empty else 0.0
                        except (ValueError, TypeError):
                            ai_default_price = 0.0
                        ai_price = ai_c4.number_input("單價", min_value=0.0, value=ai_default_price,
                                                       step=10.0, key=f"d_ai_price_{ai_sku}")
                        if st.button("➕ 加入此品項", key="d_add_item"):
                            ai_pname = ai_sel.split(" | ")[1] if " | " in ai_sel else ai_sel
                            if add_order_item(sel_ono, ai_sku, ai_pname, ai_qty, ai_price, ai_wh):
                                recalc_order_total(sel_ono, d_disc, d_ship)
                                st.success(f"已新增 {ai_pname}")
                                time.sleep(1)
                                st.rerun()

                # --- 金額調整 ---
                with edit_tab_price:
                    with st.form("edit_order_price"):
                        ep_c1, ep_c2 = st.columns(2)
                        e_discount = ep_c1.number_input("優惠折扣", min_value=0.0,
                                                         value=float(d_disc), step=10.0)
                        e_ship_fee = ep_c2.number_input("運費", min_value=0.0,
                                                         value=float(d_ship), step=10.0)
                        cur_items_total = float(items['subtotal'].sum()) if not items.empty else 0.0
                        new_total = cur_items_total - e_discount + e_ship_fee
                        st.markdown(f"商品小計: **${cur_items_total:,.0f}** - 折扣 ${e_discount:,.0f} + 運費 ${e_ship_fee:,.0f}")
                        st.markdown(f"### 新總額: **${new_total:,.0f}**")
                        if st.form_submit_button("💾 儲存金額", use_container_width=True):
                            if recalc_order_total(sel_ono, e_discount, e_ship_fee):
                                st.success("金額已更新")
                                time.sleep(1)
                                st.rerun()
        else:
            st.info("目前沒有任何訂單")

# --- 👥 會員管理 ---
elif page == "👥 會員管理":
    st.subheader("👥 會員名單管理")
    tab_m_list, tab_m_add = st.tabs(["📋 會員列表", "手動新增會員"])

    with tab_m_list:
        # 從訂單同步會員按鈕
        if st.button("🔄 從訂單同步客戶到會員名單", use_container_width=True):
            count = sync_members_from_orders()
            if count > 0:
                st.success(f"已從訂單匯入 {count} 位新會員")
                time.sleep(1)
                st.rerun()
            else:
                st.info("沒有新的客戶需要匯入（全部已存在或無訂單）")
        df_members = load_members()
        if df_members.empty:
            st.info("目前沒有任何會員，建立訂單時會自動儲存客戶為會員。")
        else:
            m_search = st.text_input("搜尋會員 (姓名/電話)", key="m_search")
            filtered_m = df_members.copy()
            if m_search:
                mask = (
                    filtered_m['name'].astype(str).str.contains(m_search, case=False, na=False) |
                    filtered_m['phone'].astype(str).str.contains(m_search, case=False, na=False)
                )
                filtered_m = filtered_m[mask]

            st.markdown(f"共 **{len(filtered_m)}** 位會員")
            disp_cols = ['name', 'phone', 'email', 'address', 'birthday', 'lunar_birthday', 'birth_time', 'last_order_date']
            disp_cols = [c for c in disp_cols if c in filtered_m.columns]
            rename_map = {'name': '姓名', 'phone': '電話', 'email': 'Email',
                          'address': '地址', 'birthday': '國曆生日',
                          'lunar_birthday': '農曆生日', 'birth_time': '出生時間',
                          'last_order_date': '最後訂單日期'}
            st.dataframe(
                filtered_m[disp_cols].rename(columns=rename_map),
                use_container_width=True, hide_index=True
            )

            st.markdown("---")
            st.markdown("##### 編輯 / 刪除會員")
            m_names = filtered_m['name'].astype(str).tolist()
            if m_names:
                sel_m = st.selectbox("選擇會員", m_names, key="m_edit_sel")
                m_data = find_member_by_name(sel_m)
                if m_data is not None:
                    # 顯示數字能量
                    m_bday = str(m_data.get('birthday', ''))
                    m_lbday = str(m_data.get('lunar_birthday', ''))
                    m_btime = str(m_data.get('birth_time', ''))
                    if m_bday or m_lbday:
                        st.markdown("#### 🔢 數字能量")
                        if m_btime:
                            st.write(f"🕐 出生時間: {m_btime}")
                        render_numerology_table(m_bday, m_lbday, key_prefix="member")
                    with st.form("edit_member"):
                        em_phone = st.text_input("電話", value=str(m_data.get('phone', '')))
                        em_email = st.text_input("Email", value=str(m_data.get('email', '')))
                        em_addr = st.text_input("地址", value=str(m_data.get('address', '')))
                        em_b1, em_b2, em_b3 = st.columns(3)
                        em_bday = em_b1.text_input("🌞 國曆生日 (YYYY/MM/DD)", value=m_bday)
                        em_lbday = em_b2.text_input("🌙 農曆生日 (YYYY/MM/DD)", value=m_lbday)
                        em_btime = em_b3.text_input("🕐 出生時間 (HH:MM)", value=m_btime)
                        if st.form_submit_button("儲存修改"):
                            ok = save_member(sel_m, em_phone, em_email, em_addr,
                                             birthday=em_bday, lunar_birthday=em_lbday, birth_time=em_btime)
                            if ok:
                                st.success("會員資料已更新")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("儲存失敗，請重試")
                    if st.button("刪除此會員", key="m_del"):
                        delete_member(sel_m)
                        st.success("已刪除")
                        time.sleep(1)
                        st.rerun()

    with tab_m_add:
        with st.form("add_member"):
            am_name = st.text_input("姓名 *必填")
            am_c1, am_c2 = st.columns(2)
            am_phone = am_c1.text_input("電話")
            am_email = am_c2.text_input("Email")
            am_addr = st.text_input("地址")
            am_b1, am_b2, am_b3 = st.columns(3)
            am_bday = am_b1.text_input("🌞 國曆生日 (YYYY/MM/DD)")
            am_lbday = am_b2.text_input("🌙 農曆生日 (YYYY/MM/DD)")
            am_btime = am_b3.text_input("🕐 出生時間 (HH:MM)")
            am_note = st.text_input("備註")
            if st.form_submit_button("新增會員", use_container_width=True):
                if not am_name:
                    st.error("請填寫姓名")
                else:
                    existing = find_member_by_name(am_name)
                    if existing is not None:
                        st.warning(f"會員 '{am_name}' 已存在，將更新資料")
                    save_member(am_name, am_phone, am_email, am_addr, am_note,
                                birthday=am_bday, lunar_birthday=am_lbday, birth_time=am_btime)
                    st.success(f"會員 '{am_name}' 已儲存")
                    time.sleep(1)
                    st.rerun()

# --- 🔨 製造作業 ---
elif page == "🔨 製造作業":
    st.subheader("🔨 生產與拆解管理")
    if 'm_in_list' not in st.session_state: st.session_state['m_in_list'] = []
    prods = get_formatted_product_df()
    t_order_mfg, t1, t2, t3 = st.tabs(["📋 訂單製造", "領料清單", "完工入庫", "產品拆解"])

    # ── 訂單製造（新功能）──────────────────────────────────────
    with t_order_mfg:
        st.markdown("##### 選擇訂單 → 對應工資產品 → 指定人員 → 建立工資")
        df_orders_mfg = load_orders()
        if df_orders_mfg.empty:
            st.info("目前沒有任何訂單")
        else:
            pending_mfg = df_orders_mfg[df_orders_mfg['status'] != "已完成"]
            if pending_mfg.empty:
                st.info("所有訂單皆已完成")
            else:
                order_labels_mfg = (
                    pending_mfg['order_no'].astype(str) + " | " +
                    pending_mfg['customer_name'].astype(str) + " | " +
                    pending_mfg['status'].astype(str)
                ).tolist()
                sel_mfg_order = st.selectbox("選擇訂單", order_labels_mfg, key="mfg_order_sel")
                sel_mfg_ono = sel_mfg_order.split(" | ")[0]

                items_mfg = load_order_items(sel_mfg_ono)
                df_cat_mfg = load_wage_catalog()
                cat_names = df_cat_mfg['name'].tolist() if not df_cat_mfg.empty else []
                cat_options = ["（不計工資）"] + cat_names

                if items_mfg.empty:
                    st.info("此訂單沒有品項")
                elif not cat_names:
                    st.warning("⚠️ 工資產品目錄為空，請先到「工資管理 → 產品目錄」設定")
                else:
                    st.markdown("##### 訂單品項 → 對應工資產品")
                    st.caption("請為每個品項選擇對應的工資產品（系統會自動嘗試比對）")
                    matched_items = []
                    for idx, (_, item) in enumerate(items_mfg.iterrows()):
                        product_name = str(item.get('product_name', ''))
                        qty = float(item.get('qty', 1) or 1)
                        # 自動比對：精確 → 包含 → 前6字
                        default_idx = 0
                        for ci, cn in enumerate(cat_names):
                            if product_name == cn:
                                default_idx = ci + 1; break
                            if product_name in cn or cn in product_name:
                                default_idx = ci + 1; break
                            if len(product_name) >= 4 and product_name[:4] in cn:
                                default_idx = ci + 1; break

                        mc1, mc2 = st.columns([2, 3])
                        mc1.write(f"**{product_name}** ×{qty:.0f}")
                        sel_cat = mc2.selectbox(
                            "對應工資產品", cat_options, index=default_idx,
                            key=f"mfg_cat_{idx}_{sel_mfg_ono}", label_visibility="collapsed")

                        if sel_cat != "（不計工資）":
                            cat_row = df_cat_mfg[df_cat_mfg['name'] == sel_cat].iloc[0]
                            w_make = float(cat_row.get('wageMake', 0) or 0)
                            w_pack = float(cat_row.get('wagePack', 0) or 0)
                            w_svc  = float(cat_row.get('wageSvc', 0) or 0)
                            matched_items.append({
                                'product_name': product_name, 'cat_name': sel_cat,
                                'qty': qty, 'w_make': w_make, 'w_pack': w_pack, 'w_svc': w_svc
                            })

                    st.markdown("---")
                    mc_p1, mc_p2, mc_p3 = st.columns(3)
                    mfg_maker = mc_p1.selectbox("👷 製造人員", KEYERS, key="mfg_maker")
                    mfg_packer = mc_p2.selectbox("📦 包裝人員", KEYERS, key="mfg_packer")
                    mfg_svc = mc_p3.selectbox("🔧 服務人員", KEYERS, key="mfg_svc")

                    if matched_items:
                        total_make = sum(x['w_make'] * x['qty'] for x in matched_items)
                        total_pack = sum(x['w_pack'] * x['qty'] for x in matched_items)
                        total_svc  = sum(x['w_svc']  * x['qty'] for x in matched_items)
                        parts = [f"製造 **${total_make:,.0f}**（{mfg_maker}）",
                                 f"包裝 **${total_pack:,.0f}**（{mfg_packer}）"]
                        if total_svc > 0:
                            parts.append(f"服務費 **${total_svc:,.0f}**（{mfg_svc}）")
                        st.info("💰 " + "　".join(parts))
                    else:
                        st.warning("⚠️ 沒有任何品項對應到工資產品")

                    if st.button("✅ 確認完成製造與包裝", type="primary", use_container_width=True):
                        wage_count = 0
                        for mi in matched_items:
                            if mi['w_make'] > 0:
                                add_wage_entry(
                                    date.today().isoformat(), mfg_maker, "產品", "製造",
                                    mi['cat_name'], mi['qty'], mi['w_make'],
                                    round(mi['w_make'] * mi['qty'], 2),
                                    f"訂單製造｜{sel_mfg_ono}", mfg_maker)
                                wage_count += 1
                            if mi['w_pack'] > 0:
                                add_wage_entry(
                                    date.today().isoformat(), mfg_packer, "產品", "包裝",
                                    mi['cat_name'], mi['qty'], mi['w_pack'],
                                    round(mi['w_pack'] * mi['qty'], 2),
                                    f"訂單包裝｜{sel_mfg_ono}", mfg_packer)
                                wage_count += 1
                            if mi['w_svc'] > 0:
                                add_wage_entry(
                                    date.today().isoformat(), mfg_svc, "產品", "服務費",
                                    mi['cat_name'], mi['qty'], mi['w_svc'],
                                    round(mi['w_svc'] * mi['qty'], 2),
                                    f"訂單服務｜{sel_mfg_ono}", mfg_svc)
                                wage_count += 1
                        if wage_count > 0:
                            st.success(f"✅ 已建立 {wage_count} 筆工資紀錄")
                        else:
                            st.warning("⚠️ 未建立任何工資紀錄（所有品項的工資金額為 0）")
                        time.sleep(1)
                        st.rerun()

    # ── 領料清單（原有功能）──────────────────────────────────────
    with t1:
        m_note = st.text_input("領料備註", key="m_note")
        c1, c2, c3 = st.columns([3, 1, 1])
        sel = c1.selectbox("原料", prods['label'], key="msel")
        wh = c2.selectbox("發料倉庫", WAREHOUSES, key="mwh"); qty = c3.number_input("數量", 1.0, key="mqty")
        if st.button("加入清單", key="madd"):
            st.session_state['m_in_list'].append({'sku': sel.split(" | ")[0], 'name': sel.split(" | ")[1], 'wh': wh, 'qty': qty})
            st.rerun()
        if st.session_state['m_in_list']:
            for i, item in enumerate(st.session_state['m_in_list']):
                st.write(f"**{item['name']}** - {item['wh']} x{item['qty']}")
                if st.button("移除", key=f"rm_m_{i}"): st.session_state['m_in_list'].pop(i); st.rerun()
            if st.button("批次確認領料", type="primary"):
                for x in st.session_state['m_in_list']:
                    add_transaction("製造領料", date.today(), x['sku'], x['wh'], x['qty'], "工廠", m_note)
                st.session_state['m_in_list'] = []; st.success("OK"); time.sleep(1); st.rerun()
    with t2:
        with st.form("m2_form"):
            sel_out = st.selectbox("成品", prods['label']); wh_out = st.selectbox("倉庫", WAREHOUSES); qty_out = st.number_input("數量", 1.0)
            if st.form_submit_button("完工確認"):
                add_transaction("製造入庫", date.today(), sel_out.split(" | ")[0], wh_out, qty_out, "工廠", "")
                st.success("OK"); time.sleep(1); st.rerun()
    with t3:
        st.info("拆解：扣成品，回原料。")
        c1, c2 = st.columns(2)
        with c1:
            with st.form("d1_form"):
                p = st.selectbox("成品", prods['label'], key="dp"); q = st.number_input("拆解量", 1.0)
                if st.form_submit_button("1. 扣除成品"):
                    add_transaction("製造入庫", date.today(), p.split(" | ")[0], "Wen", -q, "管理員", "拆解扣除")
                    st.success("OK"); time.sleep(1); st.rerun()
        with c2:
            with st.form("d2_form"):
                m = st.selectbox("原料", prods['label'], key="dm"); q = st.number_input("回庫量", 1.0)
                if st.form_submit_button("2. 回庫原料"):
                    add_transaction("製造領料", date.today(), m.split(" | ")[0], "Wen", -q, "管理員", "拆解回庫")
                    st.success("OK"); time.sleep(1); st.rerun()
    render_history_table(["製造領料", "製造入庫"])

# --- 📊 報表查詢 ---
elif page == "📊 報表查詢":
    st.subheader("📊 報表查詢")
    tab_stock, tab_profit = st.tabs(["📦 庫存報表", "📈 收益損益表"])

    with tab_stock:
        df = get_stock_overview()
        if not df.empty:
            st.dataframe(df, use_container_width=True)
            st.download_button("下載 CSV", df.to_csv(index=False).encode('utf-8-sig'), f"Stock_{date.today()}.csv", "text/csv")

    with tab_profit:
        st.markdown("##### 選擇月份")
        _profit_months = []
        for _pm_offset in range(12):
            _pm_d = date.today().replace(day=1)
            for _ in range(_pm_offset):
                _pm_d = (_pm_d - timedelta(days=1)).replace(day=1)
            _profit_months.append(_pm_d.strftime("%Y-%m"))
        _profit_months = sorted(set(_profit_months), reverse=True)
        _profit_cur = date.today().strftime("%Y-%m")
        profit_ym = st.selectbox("月份", _profit_months, index=_profit_months.index(_profit_cur) if _profit_cur in _profit_months else 0, key="profit_ym")

        # ── 1. 訂單收入（已完成訂單）──
        df_orders_rpt = load_orders()
        completed = pd.DataFrame()
        if not df_orders_rpt.empty:
            completed = df_orders_rpt[
                (df_orders_rpt['status'] == "已完成") &
                (df_orders_rpt['order_date'].astype(str).str.startswith(profit_ym))
            ]
        order_count = len(completed)
        # total_amount = 應付總額，items_total = 商品小計
        total_revenue = float(completed['total_amount'].sum()) if not completed.empty else 0.0
        shipping_income = float(completed['shipping_fee'].sum()) if not completed.empty else 0.0
        discount_total = float(completed['discount'].sum()) if not completed.empty else 0.0
        order_items_total = float(completed['items_total'].sum()) if not completed.empty else 0.0
        # 若 items_total（商品小計）為空但 total_amount（應付總額）有值，反推商品收入
        if order_items_total == 0 and total_revenue > 0:
            order_items_total = total_revenue + discount_total - shipping_income

        # ── 2. 工資支出 ──
        df_wages_rpt = load_wage_entries(profit_ym)
        wage_total = float(df_wages_rpt['amount'].sum()) if not df_wages_rpt.empty else 0.0

        # ── 3. 進貨成本 ──
        df_hist = load_data("History")
        purchase_cost = 0.0
        df_purchase_month = pd.DataFrame()
        if not df_hist.empty and 'doc_type' in df_hist.columns:
            date_col = 'date' if 'date' in df_hist.columns else df_hist.columns[2] if len(df_hist.columns) > 2 else None
            if date_col:
                df_purchase_month = df_hist[
                    (df_hist['doc_type'].astype(str) == '進貨') &
                    (df_hist[date_col].astype(str).str.startswith(profit_ym))
                ]
                if not df_purchase_month.empty and 'cost' in df_purchase_month.columns:
                    purchase_cost = float(pd.to_numeric(df_purchase_month['cost'], errors='coerce').fillna(0).sum())

        # ── 4. 計算 ──
        gross_profit = total_revenue - purchase_cost
        net_profit = gross_profit - wage_total
        margin_pct = (net_profit / total_revenue * 100) if total_revenue > 0 else 0

        # ═══════════════════════════════════════════════════════════
        # 顯示
        # ═══════════════════════════════════════════════════════════
        st.markdown(f"### 📈 {profit_ym} 損益表")

        # 四大指標
        km1, km2, km3, km4 = st.columns(4)
        km1.metric("💰 總收入", f"${total_revenue:,.0f}", f"{order_count} 筆訂單")
        km2.metric("📦 進貨成本", f"${purchase_cost:,.0f}")
        km3.metric("👷 工資支出", f"${wage_total:,.0f}")
        km4.metric("📈 淨利", f"${net_profit:,.0f}",
                    delta=f"淨利率 {margin_pct:.1f}%" if total_revenue > 0 else "無收入")

        st.markdown("---")

        # 損益明細表
        st.markdown("##### 📋 損益明細")
        pnl_rows = []
        pnl_rows.append({"項目": "🟢 商品收入", "金額": f"${order_items_total:,.0f}"})
        pnl_rows.append({"項目": "🟢 運費收入", "金額": f"${shipping_income:,.0f}"})
        if discount_total > 0:
            pnl_rows.append({"項目": "🔴 折扣", "金額": f"-${discount_total:,.0f}"})
        pnl_rows.append({"項目": "──── 總收入 ────", "金額": f"**${total_revenue:,.0f}**"})
        pnl_rows.append({"項目": "🔴 進貨成本", "金額": f"-${purchase_cost:,.0f}"})
        pnl_rows.append({"項目": "──── 毛利 ────", "金額": f"**${gross_profit:,.0f}**"})
        pnl_rows.append({"項目": "🔴 工資支出", "金額": f"-${wage_total:,.0f}"})
        pnl_rows.append({"項目": "══ 淨利 ══", "金額": f"**${net_profit:,.0f}**"})

        pnl_md = "| 項目 | 金額 |\n|------|------:|\n"
        for r in pnl_rows:
            pnl_md += f"| {r['項目']} | {r['金額']} |\n"
        st.markdown(pnl_md)

        # ── 詳細分類 ──
        st.markdown("---")
        bd1, bd2, bd3 = st.tabs(["📦 訂單明細", "👷 工資明細", "📥 進貨明細"])

        with bd1:
            if completed.empty:
                st.info(f"{profit_ym} 沒有已完成的訂單")
            else:
                st.markdown(f"共 **{order_count}** 筆已完成訂單")
                disp_cols = ['order_no', 'order_date', 'customer_name', 'items_total',
                             'discount', 'shipping_fee', 'total_amount']
                disp_cols = [c for c in disp_cols if c in completed.columns]
                disp_rename = {'order_no': '訂單號', 'order_date': '日期',
                               'customer_name': '客戶', 'items_total': '商品小計',
                               'discount': '折扣', 'shipping_fee': '運費',
                               'total_amount': '應收總額'}
                st.dataframe(completed[disp_cols].rename(columns=disp_rename),
                             use_container_width=True, hide_index=True)

                # 商品銷售排行
                st.markdown("##### 🏆 商品銷售排行")
                all_items_month = pd.DataFrame()
                for _, orow in completed.iterrows():
                    oi = load_order_items(str(orow['order_no']))
                    if not oi.empty:
                        all_items_month = pd.concat([all_items_month, oi], ignore_index=True)
                if not all_items_month.empty:
                    prod_rank = all_items_month.groupby('product_name').agg(
                        數量=('qty', 'sum'), 營收=('subtotal', 'sum')
                    ).reset_index().rename(columns={'product_name': '品名'})
                    prod_rank = prod_rank.sort_values('營收', ascending=False)
                    st.dataframe(prod_rank, use_container_width=True, hide_index=True)

        with bd2:
            if df_wages_rpt.empty:
                st.info(f"{profit_ym} 沒有工資紀錄")
            else:
                # 按員工匯總
                st.markdown("##### 👷 按員工")
                emp_sum = df_wages_rpt.groupby('employee_name')['amount'].sum().reset_index()
                emp_sum.columns = ['員工', '金額']
                emp_sum = emp_sum.sort_values('金額', ascending=False)
                for _, er in emp_sum.iterrows():
                    pct = er['金額'] / wage_total * 100 if wage_total > 0 else 0
                    st.write(f"**{er['員工']}**　NT$ {er['金額']:,.0f}　({pct:.1f}%)")

                # 按階段匯總
                if 'stage' in df_wages_rpt.columns:
                    st.markdown("##### 📊 按工作階段")
                    stage_sum = df_wages_rpt.groupby('stage')['amount'].sum().reset_index()
                    stage_sum.columns = ['階段', '金額']
                    stage_sum = stage_sum.sort_values('金額', ascending=False)
                    st.dataframe(stage_sum, use_container_width=True, hide_index=True)

                # 完整紀錄
                st.markdown("##### 📝 完整紀錄")
                w_cols = ['date', 'employee_name', 'stage', 'item', 'qty', 'price', 'amount', 'note']
                w_cols = [c for c in w_cols if c in df_wages_rpt.columns]
                w_rename = {'date': '日期', 'employee_name': '員工', 'stage': '階段',
                            'item': '項目', 'qty': '數量', 'price': '單價',
                            'amount': '金額', 'note': '備註'}
                st.dataframe(df_wages_rpt[w_cols].rename(columns=w_rename),
                             use_container_width=True, hide_index=True)

        with bd3:
            if df_purchase_month.empty:
                st.info(f"{profit_ym} 沒有進貨紀錄")
            else:
                st.markdown(f"共 **{len(df_purchase_month)}** 筆進貨")
                p_cols = ['date', 'product_name', 'sku', 'qty', 'cost', 'warehouse', 'user']
                p_cols = [c for c in p_cols if c in df_purchase_month.columns]
                p_rename = {'date': '日期', 'product_name': '品名', 'sku': '貨號',
                            'qty': '數量', 'cost': '成本', 'warehouse': '倉庫',
                            'user': '經手人'}
                st.dataframe(df_purchase_month[p_cols].rename(columns=p_rename),
                             use_container_width=True, hide_index=True)

        # 匯出 CSV
        st.markdown("---")
        export_data = pd.DataFrame([
            {"項目": "商品收入", "金額": order_items_total},
            {"項目": "運費收入", "金額": shipping_income},
            {"項目": "折扣", "金額": -discount_total},
            {"項目": "總收入", "金額": total_revenue},
            {"項目": "進貨成本", "金額": -purchase_cost},
            {"項目": "毛利", "金額": gross_profit},
            {"項目": "工資支出", "金額": -wage_total},
            {"項目": "淨利", "金額": net_profit},
        ])
        st.download_button("⬇️ 匯出損益表 CSV",
                           export_data.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'),
                           f"損益表_{profit_ym}.csv", "text/csv")

# --- 💰 工資管理 ---
elif page == "💰 工資管理":
    st.subheader("💰 工資管理系統")
    ensure_wage_sheets()

    today_str = date.today().isoformat()
    cur_ym = today_str[:7]  # e.g. "2026-05"

    tab_entry, tab_report, tab_employees, tab_catalog = st.tabs(
        ["📝 工資登錄", "📊 月度報表", "👷 員工管理", "📋 產品目錄"]
    )

    # ── 工資登錄 ──────────────────────────────────────────────────
    with tab_entry:
        st.markdown("##### 新增工資紀錄")
        df_emp = load_wage_employees()
        df_cat = load_wage_catalog()

        emp_names = df_emp['name'].tolist() if not df_emp.empty else []
        prod_names = df_cat['name'].tolist() if not df_cat.empty else []

        with st.form("wage_entry_form", clear_on_submit=True):
            wc1, wc2, wc3 = st.columns(3)
            w_date = wc1.date_input("日期", value=date.today())
            w_emp = wc2.selectbox("員工", emp_names if emp_names else ["（請先新增員工）"])
            w_cat = wc3.selectbox("工資類別", ["產品", "其他"])

            if w_cat == "產品":
                ws1, ws2 = st.columns(2)
                w_stage = ws1.selectbox("工作階段", WAGE_STAGES)
                w_item = ws2.selectbox("產品", prod_names if prod_names else ["（請先新增產品）"])

                # 自動帶入單價（含員工倍率）
                default_price = 0.0
                if prod_names and w_item in df_cat['name'].values:
                    row_p = df_cat[df_cat['name'] == w_item].iloc[0]
                    stage_col = {"製造": "wageMake", "包裝": "wagePack", "出貨": "wageShip", "服務費": "wageSvc"}.get(w_stage, "wageMake")
                    base = float(row_p.get(stage_col, 0) or 0)
                    if not df_emp.empty and w_emp in df_emp['name'].values:
                        mult = float(df_emp[df_emp['name'] == w_emp].iloc[0].get('multProd', 1) or 1)
                    else:
                        mult = 1.0
                    default_price = round(base * mult, 2)

                wp1, wp2 = st.columns(2)
                w_qty = wp1.number_input("數量", min_value=0.01, value=1.0, step=0.5)
                w_price = wp2.number_input("單價 (元)", min_value=0.0, value=default_price, step=5.0)
                w_amount = w_qty * w_price
                st.info(f"💵 金額小計：NT$ {w_amount:,.2f}")
                w_custom_name = ""
                w_direct_amount = 0.0
            else:
                w_custom_name = st.text_input("自訂項目名稱")
                w_direct_amount = st.number_input("金額 (元)", min_value=0.0, value=0.0, step=10.0)
                w_stage = ""
                w_item = ""
                w_qty = 0.0
                w_price = 0.0
                w_amount = w_direct_amount

            w_note = st.text_input("備註（選填）")
            w_creator = st.selectbox("登錄人", KEYERS)

            if st.form_submit_button("✅ 新增紀錄", use_container_width=True):
                if not emp_names:
                    st.error("請先到「員工管理」新增員工")
                elif w_cat == "其他" and not w_custom_name.strip():
                    st.error("請填寫自訂項目名稱")
                else:
                    item_name = w_item if w_cat == "產品" else w_custom_name.strip()
                    qty_val = w_qty if w_cat == "產品" else None
                    price_val = w_price if w_cat == "產品" else None
                    if add_wage_entry(w_date.isoformat(), w_emp, w_cat, w_stage, item_name, qty_val, price_val, w_amount, w_note, w_creator):
                        st.success("工資紀錄已新增！")
                        time.sleep(1)
                        st.rerun()

        st.markdown("---")
        st.markdown(f"##### 本月紀錄（{cur_ym}）")
        df_this = load_wage_entries(cur_ym)
        if df_this.empty:
            st.info("本月尚無工資紀錄")
        else:
            total_this = df_this['amount'].sum()
            st.markdown(f"共 **{len(df_this)}** 筆　合計 **NT$ {total_this:,.0f}**")
            for w_idx, (_, er) in enumerate(df_this.sort_values('date', ascending=False).iterrows()):
                ec1, ec2, ec3, ec4, ec5, ec6, ec7 = st.columns([1.2, 1.2, 1.8, 1.8, 1, 0.6, 0.6])
                ec1.text(str(er.get('date', '')))
                ec2.text(str(er.get('employee_name', '')))
                cat_txt = str(er.get('category', ''))
                stg_txt = str(er.get('stage', ''))
                ec3.text(f"{cat_txt}{' · ' + stg_txt if stg_txt else ''}")
                ec4.text(str(er.get('item', '')))
                ec5.text(f"${float(er.get('amount', 0)):,.0f}")
                _is_p = str(er.get('paid', '')).upper() == 'Y'
                ec6.markdown("✅" if _is_p else "⏳")
                if ec7.button("🗑️", key=f"wage_del_{w_idx}_{er.get('id', '')}"):
                    if delete_wage_entry(str(er.get('id', ''))):
                        st.success("已刪除")
                        time.sleep(1)
                        st.rerun()
            st.divider()
            # 匯出 CSV
            csv_cols = ['date', 'employee_name', 'category', 'stage', 'item', 'qty', 'price', 'amount', 'note']
            csv_rename = {'date': '日期', 'employee_name': '員工', 'category': '類別',
                          'stage': '階段', 'item': '項目', 'qty': '數量',
                          'price': '單價', 'amount': '金額', 'note': '備註'}
            exist_cols = [c for c in csv_cols if c in df_this.columns]
            csv_data = df_this[exist_cols].rename(columns=csv_rename)
            st.download_button("⬇️ 匯出本月 CSV", csv_data.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'),
                               f"工資報表_{cur_ym}.csv", "text/csv")

    # ── 月度報表 ──────────────────────────────────────────────────
    with tab_report:
        st.markdown("##### 選擇月份")
        import datetime as _dt
        # 產生最近 12 個月的選項
        _rpt_months = []
        for _m_offset in range(12):
            _m_d = date.today().replace(day=1)
            for _ in range(_m_offset):
                _m_d = (_m_d - timedelta(days=1)).replace(day=1)
            _rpt_months.append(_m_d.strftime("%Y-%m"))
        _rpt_months = sorted(set(_rpt_months), reverse=True)
        rpt_ym = st.selectbox("月份", _rpt_months, index=_rpt_months.index(cur_ym) if cur_ym in _rpt_months else 0, key="rpt_ym_select")

        df_rpt = load_wage_entries(rpt_ym)
        settlements = load_wage_settlements()
        is_settled = rpt_ym in settlements

        # 補建工資按鈕（放在報表最上方）
        if st.button(f"🔄 補建 {rpt_ym} 缺失工資", key=f"backfill_{rpt_ym}", help="掃描該月已完成訂單，自動補建缺失的製造/包裝/出貨/服務費工資"):
            with st.spinner("掃描訂單並補建工資中..."):
                bf_orders, bf_entries, bf_warnings = backfill_wage_entries_for_month(rpt_ym)
            if bf_entries > 0:
                st.success(f"✅ 已為 {bf_orders} 筆訂單補建 {bf_entries} 筆工資紀錄")
            else:
                st.info("無新增工資紀錄")
            if bf_warnings:
                st.warning("以下品項無法自動建立工資（請至「產品目錄」設定負責人）：")
                for _bfw in bf_warnings:
                    st.caption(_bfw)
            elif bf_entries > 0:
                time.sleep(1.5)
                st.rerun()

        if df_rpt.empty:
            st.info(f"**{rpt_ym}** 尚無工資紀錄")
        else:
            grand_total = df_rpt['amount'].sum()
            paid_total = df_rpt[df_rpt['paid'].astype(str).str.upper() == 'Y']['amount'].sum()
            unpaid_total = grand_total - paid_total
            rc1, rc2, rc3 = st.columns(3)
            rc1.metric("合計", f"NT$ {grand_total:,.0f}")
            rc2.metric("已發放", f"NT$ {paid_total:,.0f}")
            rc3.metric("未發放", f"NT$ {unpaid_total:,.0f}")

            # 確保必要欄位存在
            for _col in ['employee_name', 'amount', 'category', 'stage', 'id']:
                if _col not in df_rpt.columns:
                    df_rpt[_col] = ""

            # 按員工分組
            for emp_name, grp in df_rpt.groupby('employee_name'):
                emp_total = grp['amount'].sum()
                emp_paid = grp[grp['paid'].astype(str).str.upper() == 'Y']['amount'].sum()
                emp_unpaid = emp_total - emp_paid
                paid_badge = "✅ 全部已發放" if emp_unpaid <= 0 else f"⏳ 未發放 NT$ {emp_unpaid:,.0f}"
                with st.expander(f"👷 {emp_name}　NT$ {emp_total:,.0f}　{paid_badge}"):
                    grp_by_cols = [c for c in ['category', 'stage'] if c in grp.columns]
                    if grp_by_cols:
                        cat_summary = grp.groupby(grp_by_cols)['amount'].sum().reset_index()
                        for _, cs in cat_summary.iterrows():
                            cat_lbl = str(cs.get('category', ''))
                            if 'stage' in cs and cs['stage']:
                                cat_lbl += f" · {cs['stage']}"
                            st.write(f"  {cat_lbl}：NT$ {cs['amount']:,.0f}")
                    st.markdown("---")
                    # 顯示每筆工資及發放狀態
                    for _wi, (_, _wr) in enumerate(grp.sort_values('date', ascending=False).iterrows()):
                        _wc1, _wc2, _wc3, _wc4, _wc5, _wc6 = st.columns([1.2, 1.5, 2, 1.2, 1, 1])
                        _wc1.text(str(_wr.get('date', ''))[-5:])
                        _stg = str(_wr.get('stage', ''))
                        _wc2.text(_stg if _stg else str(_wr.get('category', '')))
                        _wc3.text(str(_wr.get('item', '')))
                        _wc4.text(f"${float(_wr.get('amount', 0)):,.0f}")
                        _is_paid = str(_wr.get('paid', '')).upper() == 'Y'
                        _entry_id = str(_wr.get('id', ''))
                        if _is_paid:
                            _wc5.markdown("✅ 已發")
                            if _wc6.button("↩️", key=f"unpay_{rpt_ym}_{_wi}_{_entry_id}", help="取消發放"):
                                mark_wage_entry_paid(_entry_id, paid=False)
                                st.rerun()
                        else:
                            _wc5.markdown("⏳ 未發")
                            if _wc6.button("💰", key=f"pay_{rpt_ym}_{_wi}_{_entry_id}", help="標記已發放"):
                                mark_wage_entry_paid(_entry_id, paid=True)
                                st.rerun()
                    # 一鍵發放此員工全部
                    _emp_unpaid_ids = [str(r.get('id', '')) for _, r in grp.iterrows() if str(r.get('paid', '')).upper() != 'Y']
                    if _emp_unpaid_ids:
                        if st.button(f"💰 全部標記已發放（{emp_name}）", key=f"payall_{rpt_ym}_{emp_name}", use_container_width=True):
                            for _eid in _emp_unpaid_ids:
                                mark_wage_entry_paid(_eid, paid=True)
                            st.rerun()

            st.divider()
            rpt_cols = ['date', 'employee_name', 'category', 'stage', 'item', 'qty', 'price', 'amount', 'paid', 'note']
            rpt_rename = {'date': '日期', 'employee_name': '員工', 'category': '類別',
                          'stage': '階段', 'item': '項目', 'qty': '數量',
                          'price': '單價', 'amount': '金額', 'paid': '已發放', 'note': '備註'}
            exist_rpt_cols = [c for c in rpt_cols if c in df_rpt.columns]
            csv_rpt = df_rpt[exist_rpt_cols].rename(columns=rpt_rename)
            st.download_button("⬇️ 匯出報表 CSV", csv_rpt.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'),
                               f"工資報表_{rpt_ym}.csv", "text/csv")

    # ── 員工管理 ──────────────────────────────────────────────────
    with tab_employees:
        st.markdown("##### 新增 / 更新員工")
        with st.form("add_emp_form", clear_on_submit=True):
            ae1, ae2, ae3 = st.columns([2, 1, 1])
            ae_name = ae1.text_input("員工姓名 *必填")
            ae_mult = ae2.number_input("工資倍率", min_value=0.1, value=1.0, step=0.1,
                                        help="套用在所有產品基本工資上的倍率，預設 1.0")
            if st.form_submit_button("儲存員工", use_container_width=True):
                if not ae_name.strip():
                    st.error("請填寫員工姓名")
                else:
                    save_wage_employee(ae_name.strip(), ae_mult)
                    st.success(f"員工 {ae_name.strip()} 已儲存")
                    time.sleep(1)
                    st.rerun()

        st.markdown("---")
        df_emp2 = load_wage_employees()
        if df_emp2.empty:
            st.info("尚無員工，請新增")
        else:
            st.markdown(f"共 **{len(df_emp2)}** 位員工")
            for _, er in df_emp2.iterrows():
                emc1, emc2, emc3 = st.columns([3, 2, 1])
                emc1.write(f"**{er.get('name', '')}**")
                emc2.write(f"倍率：{er.get('multProd', 1)}")
                if emc3.button("刪除", key=f"del_emp_{er.get('id', er.get('name', ''))}"):
                    if delete_wage_employee(str(er.get('name', ''))):
                        st.success("已刪除")
                        time.sleep(1)
                        st.rerun()

    # ── 產品目錄 ──────────────────────────────────────────────────
    with tab_catalog:
        st.markdown("##### 新增 / 編輯產品工資")
        df_cat2 = load_wage_catalog()
        prod_list2 = df_cat2['name'].tolist() if not df_cat2.empty else []
        edit_mode = st.selectbox("選擇已有產品編輯（或留空新增）", ["（新增產品）"] + prod_list2)

        default_vals = {"wageMake": 0.0, "wagePack": 0.0, "wageShip": 0.0, "wageSvc": 0.0,
                        "empMake": "", "empPack": "", "empShip": "", "empSvc": ""}
        default_name = ""
        if edit_mode != "（新增產品）" and not df_cat2.empty:
            row_e = df_cat2[df_cat2['name'] == edit_mode]
            if not row_e.empty:
                default_name = edit_mode
                for k in ["wageMake", "wagePack", "wageShip", "wageSvc"]:
                    default_vals[k] = float(row_e.iloc[0].get(k, 0) or 0)
                for k in ["empMake", "empPack", "empShip", "empSvc"]:
                    v = str(row_e.iloc[0].get(k, '') or '')
                    default_vals[k] = "" if v.lower() == 'nan' else v

        emp_options = ["（不設定）"] + [e for e in load_wage_employees()['name'].tolist() if e]

        with st.form("prod_wage_form", clear_on_submit=False):
            cp_name = st.text_input("產品名稱 *必填", value=default_name)
            st.markdown("**工資設定**")
            cp1, cp2, cp3, cp4 = st.columns(4)
            cp_make = cp1.number_input("製造工資/件", min_value=0.0, value=default_vals["wageMake"], step=1.0)
            cp_pack = cp2.number_input("包裝工資/件", min_value=0.0, value=default_vals["wagePack"], step=1.0)
            cp_ship = cp3.number_input("出貨工資/件", min_value=0.0, value=default_vals["wageShip"], step=1.0)
            cp_svc  = cp4.number_input("服務費/件",   min_value=0.0, value=default_vals["wageSvc"],  step=10.0)

            st.markdown("**訂單完成時自動帶入的員工**")
            st.caption("設定後，訂單變為「已完成」時會自動建立對應工資紀錄")
            ea1, ea2, ea3, ea4 = st.columns(4)
            def_idx = lambda k: emp_options.index(default_vals[k]) if default_vals[k] in emp_options else 0
            cp_emp_make = ea1.selectbox("製造負責人", emp_options, index=def_idx("empMake"), key="cp_em")
            cp_emp_pack = ea2.selectbox("包裝負責人", emp_options, index=def_idx("empPack"), key="cp_ep")
            cp_emp_ship = ea3.selectbox("出貨負責人", emp_options, index=def_idx("empShip"), key="cp_es")
            cp_emp_svc  = ea4.selectbox("服務負責人", emp_options, index=def_idx("empSvc"),  key="cp_ev")

            if st.form_submit_button("💾 儲存產品", use_container_width=True):
                if not cp_name.strip():
                    st.error("請填寫產品名稱")
                else:
                    em = "" if cp_emp_make == "（不設定）" else cp_emp_make
                    ep = "" if cp_emp_pack == "（不設定）" else cp_emp_pack
                    es = "" if cp_emp_ship == "（不設定）" else cp_emp_ship
                    ev = "" if cp_emp_svc  == "（不設定）" else cp_emp_svc
                    orig = edit_mode if edit_mode != "（新增產品）" else ""
                    save_wage_product(cp_name.strip(), cp_make, cp_pack, cp_ship, cp_svc, em, ep, es, ev, original_name=orig)
                    st.success(f"產品「{cp_name.strip()}」已儲存")
                    time.sleep(1)
                    st.rerun()

        st.markdown("---")
        st.markdown(f"##### 產品目錄（共 {len(df_cat2)} 項）")
        if not df_cat2.empty:
            cat_disp_cols = ['name', 'wageMake', 'wagePack', 'wageShip', 'wageSvc', 'empMake', 'empPack', 'empShip', 'empSvc']
            cat_disp_rename = {'name': '產品名稱', 'wageMake': '製造/件', 'wagePack': '包裝/件',
                               'wageShip': '出貨/件', 'wageSvc': '服務費/件',
                               'empMake': '製造負責人', 'empPack': '包裝負責人', 'empShip': '出貨負責人',
                               'empSvc': '服務負責人'}
            cat_exist = [c for c in cat_disp_cols if c in df_cat2.columns]
            display_cat = df_cat2[cat_exist].rename(columns=cat_disp_rename)
            st.dataframe(display_cat, use_container_width=True, hide_index=True)

            st.markdown("##### 刪除產品")
            del_prod = st.selectbox("選擇要刪除的產品", prod_list2, key="del_prod_sel")
            if st.button("🗑️ 刪除此產品", key="del_prod_btn"):
                if delete_wage_product(del_prod):
                    st.success(f"已刪除「{del_prod}」")
                    time.sleep(1)
                    st.rerun()

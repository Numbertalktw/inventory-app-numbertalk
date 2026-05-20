import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime
import time

# ==========================================
# 1. 系統基礎設定
# ==========================================
PAGE_TITLE = "numbertalk 雲端庫存系統"
SPREADSHEET_NAME = "numbertalk-system"

WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]
CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品", "數字珠", "數字串", "香料", "手作設備"]
SERIES = ["原料", "半成品", "成品", "包材", "生命數字能量項鍊", "數字手鍊", "貼紙", "小卡", "火漆章", "能量蠟燭", "香包", "水晶", "魔法鹽"]
KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]
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

@st.cache_data(ttl=60)
def load_data(sheet_name):
    try:
        ws = get_worksheet(sheet_name)
        if ws is None: return pd.DataFrame()
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        for col in ['sku', 'name', 'category', 'series', 'spec', 'color', 'note', 'price']:
            if col not in df.columns: df[col] = ""
        return df.fillna("")
    except: return pd.DataFrame()

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

@st.cache_data(ttl=60)
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
            ws = sh.add_worksheet(title="Orders", rows=1000, cols=15)
            ws.append_row(["order_no", "order_date", "customer_name", "customer_phone",
                           "customer_email", "shipping_address", "status", "total_amount",
                           "note", "created_by", "created_at", "discount", "shipping_fee",
                           "items_total"])
        if "OrderItems" not in existing:
            ws = sh.add_worksheet(title="OrderItems", rows=5000, cols=8)
            ws.append_row(["order_no", "sku", "product_name", "qty", "unit_price",
                           "subtotal", "warehouse"])
        st.session_state['_order_sheets_ok'] = True
    except Exception:
        pass

def generate_order_no():
    now = datetime.now()
    return f"ORD-{now.strftime('%Y%m%d')}-{int(time.time()) % 100000:05d}"

def create_order(order_no, order_date, customer_name, customer_phone,
                 customer_email, shipping_address, items, note, created_by,
                 discount=0, shipping_fee=0):
    ensure_order_sheets()
    ws_orders = get_worksheet_for_write("Orders")
    ws_items = get_worksheet_for_write("OrderItems")
    if not ws_orders or not ws_items:
        return False, "無法連線到工作表"
    try:
        items_total = sum(item['subtotal'] for item in items)
        total = items_total - float(discount) + float(shipping_fee)
        ws_orders.append_row([
            order_no, str(order_date), customer_name, customer_phone,
            customer_email, shipping_address, "已確認", float(total),
            note, created_by, str(datetime.now()),
            float(discount), float(shipping_fee), float(items_total)
        ])
        for item in items:
            ws_items.append_row([
                order_no, item['sku'], item['product_name'],
                float(item['qty']), float(item['unit_price']),
                float(item['subtotal']), item['warehouse']
            ])
        clear_cache()
        save_member(customer_name, customer_phone, customer_email, shipping_address)
        return True, f"訂單 {order_no} 建立成功"
    except Exception as e:
        return False, f"建立失敗: {e}"

def load_orders():
    ensure_order_sheets()
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
    ws = get_worksheet_for_write("Orders")
    if not ws:
        st.error("無法連線到 Orders 工作表")
        return False
    try:
        all_vals = ws.get_all_values()
        header = all_vals[0]
        no_idx = header.index("order_no")
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
    """重新計算訂單總額並更新"""
    items = load_order_items(order_no)
    items_total = float(items['subtotal'].sum()) if not items.empty else 0.0
    total = items_total - float(discount) + float(shipping_fee)
    return update_order_fields(order_no, {
        'items_total': float(items_total),
        'total_amount': float(total),
        'discount': float(discount),
        'shipping_fee': float(shipping_fee)
    })

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
            ws = sh.add_worksheet(title="Members", rows=2000, cols=8)
            ws.append_row(["member_id", "name", "phone", "email", "address",
                           "note", "created_at", "last_order_date"])
        st.session_state['_members_sheet_ok'] = True
    except Exception:
        pass

def load_members():
    ensure_members_sheet()
    df = load_data("Members")
    if df.empty:
        return pd.DataFrame(columns=["member_id", "name", "phone", "email",
                                      "address", "note", "created_at", "last_order_date"])
    return df

def find_member_by_name(name):
    df = load_members()
    if df.empty:
        return None
    match = df[df['name'].astype(str) == str(name)]
    return match.iloc[0] if not match.empty else None

def save_member(name, phone, email, address, note=""):
    ensure_members_sheet()
    ws = get_worksheet_for_write("Members")
    if not ws:
        return False
    try:
        existing = load_members()
        if not existing.empty:
            match = existing[existing['name'].astype(str) == str(name)]
            if not match.empty:
                all_vals = ws.get_all_values()
                header = all_vals[0]
                name_idx = header.index("name")
                for i, row in enumerate(all_vals[1:], 2):
                    if str(row[name_idx]) == str(name):
                        ph_idx = header.index("phone")
                        em_idx = header.index("email")
                        ad_idx = header.index("address")
                        dt_idx = header.index("last_order_date")
                        if phone:
                            ws.update_cell(i, ph_idx + 1, phone)
                        if email:
                            ws.update_cell(i, em_idx + 1, email)
                        if address:
                            ws.update_cell(i, ad_idx + 1, address)
                        ws.update_cell(i, dt_idx + 1, str(date.today()))
                        clear_cache()
                        return True
        mid = f"M-{int(time.time()) % 100000:05d}"
        ws.append_row([mid, name, phone, email, address, note,
                       str(datetime.now()), str(date.today())])
        clear_cache()
        return True
    except Exception:
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
# 4. 主程式分頁
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="💎")
st.title(f"💎 {PAGE_TITLE}")
ensure_price_column()

with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["🛒 訂單管理", "👥 會員管理", "🔨 製造作業", "🚚 出貨作業", "📦 商品管理", "📥 進貨作業", "📦 移庫作業", "📊 報表查詢"])
    if st.button("刷新資料"):
        clear_cache()
        st.rerun()

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
    st.subheader("🚚 銷售出貨 (多品項清單)")
    if 'out_list' not in st.session_state: st.session_state['out_list'] = []
    col_a, col_b, col_c = st.columns(3)
    ship_opt = col_a.selectbox("寄送方式", SHIPPING_METHODS + ["手動輸入..."])
    final_ship = col_a.text_input("自訂方式") if ship_opt == "手動輸入..." else ship_opt
    ship_no = col_b.text_input("配送號碼")
    user = col_c.selectbox("經手人", KEYERS, index=3)
    order_id = st.text_input("訂單編號 / 備註")
    st.divider()
    prods = get_formatted_product_df()
    if not prods.empty:
        col1, col2, col3 = st.columns([3, 1, 1])
        sel_p = col1.selectbox("挑選商品", prods['label'])
        wh = col2.selectbox("倉庫", WAREHOUSES, index=3); qty = col3.number_input("數量", 1.0)
        if st.button("加入待出貨清單"):
            st.session_state['out_list'].append({'sku': sel_p.split(" | ")[0], 'name': sel_p.split(" | ")[1], 'wh': wh, 'qty': qty})
            st.rerun()
    if st.session_state['out_list']:
        for i, item in enumerate(st.session_state['out_list']):
            c_l, c_d = st.columns([5, 1])
            c_l.write(f"**{item['name']}** - {item['wh']} x{item['qty']}")
            if c_d.button("移除", key=f"rm_o_{i}"): st.session_state['out_list'].pop(i); st.rerun()
        if st.button("確認出貨", type="primary", use_container_width=True):
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
            else:
                for k in ["o_cname", "o_cphone", "o_cemail", "o_caddr"]:
                    st.session_state[k] = ""

        cc1, cc2 = st.columns(2)
        cust_name = cc1.text_input("客戶名稱 *必填", key="o_cname")
        cust_phone = cc2.text_input("聯絡電話", key="o_cphone")
        cc3, cc4 = st.columns(2)
        cust_email = cc3.text_input("Email", key="o_cemail")
        ship_addr = cc4.text_input("寄送地址", key="o_caddr")
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
                        o_discount, o_ship_fee
                    )
                    if ok:
                        st.session_state['order_items'] = []
                        st.success(msg)
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg)

    with tab_list:
        df_orders = load_orders()
        if df_orders.empty:
            st.info("目前沒有任何訂單")
        else:
            search_q = st.text_input("搜尋 (訂單號/客戶名)", key="o_search")
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
                                e_note = st.text_input("備註", value=str(row.get('note', '')))
                                if st.form_submit_button("💾 儲存客戶資訊", use_container_width=True):
                                    if update_order_fields(ono, {
                                        'customer_name': e_name, 'customer_phone': e_phone,
                                        'customer_email': e_email, 'shipping_address': e_addr,
                                        'note': e_note
                                    }):
                                        st.success("客戶資訊已更新")
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
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        st.error(msg)
                                else:
                                    if update_order_status(ono, new_st):
                                        st.success(f"狀態已更新為: {new_st}")
                                        time.sleep(1)
                                        st.rerun()

                            if status in ["已確認", "已成立", "待處理"]:
                                if ac2.button("刪除", key=f"{kp}_del_{ono}"):
                                    if delete_order(ono):
                                        st.success("訂單已刪除")
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
                        e_note = st.text_input("備註", value=str(row.get('note', '')))
                        if st.form_submit_button("💾 儲存客戶資訊", use_container_width=True):
                            updates = {
                                'customer_name': e_name,
                                'customer_phone': e_phone,
                                'customer_email': e_email,
                                'shipping_address': e_addr,
                                'note': e_note
                            }
                            if update_order_fields(sel_ono, updates):
                                st.success("客戶資訊已更新")
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
            st.dataframe(
                filtered_m[['name', 'phone', 'email', 'address', 'last_order_date']].rename(
                    columns={'name': '姓名', 'phone': '電話', 'email': 'Email',
                             'address': '地址', 'last_order_date': '最後訂單日期'}
                ),
                use_container_width=True, hide_index=True
            )

            st.markdown("---")
            st.markdown("##### 編輯 / 刪除會員")
            m_names = filtered_m['name'].astype(str).tolist()
            if m_names:
                sel_m = st.selectbox("選擇會員", m_names, key="m_edit_sel")
                m_data = find_member_by_name(sel_m)
                if m_data is not None:
                    with st.form("edit_member"):
                        em_phone = st.text_input("電話", value=str(m_data.get('phone', '')))
                        em_email = st.text_input("Email", value=str(m_data.get('email', '')))
                        em_addr = st.text_input("地址", value=str(m_data.get('address', '')))
                        if st.form_submit_button("儲存修改"):
                            save_member(sel_m, em_phone, em_email, em_addr)
                            st.success("會員資料已更新")
                            time.sleep(1)
                            st.rerun()
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
            am_note = st.text_input("備註")
            if st.form_submit_button("新增會員", use_container_width=True):
                if not am_name:
                    st.error("請填寫姓名")
                else:
                    existing = find_member_by_name(am_name)
                    if existing is not None:
                        st.warning(f"會員 '{am_name}' 已存在，將更新資料")
                    save_member(am_name, am_phone, am_email, am_addr, am_note)
                    st.success(f"會員 '{am_name}' 已儲存")
                    time.sleep(1)
                    st.rerun()

# --- 🔨 製造作業 ---
elif page == "🔨 製造作業":
    st.subheader("🔨 生產與拆解管理")
    if 'm_in_list' not in st.session_state: st.session_state['m_in_list'] = []
    prods = get_formatted_product_df()
    t1, t2, t3 = st.tabs(["領料清單", "完工入庫", "產品拆解"])
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
    st.subheader("📊 庫存報表")
    df = get_stock_overview()
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        st.download_button("下載 CSV", df.to_csv(index=False).encode('utf-8-sig'), f"Stock_{date.today()}.csv", "text/csv")

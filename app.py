import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime
import time
import re

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

def ship_order(order_no, keyer, ship_method="", ship_no="",
               target_status="未付款/已出貨", stage_keyers=None, created_by=None):
    """執行出貨:扣庫存 + 更新狀態 + (若有 stage_keyers) 同步產生工資紀錄。

    stage_keyers 範例: {'make': 'James', 'pack': '千畇', 'ship': 'Imeng', 'svc': ''}
    任一階段填空字串 → 該階段不計薪。
    """
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
    if not update_order_status(order_no, target_status):
        return False, "出貨紀錄已建立但狀態更新失敗"

    # 連動工資 ─ 出貨時即時產生工資紀錄
    wage_msg = ""
    if stage_keyers and any(stage_keyers.values()):
        items_list = items.to_dict('records')
        created, skipped, _msgs = create_wage_entries_for_items(
            items_list, stage_keyers, order_no, created_by or keyer
        )
        if created > 0:
            wage_msg = f",已連動產生 {created} 筆工資紀錄"
        if skipped > 0:
            wage_msg += f"({skipped} 筆商品未對應工資設定,已跳過)"
    return True, f"出貨完成,庫存已扣除{wage_msg}"

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

# Members 工作表完整欄位 (birthday=生日, birth_time=出生時間,用於生命數字/紫微等命理)
MEMBER_COLUMNS = ["member_id", "name", "phone", "email", "address",
                  "birthday", "birth_time", "note", "created_at", "last_order_date"]

def ensure_members_sheet():
    if st.session_state.get('_members_sheet_ok'):
        return
    sh = get_spreadsheet()
    if not sh:
        return
    try:
        existing = [ws.title for ws in sh.worksheets()]
        if "Members" not in existing:
            ws = sh.add_worksheet(title="Members", rows=2000, cols=len(MEMBER_COLUMNS))
            ws.append_row(MEMBER_COLUMNS)
        else:
            # 既有工作表 → 補上缺少的欄位 (birthday / birth_time),不動既有資料
            ws = sh.worksheet("Members")
            header = ws.row_values(1)
            for col in ["birthday", "birth_time"]:
                if col not in header:
                    header.append(col)
                    ws.update_cell(1, len(header), col)
        st.session_state['_members_sheet_ok'] = True
    except Exception:
        pass

def load_members():
    ensure_members_sheet()
    df = load_data("Members")
    if df.empty:
        return pd.DataFrame(columns=MEMBER_COLUMNS)
    # 確保新欄位存在 (舊資料可能還沒這兩欄)
    for col in ["birthday", "birth_time"]:
        if col not in df.columns:
            df[col] = ""
    return df

def find_member_by_name(name):
    df = load_members()
    if df.empty:
        return None
    match = df[df['name'].astype(str) == str(name)]
    return match.iloc[0] if not match.empty else None

def save_member(name, phone, email, address, note="", birthday="", birth_time=""):
    ensure_members_sheet()
    birthday = normalize_birthday(birthday)  # 統一為 YYYY/MM/DD
    ws = get_worksheet_for_write("Members")
    if not ws:
        return False
    try:
        all_vals = ws.get_all_values()
        header = all_vals[0] if all_vals else MEMBER_COLUMNS
        name_idx = header.index("name") if "name" in header else 1

        # 依欄位名稱安全取得 index 的小工具
        def col_i(col_name):
            return header.index(col_name) if col_name in header else -1

        # 1. 既有會員 → 更新欄位
        for i, row in enumerate(all_vals[1:], 2):
            if len(row) > name_idx and str(row[name_idx]) == str(name):
                updates = {
                    "phone": phone, "email": email, "address": address,
                    "birthday": birthday, "birth_time": birth_time,
                    "last_order_date": str(date.today()),
                }
                # note 只在有傳入時才覆蓋
                if note:
                    updates["note"] = note
                for col_name, val in updates.items():
                    ci = col_i(col_name)
                    # 生日/出生時間/電話等:有值才覆蓋,避免清空既有資料
                    if ci >= 0 and (val or col_name == "last_order_date"):
                        ws.update_cell(i, ci + 1, val)
                clear_cache()
                return True

        # 2. 新會員 → 依 header 順序組 row
        mid = f"M-{int(time.time()) % 100000:05d}"
        value_map = {
            "member_id": mid, "name": name, "phone": phone, "email": email,
            "address": address, "birthday": birthday, "birth_time": birth_time,
            "note": note, "created_at": str(datetime.now()),
            "last_order_date": str(date.today()),
        }
        new_row = [value_map.get(h, "") for h in header]
        ws.append_row(new_row)
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


def normalize_birthday(s):
    """將各種日期輸入正規化為 YYYY/MM/DD (例: 2000/01/05)。
    支援: 2000-1-5 / 2000.1.5 / 2000/01/05 / 20000105。
    無法解析(如「不詳」、農曆描述)則原樣返回。"""
    s = str(s or '').strip()
    if not s:
        return ''
    # 8 碼純數字: 20000105
    if re.fullmatch(r'\d{8}', s):
        return f"{s[0:4]}/{int(s[4:6]):02d}/{int(s[6:8]):02d}"
    # 以 - / . 空格 分隔的年月日
    digits = re.findall(r'\d+', s)
    if len(digits) >= 3 and len(digits[0]) == 4:
        try:
            y, m, d = int(digits[0]), int(digits[1]), int(digits[2])
            if 1 <= m <= 12 and 1 <= d <= 31:
                return f"{y:04d}/{m:02d}/{d:02d}"
        except ValueError:
            pass
    return s


# ==========================================
# 3.6b 會員關係鏈核心功能
# ==========================================
# 在會員之間建立關係 (配偶/父子/母女/朋友等),支援雙向查詢。
# 儲存語意: "member_a 與 member_b 為 [relation_type]"
# 親子類 (父子/父女/母子/母女): 會員A = 長輩(父/母),會員B = 晚輩(子/女)
RELATION_TYPES = ["配偶", "伴侶", "父子", "父女", "母子", "母女",
                  "兄弟姊妹", "祖父母", "孫子女", "親戚", "朋友", "同事", "其他"]
# 親子類為「成對名稱」,本身已含雙方角色,反向視為同一關係
RELATION_REVERSE = {
    "配偶": "配偶", "伴侶": "伴侶",
    "父子": "父子", "父女": "父女", "母子": "母子", "母女": "母女",
    "兄弟姊妹": "兄弟姊妹", "祖父母": "孫子女", "孫子女": "祖父母",
    "親戚": "親戚", "朋友": "朋友", "同事": "同事", "其他": "其他",
}
# 親子類關係:A 為長輩、B 為晚輩,顯示時標明方向
PARENT_CHILD_RELATIONS = {"父子", "父女", "母子", "母女"}
RELATION_COLUMNS = ["relation_id", "member_a", "relation_type",
                    "member_b", "note", "created_at"]


def ensure_relations_sheet():
    if st.session_state.get('_relations_sheet_ok'):
        return
    sh = get_spreadsheet()
    if not sh:
        return
    try:
        existing = [ws.title for ws in sh.worksheets()]
        if "MemberRelations" not in existing:
            ws = sh.add_worksheet(title="MemberRelations", rows=2000,
                                  cols=len(RELATION_COLUMNS))
            ws.append_row(RELATION_COLUMNS)
        st.session_state['_relations_sheet_ok'] = True
    except Exception:
        pass


def load_relations():
    ensure_relations_sheet()
    df = load_data("MemberRelations")
    if df.empty:
        return pd.DataFrame(columns=RELATION_COLUMNS)
    for col in RELATION_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df


def add_relation(member_a, relation_type, member_b, note=""):
    """新增關係: member_a 的 relation_type 是 member_b。"""
    if not member_a or not member_b or member_a == member_b:
        return False, "請選擇兩位不同的會員"
    ensure_relations_sheet()
    ws = get_worksheet_for_write("MemberRelations")
    if not ws:
        return False, "無法連線到 MemberRelations 工作表"
    try:
        # 避免重複建立同一組關係
        existing = load_relations()
        if not existing.empty:
            dup = existing[
                (existing['member_a'].astype(str) == str(member_a)) &
                (existing['member_b'].astype(str) == str(member_b)) &
                (existing['relation_type'].astype(str) == str(relation_type))
            ]
            if not dup.empty:
                return False, "此關係已存在"
        rid = f"R-{int(time.time() * 1000) % 10**10}"
        ws.append_row([rid, member_a, relation_type, member_b, note,
                       str(datetime.now())])
        clear_cache()
        if relation_type in PARENT_CHILD_RELATIONS:
            return True, f"已建立關係: {member_a}(長輩) 與 {member_b}(晚輩) 為 {relation_type}"
        return True, f"已建立關係: {member_a} 與 {member_b} 為 {relation_type}"
    except Exception as e:
        return False, f"建立失敗: {e}"


def delete_relation(relation_id):
    ws = get_worksheet_for_write("MemberRelations")
    if not ws:
        return False
    try:
        all_vals = ws.get_all_values()
        header = all_vals[0]
        rid_idx = header.index("relation_id")
        for i, row in enumerate(all_vals[1:], 2):
            if len(row) > rid_idx and str(row[rid_idx]) == str(relation_id):
                ws.delete_rows(i)
                clear_cache()
                return True
        return False
    except Exception:
        return False


def describe_relation(viewer, target, relation_type, viewer_is_a):
    """產生一句清楚的關係描述。
    親子類: A=長輩(父/母), B=晚輩(子/女)。
    例: 父子 → 長輩王大 對 晚輩王小,viewer 是長輩則「王大 是 王小 的 父親」。
    """
    if relation_type in PARENT_CHILD_RELATIONS:
        elder_role = "父" if relation_type[0] == "父" else "母"
        younger_role = "子" if relation_type[1] == "子" else "女"
        elder_word = "父親" if elder_role == "父" else "母親"
        younger_word = "兒子" if younger_role == "子" else "女兒"
        if viewer_is_a:  # viewer 是長輩
            return f"{viewer} 是 {target} 的 {elder_word}（{relation_type}）"
        else:            # viewer 是晚輩
            return f"{viewer} 是 {target} 的 {younger_word}（{relation_type}）"
    else:
        # 對稱關係: 配偶/朋友/兄弟姊妹...
        return f"{viewer} 與 {target} 為 {relation_type}"


def get_member_relations(name):
    """回傳該會員的所有關係(含反向推導)。
    每筆: {target, relation, relation_id, note, reverse, desc}
    """
    df = load_relations()
    if df.empty:
        return []
    results = []
    for _, r in df.iterrows():
        a = str(r.get('member_a', ''))
        b = str(r.get('member_b', ''))
        rt = str(r.get('relation_type', ''))
        rid = str(r.get('relation_id', ''))
        note = str(r.get('note', ''))
        if a == name:
            results.append({
                'target': b, 'relation': rt, 'relation_id': rid,
                'note': note, 'reverse': False,
                'desc': describe_relation(name, b, rt, viewer_is_a=True),
            })
        elif b == name:
            rev = RELATION_REVERSE.get(rt, rt)
            results.append({
                'target': a, 'relation': rev, 'relation_id': rid,
                'note': note, 'reverse': True,
                'desc': describe_relation(name, a, rt, viewer_is_a=False),
            })
    return results


def rename_member_in_relations(old_name, new_name):
    """會員改名時同步更新關係鏈中的名稱(選用)。"""
    ws = get_worksheet_for_write("MemberRelations")
    if not ws:
        return
    try:
        all_vals = ws.get_all_values()
        header = all_vals[0]
        a_idx = header.index("member_a")
        b_idx = header.index("member_b")
        for i, row in enumerate(all_vals[1:], 2):
            if len(row) > a_idx and str(row[a_idx]) == str(old_name):
                ws.update_cell(i, a_idx + 1, new_name)
            if len(row) > b_idx and str(row[b_idx]) == str(old_name):
                ws.update_cell(i, b_idx + 1, new_name)
        clear_cache()
    except Exception:
        pass


# ==========================================
# 3.7 工資計算核心功能 (與 wage-app 整合)
# ==========================================
# 設計目標:
#   - 出貨/訂單 → 自動產生工資紀錄
#   - 與獨立的 wage-app 共用同一份 catalog 與 entries 資料模型
#   - 全部存在 Google Sheet (WageCatalog / WageEntries / WageSettlements)
#   - 員工統一使用 KEYERS 名單,姓名作為對接鍵

# 預設工資對照表 - 來源: wage-app/catalog-data.js
DEFAULT_WAGE_CATALOG = [
    # (product_name, wage_make, wage_pack, wage_ship, wage_svc)
    ("光之鹽語 - 光之鹽語禮盒", 52, 6, 10, 0),
    ("艾草包10入", 0, 10, 10, 0),
    ("艾草包5入", 0, 5, 10, 0),
    ("脈輪淨化蠟燭組 - 9入", 45, 4.5, 10, 0),
    ("光之鹽語 - 單購魔法鹽", 24, 4, 10, 0),
    ("大淨化包", 24, 3, 10, 0),
    ("大淨化包｜三日快速顯化儀式 - 代點顯化蠟燭", 24, 3, 0, 250),
    ("顯化蠟燭2入", 10, 1, 10, 0),
    ("顯化蠟燭｜代點服務", 10, 1, 0, 200),
    ("2026 馬上成功・人財貴圓滿組", 48, 6, 10, 0),
    ("28天脈輪能量日常守護組", 152, 16, 10, 0),
    ("數字水晶手鍊(細)", 50, 0, 10, 0),
    ("數字水晶手鍊(粗)", 100, 0, 10, 0),
    ("生命數字能量項鍊(鈦鋼), 項鍊整組", 200, 0, 10, 0),
    ("銅鑼浴", 0, 0, 0, 450),
    ("生命靈數解盤服務", 0, 0, 0, 2520),
    ("【清明節氣祈福組 】- 家族能量清理與內在小孩療癒 - 老師代點", 24, 3, 0, 250),
]

WAGE_STAGE_LABELS = {"make": "製造", "pack": "包裝", "ship": "出貨", "svc": "服務費"}


def ensure_wage_sheets():
    """確保 WageCatalog / WageEntries / WageSettlements 工作表存在,並 seed 預設資料。"""
    if st.session_state.get('_wage_sheets_ok'):
        return
    sh = get_spreadsheet()
    if not sh:
        return
    try:
        existing = [ws.title for ws in sh.worksheets()]
        if "WageCatalog" not in existing:
            ws = sh.add_worksheet(title="WageCatalog", rows=300, cols=6)
            ws.append_row(["product_name", "wage_make", "wage_pack",
                           "wage_ship", "wage_svc", "note"])
            for it in DEFAULT_WAGE_CATALOG:
                ws.append_row([it[0], float(it[1]), float(it[2]),
                               float(it[3]), float(it[4]), ""])
        if "WageEntries" not in existing:
            ws = sh.add_worksheet(title="WageEntries", rows=5000, cols=14)
            ws.append_row(["entry_id", "date", "employee_name", "category",
                           "stage", "item_name", "qty", "price", "amount",
                           "note", "order_no", "created_by", "created_at",
                           "settled"])
        if "WageSettlements" not in existing:
            ws = sh.add_worksheet(title="WageSettlements", rows=200, cols=4)
            ws.append_row(["year_month", "settled_at", "total", "settled_by"])
        st.session_state['_wage_sheets_ok'] = True
    except Exception:
        pass


@st.cache_data(ttl=60)
def load_wage_catalog():
    ws = get_worksheet("WageCatalog")
    if not ws:
        return pd.DataFrame()
    try:
        df = pd.DataFrame(ws.get_all_records())
        for col in ['wage_make', 'wage_pack', 'wage_ship', 'wage_svc']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        return df
    except Exception:
        return pd.DataFrame()


def is_proxy_product(product_name):
    """代點服務 → 不計出貨工資"""
    return '代點' in str(product_name or '')


def match_wage_catalog(product_name):
    """依商品名稱比對工資對照表,回傳 dict 或 None。
    比對順序: 完全相等 → 前綴(以 ' - ' 切分) → 雙向 substring。
    """
    df = load_wage_catalog()
    if df.empty:
        return None
    name = str(product_name or '').strip()
    if not name:
        return None
    names = df['product_name'].astype(str)
    # 1. exact
    exact = df[names == name]
    if not exact.empty:
        return exact.iloc[0].to_dict()
    # 2. 以分隔符切分 baseName 後比對
    base = name.split(' - ')[0].split('｜')[0].strip()
    if base and base != name:
        bm = df[names.str.startswith(base) | (names == base)]
        if not bm.empty:
            return bm.iloc[0].to_dict()
    # 3. substring 雙向
    sub = df[names.apply(lambda x: (x and (x in name or name in x)))]
    if not sub.empty:
        return sub.iloc[0].to_dict()
    return None


def generate_wage_id():
    return f"W-{int(time.time() * 1000)}"


def add_wage_entry(entry):
    """寫入一筆工資紀錄。entry 為 dict。"""
    ensure_wage_sheets()
    ws = get_worksheet_for_write("WageEntries")
    if not ws:
        return False
    try:
        ws.append_row([
            entry.get('entry_id') or generate_wage_id(),
            entry.get('date', str(date.today())),
            entry.get('employee_name', ''),
            entry.get('category', '產品'),
            entry.get('stage', '') or '',
            entry.get('item_name', ''),
            float(entry.get('qty', 0) or 0),
            float(entry.get('price', 0) or 0),
            float(entry.get('amount', 0) or 0),
            entry.get('note', '') or '',
            entry.get('order_no', '') or '',
            entry.get('created_by', '') or '',
            entry.get('created_at') or str(datetime.now()),
            entry.get('settled', 'N') or 'N',
        ])
        clear_cache()
        return True
    except Exception:
        return False


def create_wage_entries_for_items(items_list, stage_keyers, order_no, created_by):
    """
    依「項目清單 + 各階段員工」,參照 WageCatalog 產生工資紀錄。

    items_list: [{'product_name': str, 'qty': float, ...}, ...]
    stage_keyers: {'make': name|'', 'pack': name|'', 'ship': name|'', 'svc': name|''}
    回傳 (created_count, skipped_count, messages)
    """
    ensure_wage_sheets()
    created = 0
    skipped = 0
    messages = []
    today = str(date.today())
    now = str(datetime.now())

    for item in items_list:
        pname = (item.get('product_name')
                 or item.get('name', '') or '')
        qty = float(item.get('qty', 0) or 0)
        if qty <= 0 or not pname:
            continue
        catalog = match_wage_catalog(pname)
        if catalog is None:
            skipped += 1
            messages.append(f"⚠ 商品「{pname}」未對應工資設定")
            continue
        is_proxy = is_proxy_product(pname) or is_proxy_product(catalog.get('product_name', ''))
        stages = [
            ('make', '製造', float(catalog.get('wage_make', 0) or 0)),
            ('pack', '包裝', float(catalog.get('wage_pack', 0) or 0)),
            ('ship', '出貨', 0.0 if is_proxy else float(catalog.get('wage_ship', 0) or 0)),
            ('svc',  '服務費', float(catalog.get('wage_svc', 0) or 0)),
        ]
        for key, stage_name, unit_price in stages:
            emp = stage_keyers.get(key, '')
            if not emp or unit_price <= 0:
                continue
            amount = unit_price * qty
            ok = add_wage_entry({
                'entry_id': generate_wage_id() + f"-{key}",
                'date': today,
                'employee_name': emp,
                'category': '產品',
                'stage': stage_name,
                'item_name': catalog.get('product_name', pname),
                'qty': qty,
                'price': unit_price,
                'amount': amount,
                'note': f"訂單 {order_no}" if order_no else "",
                'order_no': order_no or '',
                'created_by': created_by or '',
                'created_at': now,
                'settled': 'N',
            })
            if ok:
                created += 1
    return created, skipped, messages


@st.cache_data(ttl=30)
def load_wage_entries():
    ws = get_worksheet("WageEntries")
    if not ws:
        return pd.DataFrame()
    try:
        df = pd.DataFrame(ws.get_all_records())
        for col in ['qty', 'price', 'amount']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        return df
    except Exception:
        return pd.DataFrame()


def delete_wage_entry(entry_id):
    ws = get_worksheet_for_write("WageEntries")
    if not ws:
        return False
    try:
        all_vals = ws.get_all_values()
        if not all_vals:
            return False
        header = all_vals[0]
        if 'entry_id' not in header:
            return False
        idx = header.index('entry_id')
        rows_to_del = []
        for i, row in enumerate(all_vals[1:], 2):
            if len(row) > idx and str(row[idx]) == str(entry_id):
                rows_to_del.append(i)
        for r in sorted(rows_to_del, reverse=True):
            ws.delete_rows(r)
        clear_cache()
        return len(rows_to_del) > 0
    except Exception:
        return False


def delete_wage_entries_by_order(order_no):
    """訂單取消/刪除時,連同其工資紀錄一起移除。"""
    ws = get_worksheet_for_write("WageEntries")
    if not ws:
        return 0
    try:
        all_vals = ws.get_all_values()
        if not all_vals:
            return 0
        header = all_vals[0]
        if 'order_no' not in header:
            return 0
        ono_idx = header.index('order_no')
        rows = [i for i, row in enumerate(all_vals[1:], 2)
                if len(row) > ono_idx and str(row[ono_idx]) == str(order_no)]
        for r in sorted(rows, reverse=True):
            ws.delete_rows(r)
        clear_cache()
        return len(rows)
    except Exception:
        return 0


def upsert_wage_catalog(product_name, wage_make, wage_pack, wage_ship, wage_svc, note=""):
    ensure_wage_sheets()
    ws = get_worksheet_for_write("WageCatalog")
    if not ws:
        return False
    try:
        all_vals = ws.get_all_values()
        header = all_vals[0]
        name_idx = header.index('product_name')
        for i, row in enumerate(all_vals[1:], 2):
            if len(row) > name_idx and str(row[name_idx]) == str(product_name):
                ws.update(f'A{i}:F{i}', [[product_name,
                                          float(wage_make), float(wage_pack),
                                          float(wage_ship), float(wage_svc),
                                          note]])
                clear_cache()
                return True
        ws.append_row([product_name, float(wage_make), float(wage_pack),
                       float(wage_ship), float(wage_svc), note])
        clear_cache()
        return True
    except Exception:
        return False


def delete_wage_catalog_item(product_name):
    ws = get_worksheet_for_write("WageCatalog")
    if not ws:
        return False
    try:
        all_vals = ws.get_all_values()
        header = all_vals[0]
        name_idx = header.index('product_name')
        rows_to_del = []
        for i, row in enumerate(all_vals[1:], 2):
            if len(row) > name_idx and str(row[name_idx]) == str(product_name):
                rows_to_del.append(i)
        for r in sorted(rows_to_del, reverse=True):
            ws.delete_rows(r)
        clear_cache()
        return len(rows_to_del) > 0
    except Exception:
        return False


def load_wage_settlements():
    ws = get_worksheet("WageSettlements")
    if not ws:
        return pd.DataFrame()
    try:
        return pd.DataFrame(ws.get_all_records())
    except Exception:
        return pd.DataFrame()


def mark_entries_settled(year_month):
    """將指定月份的所有工資紀錄 settled 標為 Y。"""
    ws = get_worksheet_for_write("WageEntries")
    if not ws:
        return
    try:
        all_vals = ws.get_all_values()
        if not all_vals:
            return
        header = all_vals[0]
        if 'date' not in header or 'settled' not in header:
            return
        date_idx = header.index('date')
        settled_idx = header.index('settled')
        for i, row in enumerate(all_vals[1:], 2):
            if len(row) > date_idx and str(row[date_idx]).startswith(year_month):
                ws.update_cell(i, settled_idx + 1, 'Y')
    except Exception:
        pass


def mark_month_settled(year_month, total, settled_by):
    ensure_wage_sheets()
    ws = get_worksheet_for_write("WageSettlements")
    if not ws:
        return False
    try:
        all_vals = ws.get_all_values()
        if all_vals and len(all_vals) > 0:
            header = all_vals[0]
            ym_idx = header.index('year_month')
            for i, row in enumerate(all_vals[1:], 2):
                if len(row) > ym_idx and str(row[ym_idx]) == str(year_month):
                    ws.update(f'A{i}:D{i}', [[year_month, str(datetime.now()),
                                              float(total), settled_by]])
                    mark_entries_settled(year_month)
                    clear_cache()
                    return True
        ws.append_row([year_month, str(datetime.now()), float(total), settled_by])
        mark_entries_settled(year_month)
        clear_cache()
        return True
    except Exception:
        return False


def build_wage_app_json(df_entries=None):
    """將 WageEntries 轉成 wage-app 相容的 JSON 結構,可直接匯入 wage-app。"""
    if df_entries is None:
        df_entries = load_wage_entries()
    # employees
    emp_names = []
    if not df_entries.empty:
        emp_names = sorted(set(str(n) for n in df_entries['employee_name'].tolist() if n))
    # 補上 KEYERS 確保所有員工都在
    for k in KEYERS:
        if k not in emp_names:
            emp_names.append(k)
    employees = []
    name_to_id = {}
    for n in emp_names:
        if not n or n.lower() == 'nan':
            continue
        eid = f"e_{abs(hash(n)) % 10**10}"
        employees.append({"id": eid, "name": n, "multProd": 1})
        name_to_id[n] = eid
    # entries
    entries = []
    if not df_entries.empty:
        for _, row in df_entries.iterrows():
            stage_val = row.get('stage', '')
            entries.append({
                "id": str(row.get('entry_id', '')),
                "date": str(row.get('date', '')),
                "employeeId": name_to_id.get(str(row.get('employee_name', '')), ''),
                "category": str(row.get('category', '產品')),
                "stage": str(stage_val) if stage_val else None,
                "item": str(row.get('item_name', '')),
                "qty": float(row.get('qty', 0) or 0),
                "price": float(row.get('price', 0) or 0),
                "amount": float(row.get('amount', 0) or 0),
                "note": str(row.get('note', '')),
                "createdBy": str(row.get('created_by', '')),
                "createdAt": str(row.get('created_at', '')),
            })
    # settlements
    settlements = {}
    df_s = load_wage_settlements()
    if not df_s.empty:
        for _, row in df_s.iterrows():
            settlements[str(row.get('year_month', ''))] = {
                "settledAt": str(row.get('settled_at', '')),
                "total": float(row.get('total', 0) or 0),
            }
    # catalog
    cat_df = load_wage_catalog()
    products = []
    if not cat_df.empty:
        for _, r in cat_df.iterrows():
            products.append({
                "name": str(r.get('product_name', '')),
                "wageMake": float(r.get('wage_make', 0) or 0),
                "wagePack": float(r.get('wage_pack', 0) or 0),
                "wageShip": float(r.get('wage_ship', 0) or 0),
                "wageSvc": float(r.get('wage_svc', 0) or 0),
            })
    return {
        "employees": employees,
        "entries": entries,
        "settlements": settlements,
        "catalog": {"products": products},
        "settings": {"reminderDay": 5, "localUser": "", "lastBackupAt": None}
    }


# ==========================================
# 3.8 歷史紀錄顯示
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
ensure_wage_sheets()

with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["🛒 訂單管理", "👥 會員管理", "🔨 製造作業",
                             "🚚 出貨作業", "📦 商品管理", "📥 進貨作業",
                             "📦 移庫作業", "💰 工資計算", "📊 報表查詢"])
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
    st.subheader("🚚 銷售出貨")
    ensure_wage_sheets()

    # === 出貨模式選擇 ===
    ship_mode = st.radio("出貨方式",
                          ["📋 從訂單出貨 (推薦,自動帶出商品)",
                           "✋ 手動挑選商品"],
                          horizontal=True, key="ship_mode_radio")
    st.divider()

    # =====================================================
    # 📋 模式 A: 從訂單出貨
    # =====================================================
    if ship_mode.startswith("📋"):
        df_orders = load_orders()
        pending_statuses = ["已確認", "未付款/未出貨", "已付款/未出貨",
                            "處理中", "已成立", "待處理"]
        if df_orders.empty:
            st.info("目前還沒有任何訂單。請先到「🛒 訂單管理 → 📝 新增訂單」建立訂單。")
        else:
            pending = df_orders[df_orders['status'].isin(pending_statuses)].copy()
            if pending.empty:
                st.success("目前沒有待出貨的訂單,所有訂單都已出貨或已完成。")
            else:
                # 排序: 最新訂單在最上面
                pending = pending.sort_values('created_at', ascending=False) \
                            if 'created_at' in pending.columns else pending
                order_labels = []
                for _, r in pending.iterrows():
                    status_icon = ORDER_STATUS_COLORS.get(str(r.get('status', '')), '⚪')
                    order_labels.append(
                        f"{r['order_no']} | {status_icon} {r.get('status','')} | "
                        f"{r.get('customer_name','')} | ${float(r.get('total_amount',0)):,.0f}"
                    )
                sel_order = st.selectbox(
                    f"📋 選擇要出貨的訂單 (共 {len(pending)} 張待出貨)",
                    order_labels, key="ship_order_sel"
                )
                sel_ono = sel_order.split(" | ")[0].strip()
                row = pending[pending['order_no'].astype(str) == sel_ono].iloc[0]
                items = load_order_items(sel_ono)

                # === 訂單資訊摘要 ===
                ic1, ic2, ic3, ic4 = st.columns(4)
                ic1.metric("客戶", str(row.get('customer_name', '') or '—'))
                ic2.metric("電話", str(row.get('customer_phone', '') or '—'))
                ic3.metric("應付金額", f"${float(row.get('total_amount', 0)):,.0f}")
                ic4.metric("狀態", str(row.get('status', '')))
                addr = str(row.get('shipping_address', '') or '')
                if addr:
                    st.caption(f"📍 寄送地址: {addr}")
                ord_note = str(row.get('note', '') or '')
                if ord_note:
                    st.caption(f"📝 訂單備註: {ord_note}")

                if items.empty:
                    st.warning("⚠ 此訂單沒有任何品項,無法出貨。請至「🛒 訂單管理」補加品項。")
                else:
                    # === 自動帶出的品項 ===
                    st.markdown(f"##### 📦 訂單品項 (共 {len(items)} 項,將全部出貨)")
                    items_disp = items[['product_name', 'sku', 'warehouse', 'qty',
                                        'unit_price', 'subtotal']].rename(columns={
                        'product_name': '品名', 'sku': '貨號',
                        'warehouse': '出貨倉庫', 'qty': '數量',
                        'unit_price': '單價', 'subtotal': '小計'
                    })
                    st.dataframe(items_disp, use_container_width=True, hide_index=True)

                    # === 出貨資訊 ===
                    st.markdown("##### 🚚 出貨資訊")
                    sc1, sc2 = st.columns(2)
                    so_method = sc1.selectbox("寄送方式", SHIPPING_METHODS,
                                              key=f"so_sm_{sel_ono}")
                    so_no = sc2.text_input("配送號碼 / 物流單號",
                                           key=f"so_sn_{sel_ono}")
                    sc3, sc4 = st.columns(2)
                    so_keyer = sc3.selectbox("出貨經手人", KEYERS, index=3,
                                             key=f"so_su_{sel_ono}")
                    so_target = sc4.selectbox(
                        "出貨後訂單狀態",
                        ["未付款/已出貨", "已完成"],
                        index=(1 if str(row.get('status', '')) == "已付款/未出貨" else 0),
                        key=f"so_ts_{sel_ono}"
                    )

                    # === 工資階段員工指派 ===
                    st.markdown("##### 💰 工資階段員工指派(各階段留「—」= 不計薪)")
                    keyer_opts = ["—"] + KEYERS
                    default_ship_idx = (KEYERS.index(so_keyer) + 1) if so_keyer in KEYERS else 0
                    sk1, sk2, sk3, sk4 = st.columns(4)
                    so_skm = sk1.selectbox("製造", keyer_opts, key=f"so_skm_{sel_ono}")
                    so_skp = sk2.selectbox("包裝", keyer_opts, key=f"so_skp_{sel_ono}")
                    so_sks = sk3.selectbox("出貨", keyer_opts, index=default_ship_idx,
                                           key=f"so_sks_{sel_ono}")
                    so_skv = sk4.selectbox("服務費", keyer_opts, key=f"so_skv_{sel_ono}")
                    so_stage_keyers = {
                        'make': so_skm if so_skm != '—' else '',
                        'pack': so_skp if so_skp != '—' else '',
                        'ship': so_sks if so_sks != '—' else '',
                        'svc':  so_skv if so_skv != '—' else '',
                    }

                    # === 工資預覽 ===
                    if any(so_stage_keyers.values()):
                        preview, unmatched, total_wage = [], [], 0.0
                        for _, it in items.iterrows():
                            pn = str(it.get('product_name', ''))
                            cat = match_wage_catalog(pn)
                            if not cat:
                                unmatched.append(pn)
                                continue
                            is_p = is_proxy_product(pn) or is_proxy_product(cat.get('product_name', ''))
                            q = float(it['qty'])
                            m = float(cat.get('wage_make', 0)) * q if so_stage_keyers['make'] else 0
                            p = float(cat.get('wage_pack', 0)) * q if so_stage_keyers['pack'] else 0
                            s = (0 if is_p else float(cat.get('wage_ship', 0)) * q) if so_stage_keyers['ship'] else 0
                            v = float(cat.get('wage_svc',  0)) * q if so_stage_keyers['svc']  else 0
                            row_total = m + p + s + v
                            total_wage += row_total
                            preview.append({
                                '品名': pn, '數量': q,
                                f"製造({so_stage_keyers['make'] or '—'})": m,
                                f"包裝({so_stage_keyers['pack'] or '—'})": p,
                                f"出貨({so_stage_keyers['ship'] or '—'})": s,
                                f"服務費({so_stage_keyers['svc'] or '—'})": v,
                                '小計': row_total,
                            })
                        with st.expander(f"💰 工資預覽 (合計 NT$ {total_wage:,.0f})",
                                         expanded=True):
                            if preview:
                                st.dataframe(pd.DataFrame(preview),
                                             use_container_width=True, hide_index=True)
                            if unmatched:
                                st.warning("以下商品未對應工資設定,將不會產生工資:\n" +
                                           "\n".join(f"- {n}" for n in unmatched))
                                st.caption("可至「💰 工資計算 → 📚 工資對照表」新增對應")
                    else:
                        st.info("ℹ 目前所有階段員工皆為「—」,本次出貨將不會產生工資紀錄")

                    # === 確認出貨 ===
                    if st.button(f"🚚 確認出貨「{sel_ono}」並產生工資",
                                  type="primary", use_container_width=True,
                                  key=f"so_btn_{sel_ono}"):
                        ok, msg = ship_order(
                            sel_ono, so_keyer, so_method, so_no, so_target,
                            stage_keyers=so_stage_keyers, created_by=so_keyer
                        )
                        if ok:
                            st.success(msg)
                            time.sleep(1.5)
                            st.rerun()
                        else:
                            st.error(msg)

    # =====================================================
    # ✋ 模式 B: 手動挑選商品 (保留原邏輯)
    # =====================================================
    else:
        if 'out_list' not in st.session_state:
            st.session_state['out_list'] = []
        col_a, col_b, col_c = st.columns(3)
        ship_opt = col_a.selectbox("寄送方式", SHIPPING_METHODS + ["手動輸入..."])
        final_ship = col_a.text_input("自訂方式") if ship_opt == "手動輸入..." else ship_opt
        ship_no = col_b.text_input("配送號碼")
        user = col_c.selectbox("經手人", KEYERS, index=3)
        order_id = st.text_input("訂單編號 / 備註")

        st.markdown("##### 💰 工資階段員工指派(各階段留「—」= 不計薪)")
        sk1, sk2, sk3, sk4 = st.columns(4)
        keyer_opts = ["—"] + KEYERS
        default_ship_idx = (KEYERS.index(user) + 1) if user in KEYERS else 0
        skey_make = sk1.selectbox("製造", keyer_opts, index=0, key="m_ship_skey_make")
        skey_pack = sk2.selectbox("包裝", keyer_opts, index=0, key="m_ship_skey_pack")
        skey_ship = sk3.selectbox("出貨", keyer_opts, index=default_ship_idx, key="m_ship_skey_ship")
        skey_svc  = sk4.selectbox("服務費", keyer_opts, index=0, key="m_ship_skey_svc")

        st.divider()
        prods = get_formatted_product_df()
        if not prods.empty:
            col1, col2, col3 = st.columns([3, 1, 1])
            sel_p = col1.selectbox("挑選商品", prods['label'])
            wh = col2.selectbox("倉庫", WAREHOUSES, index=3)
            qty = col3.number_input("數量", 1.0)
            if st.button("加入待出貨清單"):
                st.session_state['out_list'].append({
                    'sku': sel_p.split(" | ")[0],
                    'name': sel_p.split(" | ")[1],
                    'wh': wh, 'qty': qty
                })
                st.rerun()

        if st.session_state['out_list']:
            for i, item in enumerate(st.session_state['out_list']):
                c_l, c_d = st.columns([5, 1])
                c_l.write(f"**{item['name']}** - {item['wh']} x{item['qty']}")
                if c_d.button("移除", key=f"rm_o_{i}"):
                    st.session_state['out_list'].pop(i)
                    st.rerun()

            stage_keyers = {
                'make': skey_make if skey_make != '—' else '',
                'pack': skey_pack if skey_pack != '—' else '',
                'ship': skey_ship if skey_ship != '—' else '',
                'svc':  skey_svc  if skey_svc  != '—' else '',
            }
            if any(stage_keyers.values()):
                preview, unmatched, total_wage = [], [], 0.0
                for x in st.session_state['out_list']:
                    cat = match_wage_catalog(x['name'])
                    if not cat:
                        unmatched.append(x['name'])
                        continue
                    is_p = is_proxy_product(x['name']) or is_proxy_product(cat.get('product_name', ''))
                    m = (float(cat.get('wage_make', 0)) * x['qty']) if stage_keyers['make'] else 0
                    p = (float(cat.get('wage_pack', 0)) * x['qty']) if stage_keyers['pack'] else 0
                    s = (0 if is_p else float(cat.get('wage_ship', 0)) * x['qty']) if stage_keyers['ship'] else 0
                    v = (float(cat.get('wage_svc',  0)) * x['qty']) if stage_keyers['svc']  else 0
                    row_total = m + p + s + v
                    total_wage += row_total
                    preview.append({
                        '商品': x['name'], '數量': x['qty'],
                        f"製造({stage_keyers['make'] or '—'})": m,
                        f"包裝({stage_keyers['pack'] or '—'})": p,
                        f"出貨({stage_keyers['ship'] or '—'})": s,
                        f"服務費({stage_keyers['svc'] or '—'})": v,
                        '小計': row_total,
                    })
                with st.expander(f"💰 工資預覽 (合計 NT$ {total_wage:,.0f})", expanded=True):
                    if preview:
                        st.dataframe(pd.DataFrame(preview),
                                     use_container_width=True, hide_index=True)
                    if unmatched:
                        st.warning("以下商品未對應工資設定,將不會產生工資紀錄:\n" +
                                   "\n".join(f"- {n}" for n in unmatched))
                        st.caption("可至「💰 工資計算 → 📚 工資對照表」新增對應")
            else:
                st.info("ℹ 目前所有階段員工皆為「—」,本次出貨將不會產生工資紀錄")

            if st.button("確認出貨", type="primary", use_container_width=True):
                ok_all = True
                for x in st.session_state['out_list']:
                    if not add_transaction("銷售出貨", date.today(), x['sku'], x['wh'],
                                            x['qty'], user, order_id, final_ship, ship_no):
                        ok_all = False
                wage_msg = ""
                if ok_all and any(stage_keyers.values()):
                    items_for_wage = [{'product_name': x['name'], 'qty': x['qty']}
                                      for x in st.session_state['out_list']]
                    created, skipped, _ = create_wage_entries_for_items(
                        items_for_wage, stage_keyers, order_id or "", user)
                    if created > 0:
                        wage_msg = f",已產生 {created} 筆工資紀錄"
                    if skipped > 0:
                        wage_msg += f"({skipped} 筆未對應)"
                st.session_state['out_list'] = []
                st.success(f"出貨完成{wage_msg}")
                time.sleep(1)
                st.rerun()

    st.divider()
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

                            need_ship = (status in ["已確認", "已成立", "待處理",
                                                     "未付款/未出貨", "處理中",
                                                     "已付款/未出貨"]
                                         and new_st in ["未付款/已出貨", "已完成"])
                            stage_keyers = {}
                            ship_user = None
                            s_method = ""
                            s_no = ""
                            if need_ship:
                                sc1, sc2, sc3 = st.columns(3)
                                ship_user = sc1.selectbox("出貨經手人", KEYERS, key=f"{kp}_su_{ono}")
                                s_method = sc2.selectbox("寄送方式", SHIPPING_METHODS, key=f"{kp}_sm_{ono}")
                                s_no = sc3.text_input("配送號碼", key=f"{kp}_sn_{ono}")

                                # === 工資階段員工指派 ===
                                st.markdown("**💰 工資階段員工指派(留「—」= 不計薪)**")
                                sk1, sk2, sk3, sk4 = st.columns(4)
                                keyer_opts = ["—"] + KEYERS
                                default_ship_idx = (KEYERS.index(ship_user) + 1) if ship_user in KEYERS else 0
                                sk_make = sk1.selectbox("製造", keyer_opts, key=f"{kp}_skm_{ono}")
                                sk_pack = sk2.selectbox("包裝", keyer_opts, key=f"{kp}_skp_{ono}")
                                sk_ship = sk3.selectbox("出貨", keyer_opts, index=default_ship_idx, key=f"{kp}_sks_{ono}")
                                sk_svc  = sk4.selectbox("服務費", keyer_opts, key=f"{kp}_skv_{ono}")
                                stage_keyers = {
                                    'make': sk_make if sk_make != '—' else '',
                                    'pack': sk_pack if sk_pack != '—' else '',
                                    'ship': sk_ship if sk_ship != '—' else '',
                                    'svc':  sk_svc  if sk_svc  != '—' else '',
                                }

                                # 工資預覽
                                if any(stage_keyers.values()) and not items.empty:
                                    preview = []
                                    unmatched = []
                                    total_wage = 0.0
                                    for _, it in items.iterrows():
                                        pn = str(it.get('product_name', ''))
                                        cat = match_wage_catalog(pn)
                                        if not cat:
                                            unmatched.append(pn)
                                            continue
                                        is_p = is_proxy_product(pn) or is_proxy_product(cat.get('product_name', ''))
                                        q = float(it['qty'])
                                        m = (float(cat.get('wage_make', 0)) * q) if stage_keyers['make'] else 0
                                        p_ = (float(cat.get('wage_pack', 0)) * q) if stage_keyers['pack'] else 0
                                        s_ = (0 if is_p else float(cat.get('wage_ship', 0)) * q) if stage_keyers['ship'] else 0
                                        v = (float(cat.get('wage_svc',  0)) * q) if stage_keyers['svc']  else 0
                                        row_total = m + p_ + s_ + v
                                        total_wage += row_total
                                        preview.append({
                                            '商品': pn, '數量': q,
                                            '製造': m, '包裝': p_, '出貨': s_, '服務費': v,
                                            '小計': row_total,
                                        })
                                    with st.expander(f"💰 工資預覽 (合計 NT$ {total_wage:,.0f})"):
                                        if preview:
                                            st.dataframe(pd.DataFrame(preview),
                                                         use_container_width=True, hide_index=True)
                                        if unmatched:
                                            st.warning("未對應工資設定: " + ", ".join(unmatched))

                            ac1, ac2 = st.columns([3, 1])
                            btn_label = "🚚 確認出貨" if need_ship else "✅ 確認變更"
                            if ac1.button(btn_label, key=f"{kp}_apply_{ono}", type="primary"):
                                if need_ship:
                                    ok, msg = ship_order(ono, ship_user, s_method, s_no, new_st,
                                                          stage_keyers=stage_keyers,
                                                          created_by=ship_user)
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
    ensure_relations_sheet()
    tab_m_list, tab_m_add, tab_m_rel = st.tabs(
        ["📋 會員列表", "手動新增會員", "🔗 關係鏈"])

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
            list_cols = ['name', 'phone', 'email', 'birthday', 'birth_time',
                         'address', 'last_order_date']
            list_cols = [c for c in list_cols if c in filtered_m.columns]
            st.dataframe(
                filtered_m[list_cols].rename(
                    columns={'name': '姓名', 'phone': '電話', 'email': 'Email',
                             'birthday': '生日', 'birth_time': '出生時間',
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
                        em_c1, em_c2 = st.columns(2)
                        em_phone = em_c1.text_input("電話", value=str(m_data.get('phone', '')))
                        em_email = em_c2.text_input("Email", value=str(m_data.get('email', '')))
                        em_c3, em_c4 = st.columns(2)
                        em_birthday = em_c3.text_input("生日 (格式 2000/01/05)",
                                                       value=str(m_data.get('birthday', '') or ''),
                                                       placeholder="2000/01/05")
                        em_birthtime = em_c4.text_input("出生時間 (例: 14:30 或 不詳)",
                                                        value=str(m_data.get('birth_time', '') or ''),
                                                        placeholder="14:30")
                        em_addr = st.text_input("地址", value=str(m_data.get('address', '')))
                        if st.form_submit_button("儲存修改"):
                            save_member(sel_m, em_phone, em_email, em_addr,
                                        birthday=em_birthday, birth_time=em_birthtime)
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
            am_c3, am_c4 = st.columns(2)
            am_birthday = am_c3.text_input("生日 (格式 2000/01/05)", placeholder="2000/01/05")
            am_birthtime = am_c4.text_input("出生時間 (例: 14:30 或 不詳)", placeholder="14:30")
            am_addr = st.text_input("地址")
            am_note = st.text_input("備註")
            if st.form_submit_button("新增會員", use_container_width=True):
                if not am_name:
                    st.error("請填寫姓名")
                else:
                    existing = find_member_by_name(am_name)
                    if existing is not None:
                        st.warning(f"會員 '{am_name}' 已存在，將更新資料")
                    save_member(am_name, am_phone, am_email, am_addr, am_note,
                                birthday=am_birthday, birth_time=am_birthtime)
                    st.success(f"會員 '{am_name}' 已儲存")
                    time.sleep(1)
                    st.rerun()

    # === 🔗 關係鏈 ===
    with tab_m_rel:
        df_members = load_members()
        if df_members.empty or len(df_members) < 1:
            st.info("目前沒有會員。請先新增會員後再建立關係。")
        else:
            member_names = df_members['name'].astype(str).tolist()

            # --- 建立新關係 ---
            st.markdown("##### ➕ 建立關係")
            st.caption("親子類(父子/父女/母子/母女):**會員A 選長輩(父/母)、會員B 選晚輩(子/女)**。"
                       "其他對稱關係(配偶/朋友等)順序不拘。系統會自動推導反向關係。")
            with st.form("add_relation_form"):
                rc1, rc2, rc3 = st.columns([2, 1.2, 2])
                rel_a = rc1.selectbox("會員 A (親子類請選長輩)", member_names, key="rel_a")
                rel_type = rc2.selectbox("關係", RELATION_TYPES, key="rel_type")
                rel_b = rc3.selectbox("會員 B (親子類請選晚輩)", member_names,
                                      index=min(1, len(member_names) - 1), key="rel_b")
                rel_note = st.text_input("備註 (選填)", key="rel_note")
                if st.form_submit_button("建立關係", type="primary"):
                    ok, msg = add_relation(rel_a, rel_type, rel_b, rel_note)
                    if ok:
                        st.success(msg)
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.warning(msg)

            st.markdown("---")

            # --- 檢視某會員的關係網 ---
            st.markdown("##### 🔍 檢視會員關係")
            view_member = st.selectbox("選擇會員", member_names, key="rel_view")
            rels = get_member_relations(view_member)
            if not rels:
                st.info(f"「{view_member}」目前沒有建立任何關係。")
            else:
                st.markdown(f"**{view_member}** 共有 {len(rels)} 筆關係:")
                for rel in rels:
                    rc1, rc2, rc3 = st.columns([4, 2, 1])
                    arrow = "↩ (反向推導)" if rel['reverse'] else ""
                    rc1.write(f"{rel['desc']} {arrow}")
                    # 顯示對方生日(若有)
                    tgt = df_members[df_members['name'].astype(str) == rel['target']]
                    if not tgt.empty:
                        bd = str(tgt.iloc[0].get('birthday', '') or '')
                        bt = str(tgt.iloc[0].get('birth_time', '') or '')
                        info = " / ".join([x for x in [bd, bt] if x])
                        rc2.caption(f"🎂 {info}" if info else "—")
                    else:
                        rc2.caption("(非會員)")
                    if rc3.button("刪除", key=f"rel_del_{rel['relation_id']}"):
                        if delete_relation(rel['relation_id']):
                            st.success("已刪除關係")
                            time.sleep(0.8)
                            st.rerun()
                    if rel['note']:
                        st.caption(f"　　備註: {rel['note']}")

            # --- 全部關係總表 ---
            st.markdown("---")
            with st.expander("📋 所有關係總表"):
                df_rel = load_relations()
                if df_rel.empty:
                    st.caption("尚無任何關係")
                else:
                    disp = df_rel[['member_a', 'relation_type', 'member_b', 'note']].rename(
                        columns={'member_a': '會員 A', 'relation_type': '關係',
                                 'member_b': '會員 B', 'note': '備註'})
                    st.dataframe(disp, use_container_width=True, hide_index=True)

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

# --- 💰 工資計算 ---
elif page == "💰 工資計算":
    st.subheader("💰 工資計算系統 (與出貨/訂單連動)")
    ensure_wage_sheets()

    tab_summary, tab_entry, tab_list, tab_catalog, tab_export = st.tabs(
        ["📊 月度報表", "➕ 手動登錄", "📋 工資明細", "📚 工資對照表", "📤 匯出 / 同步"])

    # === 📊 月度報表 ===
    with tab_summary:
        ym_default = date.today().strftime("%Y-%m")
        ym = st.text_input("查詢月份 (YYYY-MM)", value=ym_default, key="w_sum_ym")
        df_w = load_wage_entries()
        df_settle = load_wage_settlements()
        if df_w.empty:
            st.info("尚無任何工資紀錄。請至「🚚 出貨作業」或「🛒 訂單管理」執行出貨即會自動產生,或在「➕ 手動登錄」新增。")
        else:
            df_w['date_str'] = df_w['date'].astype(str)
            month_df = df_w[df_w['date_str'].str.startswith(ym)].copy()
            if month_df.empty:
                st.info(f"{ym} 沒有工資紀錄")
            else:
                pivot = month_df.pivot_table(
                    index='employee_name', columns='stage', values='amount',
                    aggfunc='sum', fill_value=0
                )
                pivot['合計'] = pivot.sum(axis=1)
                pivot = pivot.reset_index().rename(columns={'employee_name': '員工'})
                st.dataframe(pivot, use_container_width=True, hide_index=True)
                grand_total = float(month_df['amount'].sum())
                cnt_orders = month_df['order_no'].astype(str).replace('', pd.NA).dropna().nunique()
                m1, m2, m3 = st.columns(3)
                m1.metric("全月總計", f"NT$ {grand_total:,.0f}")
                m2.metric("工資筆數", f"{len(month_df)}")
                m3.metric("涉及訂單數", f"{cnt_orders}")

                # 結算狀態
                st.markdown("---")
                settled_row = pd.DataFrame()
                if not df_settle.empty:
                    settled_row = df_settle[df_settle['year_month'].astype(str) == ym]
                if not settled_row.empty:
                    st.success(f"✓ 已於 {str(settled_row.iloc[0]['settled_at'])[:19]} 由 "
                               f"{settled_row.iloc[0].get('settled_by', '')} 結算,"
                               f"金額 NT$ {float(settled_row.iloc[0]['total']):,.0f}")
                else:
                    st.warning("此月份尚未結算")
                    cs1, cs2 = st.columns(2)
                    settled_by = cs1.selectbox("結算人", KEYERS, key="settle_by")
                    if cs2.button("📌 標記為已結算", type="primary"):
                        if mark_month_settled(ym, grand_total, settled_by):
                            st.success(f"已結算 {ym} (NT$ {grand_total:,.0f})")
                            time.sleep(1)
                            st.rerun()

    # === ➕ 手動登錄 ===
    with tab_entry:
        st.markdown("適用情境:非由出貨自動產生的工資,例如代點服務費、其他自訂項目。")
        with st.form("wage_entry_form"):
            we_c1, we_c2, we_c3 = st.columns(3)
            we_date = we_c1.date_input("日期", value=date.today())
            we_emp = we_c2.selectbox("員工", KEYERS)
            we_stage = we_c3.selectbox("階段 / 類別", ["製造", "包裝", "出貨", "服務費", "其他"])
            we_c4, we_c5 = st.columns(2)
            we_item = we_c4.text_input("項目名稱 *必填")
            we_qty = we_c5.number_input("數量", min_value=0.0, value=1.0, step=1.0)
            we_c6, we_c7 = st.columns(2)
            we_price = we_c6.number_input("單價", min_value=0.0, value=0.0, step=10.0)
            we_amount = we_c7.number_input("金額 (留 0 → 自動 = 數量×單價)", min_value=0.0, value=0.0, step=10.0)
            we_note = st.text_input("備註")
            if st.form_submit_button("新增工資紀錄", type="primary"):
                if not we_item.strip():
                    st.error("請輸入項目名稱")
                else:
                    amount = we_amount if we_amount > 0 else we_qty * we_price
                    ok = add_wage_entry({
                        'date': str(we_date),
                        'employee_name': we_emp,
                        'category': '產品' if we_stage != '其他' else '其他',
                        'stage': we_stage if we_stage != '其他' else '',
                        'item_name': we_item.strip(),
                        'qty': we_qty,
                        'price': we_price,
                        'amount': amount,
                        'note': we_note,
                        'order_no': '',
                        'created_by': we_emp,
                        'settled': 'N',
                    })
                    if ok:
                        st.success(f"已新增工資紀錄,金額 NT$ {amount:,.0f}")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("新增失敗,請檢查 Google Sheet 連線")

    # === 📋 工資明細 ===
    with tab_list:
        df_w = load_wage_entries()
        if df_w.empty:
            st.info("尚無紀錄")
        else:
            wlc1, wlc2, wlc3 = st.columns(3)
            ym2 = wlc1.text_input("月份篩選", value=date.today().strftime("%Y-%m"), key="w_list_ym")
            emp_filter = wlc2.selectbox("員工", ["全部"] + KEYERS, key="w_list_emp")
            stage_filter = wlc3.selectbox("階段", ["全部", "製造", "包裝", "出貨", "服務費"], key="w_list_stage")
            filtered = df_w.copy()
            if ym2:
                filtered = filtered[filtered['date'].astype(str).str.startswith(ym2)]
            if emp_filter != "全部":
                filtered = filtered[filtered['employee_name'].astype(str) == emp_filter]
            if stage_filter != "全部":
                filtered = filtered[filtered['stage'].astype(str) == stage_filter]
            filtered = filtered.sort_values('date', ascending=False)
            st.markdown(f"共 **{len(filtered)}** 筆,合計 **NT$ {float(filtered['amount'].sum()):,.0f}**")
            if filtered.empty:
                st.info("無符合資料")
            else:
                # 表頭
                hc = st.columns([1.4, 1, 1, 2.5, 0.7, 0.8, 1, 1.5, 0.6])
                for col, h in zip(hc, ["日期", "員工", "階段", "項目", "數量", "單價",
                                       "金額", "訂單/備註", " "]):
                    col.markdown(f"**{h}**")
                for idx, row in filtered.head(100).iterrows():
                    c1, c2, c3, c4, c5, c6, c7, c8, c9 = st.columns([1.4, 1, 1, 2.5, 0.7, 0.8, 1, 1.5, 0.6])
                    c1.text(str(row.get('date', '')))
                    c2.text(str(row.get('employee_name', '')))
                    c3.text(str(row.get('stage', '') or '—'))
                    item_name = str(row.get('item_name', ''))
                    c4.text(item_name[:18] + ('…' if len(item_name) > 18 else ''))
                    c5.text(f"{float(row.get('qty', 0)):g}")
                    c6.text(f"{float(row.get('price', 0)):,.0f}")
                    c7.text(f"${float(row.get('amount', 0)):,.0f}")
                    ono = str(row.get('order_no', ''))
                    note = str(row.get('note', ''))
                    c8.text((ono or note)[:14])
                    is_settled = str(row.get('settled', 'N')).upper() == 'Y'
                    if is_settled:
                        c9.text("🔒")
                    else:
                        eid = str(row.get('entry_id', ''))
                        if c9.button("🗑", key=f"wd_{eid}_{idx}"):
                            if delete_wage_entry(eid):
                                st.success("已刪除")
                                time.sleep(0.5)
                                st.rerun()
                if len(filtered) > 100:
                    st.caption(f"僅顯示前 100 筆,共 {len(filtered)} 筆。請用篩選縮小範圍。")

    # === 📚 工資對照表 ===
    with tab_catalog:
        st.markdown("##### 工資對照表 (產品 → 各階段單價)")
        st.caption("出貨時系統依此表計算每件商品的製造/包裝/出貨/服務費。代點商品(名稱含「代點」)出貨工資自動為 0。")
        df_cat = load_wage_catalog()
        if not df_cat.empty:
            display = df_cat.rename(columns={
                'product_name': '產品名稱', 'wage_make': '製造',
                'wage_pack': '包裝', 'wage_ship': '出貨',
                'wage_svc': '服務費', 'note': '備註'
            })
            st.dataframe(display, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("##### 新增 / 修改項目")
        with st.form("wage_cat_form"):
            wc_name = st.text_input("產品名稱(會用此名稱比對訂單品名,支援前綴/包含比對)")
            wcc1, wcc2, wcc3, wcc4 = st.columns(4)
            wc_make = wcc1.number_input("製造工資", min_value=0.0, value=0.0, step=1.0)
            wc_pack = wcc2.number_input("包裝工資", min_value=0.0, value=0.0, step=1.0)
            wc_ship = wcc3.number_input("出貨工資", min_value=0.0, value=0.0, step=1.0)
            wc_svc  = wcc4.number_input("服務費",   min_value=0.0, value=0.0, step=10.0)
            wc_note = st.text_input("備註")
            fc1, fc2 = st.columns(2)
            submit = fc1.form_submit_button("💾 新增 / 更新", type="primary")
            delete_it = fc2.form_submit_button("🗑️ 刪除此產品")
            if submit:
                if wc_name.strip():
                    if upsert_wage_catalog(wc_name.strip(), wc_make, wc_pack, wc_ship, wc_svc, wc_note):
                        st.success("已儲存")
                        time.sleep(1)
                        st.rerun()
                else:
                    st.error("請輸入產品名稱")
            if delete_it:
                if wc_name.strip():
                    if delete_wage_catalog_item(wc_name.strip()):
                        st.success("已刪除")
                        time.sleep(1)
                        st.rerun()

    # === 📤 匯出 / 同步 ===
    with tab_export:
        st.markdown("##### 匯出工資資料給 wage-app")
        st.caption("提供兩種格式: CSV(報表用) / wage-app JSON(可直接在 wage-app「資料備份 → 匯入 JSON 備份」載入)")
        ex_ym = st.text_input("匯出月份(留空 = 全部)",
                              value=date.today().strftime("%Y-%m"), key="ex_ym")
        df_w_all = load_wage_entries()
        if df_w_all.empty:
            st.info("尚無資料可匯出")
        else:
            export_df = df_w_all.copy()
            if ex_ym.strip():
                export_df = export_df[export_df['date'].astype(str).str.startswith(ex_ym.strip())]
            st.markdown(f"範圍內共 **{len(export_df)}** 筆,"
                        f"合計 **NT$ {float(export_df['amount'].sum()):,.0f}**")
            ec1, ec2 = st.columns(2)
            csv_data = export_df.to_csv(index=False).encode('utf-8-sig')
            ec1.download_button("📄 下載 CSV", csv_data,
                                file_name=f"wage_{ex_ym or 'all'}.csv",
                                mime='text/csv', use_container_width=True)
            import json as _json
            json_state = build_wage_app_json(export_df if ex_ym.strip() else df_w_all)
            ec2.download_button("📦 下載 wage-app JSON",
                                _json.dumps(json_state, ensure_ascii=False, indent=2).encode('utf-8'),
                                file_name=f"numbertalk_wage_backup_{ex_ym or 'all'}.json",
                                mime='application/json', use_container_width=True)

        st.markdown("---")
        st.markdown("##### Google Sheet 公開 CSV URL (給 wage-app 直接拉取)")
        st.caption("到 Google Sheet → 檔案 → 共用 → 「具有連結的任何人皆可檢視」,然後 wage-app 訂單匯入頁可貼下方 URL 直接抓資料。")
        sh = get_spreadsheet()
        if sh:
            try:
                sid = sh.id
                csv_url = f"https://docs.google.com/spreadsheets/d/{sid}/gviz/tq?tqx=out:csv&sheet=WageEntries"
                st.code(csv_url, language=None)
            except Exception:
                st.caption("(無法取得 Sheet ID)")

# --- 📊 報表查詢 ---
elif page == "📊 報表查詢":
    st.subheader("📊 庫存報表")
    df = get_stock_overview()
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        st.download_button("下載 CSV", df.to_csv(index=False).encode('utf-8-sig'), f"Stock_{date.today()}.csv", "text/csv")

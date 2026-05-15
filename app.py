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

ORDER_STATUSES = ["待處理", "處理中", "已出貨", "已完成", "已取消"]
ORDER_STATUS_COLORS = {
    "待處理": "🟡", "處理中": "🔵", "已出貨": "🟠", "已完成": "🟢", "已取消": "🔴"
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

def get_worksheet(sheet_name):
    client = get_client()
    try: return client.open(SPREADSHEET_NAME).worksheet(sheet_name)
    except: return None

@st.cache_data(ttl=5) 
def load_data(sheet_name):
    ws = get_worksheet(sheet_name)
    if ws is None: return pd.DataFrame()
    try:
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        # 確保基礎欄位存在
        for col in ['sku', 'name', 'category', 'series', 'spec', 'color', 'note']:
            if col not in df.columns: df[col] = ""
        return df.fillna("")
    except: return pd.DataFrame()

def clear_cache(): load_data.clear()

# ==========================================
# 3. 核心功能函式
# ==========================================

def get_formatted_product_df():
    df = load_data("Products")
    if df.empty: return df
    df['sku'] = df['sku'].astype(str)
    df['name'] = df['name'].astype(str)
    df['label'] = df['sku'] + " | " + df['name'] + " (" + df['spec'].astype(str) + " / " + df['color'].astype(str) + ")"
    return df

def update_stock_qty(sku, warehouse, delta_qty):
    ws = get_worksheet("Stock")
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

def add_transaction(doc_type, date_str, sku, wh, qty, user, note, ship_method="", ship_no=""):
    ws_hist = get_worksheet("History")
    # 自動抓取品名填入 K 欄
    df_p = load_data("Products")
    p_name = ""
    if not df_p.empty:
        match = df_p[df_p['sku'].astype(str) == str(sku)]
        if not match.empty: p_name = match.iloc[0]['name']

    prefix = {"進貨":"IN", "銷售出貨":"OUT", "製造領料":"MO", "製造入庫":"PD", "移庫(撥出)":"TR-O", "移庫(撥入)":"TR-I"}.get(doc_type, "ADJ")
    doc_no = f"{prefix}-{int(time.time())}"
    try:
        # A:type, B:no, C:date, D:sku, E:wh, F:qty, G:user, H:note, I:cost, J:time, K:product_name, L:ship_method, M:ship_no
        ws_hist.append_row([
            doc_type, doc_no, str(date_str), str(sku), wh, float(qty), 
            user, note, 0, str(datetime.now()), p_name, ship_method, ship_no
        ])
        factor = -1 if doc_type in ['銷售出貨', '製造領料', '移庫(撥出)'] else 1
        update_stock_qty(sku, wh, float(qty) * factor)
        clear_cache()
        return True
    except: return False

def delete_transaction(doc_no):
    ws_hist = get_worksheet("History")
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

def add_product(sku, name, category, series, spec, note, color):
    ws = get_worksheet("Products")
    try:
        ws.append_row([str(sku), series, category, name, spec, color, note])
        ws_stock = get_worksheet("Stock")
        if ws_stock:
            ws_stock.append_rows([[str(sku), wh, 0.0] for wh in WAREHOUSES])
        clear_cache()
        return True, "✅ 新增成功"
    except: return False, "連線錯誤"

def update_product(sku, new_data):
    ws = get_worksheet("Products")
    try:
        cell = ws.find(str(sku))
        row = cell.row
        if 'name' in new_data: ws.update_cell(row, 4, new_data['name'])
        if 'spec' in new_data: ws.update_cell(row, 5, new_data['spec'])
        if 'color' in new_data: ws.update_cell(row, 6, new_data['color'])
        if 'note' in new_data: ws.update_cell(row, 7, new_data['note'])
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
    target_cols = ['sku', 'series', 'category', 'name', 'spec', 'color', 'note', '總庫存'] + WAREHOUSES
    return result[[c for c in target_cols if c in result.columns]]

# ==========================================
# 3.5 訂單系統核心功能
# ==========================================

def ensure_order_sheets():
    client = get_client()
    if not client:
        return
    try:
        sh = client.open(SPREADSHEET_NAME)
        existing = [ws.title for ws in sh.worksheets()]
        if "Orders" not in existing:
            ws = sh.add_worksheet(title="Orders", rows=1000, cols=15)
            ws.append_row(["order_no", "order_date", "customer_name", "customer_phone",
                           "customer_email", "shipping_address", "status", "total_amount",
                           "note", "created_by", "created_at"])
        if "OrderItems" not in existing:
            ws = sh.add_worksheet(title="OrderItems", rows=5000, cols=8)
            ws.append_row(["order_no", "sku", "product_name", "qty", "unit_price",
                           "subtotal", "warehouse"])
    except Exception:
        pass

def generate_order_no():
    now = datetime.now()
    return f"ORD-{now.strftime('%Y%m%d')}-{int(time.time()) % 100000:05d}"

def create_order(order_no, order_date, customer_name, customer_phone,
                 customer_email, shipping_address, items, note, created_by):
    ensure_order_sheets()
    ws_orders = get_worksheet("Orders")
    ws_items = get_worksheet("OrderItems")
    if not ws_orders or not ws_items:
        return False, "無法連線到工作表"
    try:
        total = sum(item['subtotal'] for item in items)
        ws_orders.append_row([
            order_no, str(order_date), customer_name, customer_phone,
            customer_email, shipping_address, "待處理", float(total),
            note, created_by, str(datetime.now())
        ])
        for item in items:
            ws_items.append_row([
                order_no, item['sku'], item['product_name'],
                float(item['qty']), float(item['unit_price']),
                float(item['subtotal']), item['warehouse']
            ])
        clear_cache()
        return True, f"✅ 訂單 {order_no} 建立成功，總金額 ${total:,.0f}"
    except Exception as e:
        return False, f"❌ 建立失敗: {e}"

def load_orders():
    ensure_order_sheets()
    df = load_data("Orders")
    if df.empty:
        return pd.DataFrame(columns=["order_no", "order_date", "customer_name",
                                      "customer_phone", "customer_email",
                                      "shipping_address", "status", "total_amount",
                                      "note", "created_by", "created_at"])
    df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce').fillna(0)
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
    ws = get_worksheet("Orders")
    if not ws:
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
        return False
    except Exception:
        return False

def ship_order(order_no, keyer, ship_method="", ship_no=""):
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
    if update_order_status(order_no, "已出貨"):
        return True, "✅ 出貨完成，庫存已扣除"
    return False, "出貨紀錄已建立但狀態更新失敗"

def delete_order(order_no):
    ws_orders = get_worksheet("Orders")
    ws_items = get_worksheet("OrderItems")
    if not ws_orders or not ws_items:
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
    except Exception:
        return False

def render_history_table(doc_type_filter=None):
    st.markdown("#### 🕒 最近紀錄")
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
        # 優先抓取 History 存的品名，若無則查 Products
        d_name = row.get('product_name', sku_map.get(sku, '未知'))
        c3.text(f"{d_name}\n({sku})")
        c4.text(row.get('warehouse', ''))
        c5.text(row.get('qty', 0)); c6.text(row.get('user', '')); c7.text(row.get('note', ''))
        if c8.button("🗑️", key=f"del_{doc_no}_{idx}"):
            if delete_transaction(doc_no): st.rerun()
        st.divider()

# ==========================================
# 4. 主程式分頁
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="💎")
st.title(f"💎 {PAGE_TITLE}")

with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["📦 商品管理", "📦 移庫作業", "📥 進貨作業", "🚚 出貨作業", "🛒 訂單管理", "🔨 製造作業", "📊 報表查詢"])
    if st.button("🔄 強制刷新資料"):
        clear_cache()
        st.rerun()

# --- 📦 商品管理 ---
if page == "📦 商品管理":
    st.subheader("📦 商品資料維護")
    t1, t2 = st.tabs(["✨ 新增商品", "✏️ 修改商品"])
    with t1:
        current_df = load_data("Products")
        existing_cats = sorted(list(set(current_df['category'].tolist()))) if not current_df.empty else []
        cat_list = sorted(list(set(CATEGORIES + existing_cats)))
        c_cat, c_ser = st.columns(2)
        cat_opt = c_cat.selectbox("1. 分類", cat_list + ["➕ 手動輸入新分類..."])
        final_cat = c_cat.text_input("✍️ 新分類名稱") if cat_opt == "➕ 手動輸入新分類..." else cat_opt
        if cat_opt != "➕ 手動輸入新分類..." and not current_df.empty:
            filtered_sers = current_df[current_df['category'] == cat_opt]['series'].unique().tolist()
            final_ser_list = sorted(list(set(filtered_sers))) if filtered_sers else sorted(SERIES)
        else: final_ser_list = sorted(SERIES)
        ser_opt = c_ser.selectbox("2. 系列", final_ser_list + ["➕ 手動輸入新系列..."])
        final_ser = c_ser.text_input("✍️ 新系列名稱") if ser_opt == "➕ 手動輸入新系列..." else ser_opt
        auto_sku = generate_auto_sku(final_ser, final_cat, set(current_df['sku'].astype(str)) if not current_df.empty else set())
        c1, c2 = st.columns(2)
        sku = c1.text_input("3. 貨號", value=auto_sku)
        name = c2.text_input("4. 品名 *必填")
        v_spec = st.text_input("5. 規格"); v_color = st.text_input("6. 顏色"); note = st.text_input("7. 備註")
        if st.button("✨ 確認新增商品"):
            if sku and name:
                s, m = add_product(sku, name, final_cat, final_ser, v_spec, note, v_color)
                if s: st.success("新增成功"); time.sleep(1); st.rerun()
    with t2:
        df_p_f = get_formatted_product_df()
        if not df_p_f.empty:
            sel_l = st.selectbox("🔍 選擇商品", options=df_p_f['label'].tolist())
            if sel_l:
                sku_s = sel_l.split(" | ")[0]
                curr = df_p_f[df_p_f['sku'].astype(str) == sku_s].iloc[0]
                with st.form("edit_f"):
                    n_n = st.text_input("品名", value=str(curr['name']))
                    n_s = st.text_input("規格", value=str(curr['spec']))
                    n_c = st.text_input("顏色", value=str(curr['color']))
                    n_nt = st.text_input("備註", value=str(curr['note']))
                    if st.form_submit_button("💾 儲存修改"):
                        if update_product(sku_s, {'name': n_n, 'spec': n_s, 'color': n_c, 'note': n_nt}):
                            st.success("✅ 更新成功"); time.sleep(1); st.rerun()
        else: st.warning("⚠️ 資料庫為空，請先新增商品。")

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
            if st.form_submit_button("🚀 執行移庫"):
                if f_wh == t_wh: st.error("❌ 來源與目標不可相同")
                else:
                    sku = sel_p.split(" | ")[0]
                    add_transaction("移庫(撥出)", date.today(), sku, f_wh, qty, user, f"移至 {t_wh}")
                    add_transaction("移庫(撥入)", date.today(), sku, t_wh, qty, user, f"來自 {f_wh}")
                    st.success("✅ 移庫完成"); time.sleep(1); st.rerun()
    render_history_table(["移庫(撥出)", "移庫(撥入)"])

# --- 📥 進貨作業 ---
elif page == "📥 進貨作業":
    st.subheader("📥 進貨入庫")
    prods = get_formatted_product_df()
    if not prods.empty:
        with st.form("in_form"):
            sel_p = st.selectbox("商品", prods['label'])
            wh = st.selectbox("倉庫", WAREHOUSES)
            qty = st.number_input("數量", min_value=1.0, value=1.0)
            user = st.selectbox("經手人", KEYERS)
            if st.form_submit_button("執行進貨"):
                sku_only = sel_p.split(" | ")[0]
                if add_transaction("進貨", date.today(), sku_only, wh, qty, user, ""):
                    st.success("✅ 進貨成功"); time.sleep(1); st.rerun()
    render_history_table("進貨")

# --- 🚚 出貨作業 ---
elif page == "🚚 出貨作業":
    st.subheader("🚚 銷售出貨 (多品項清單)")
    if 'out_list' not in st.session_state: st.session_state['out_list'] = []
    col_a, col_b, col_c = st.columns(3)
    ship_opt = col_a.selectbox("寄送方式", SHIPPING_METHODS + ["➕ 手動輸入..."])
    final_ship = col_a.text_input("✍️ 自訂方式") if ship_opt == "➕ 手動輸入..." else ship_opt
    ship_no = col_b.text_input("配送號碼")
    user = col_c.selectbox("經手人", KEYERS, index=3)
    order_id = st.text_input("訂單編號 / 備註")
    st.divider()
    prods = get_formatted_product_df()
    if not prods.empty:
        col1, col2, col3 = st.columns([3, 1, 1])
        sel_p = col1.selectbox("挑選商品", prods['label'])
        wh = col2.selectbox("倉庫", WAREHOUSES, index=3); qty = col3.number_input("數量", 1.0)
        if st.button("⬇️ 加入待出貨清單"):
            st.session_state['out_list'].append({'sku': sel_p.split(" | ")[0], 'name': sel_p.split(" | ")[1], 'wh': wh, 'qty': qty})
            st.rerun()
    if st.session_state['out_list']:
        for i, item in enumerate(st.session_state['out_list']):
            c_l, c_d = st.columns([5, 1])
            c_l.write(f"🔸 **{item['name']}** - {item['wh']} x{item['qty']}")
            if c_d.button("❌", key=f"rm_o_{i}"): st.session_state['out_list'].pop(i); st.rerun()
        if st.button("✅ 確認出貨", type="primary", use_container_width=True):
            for x in st.session_state['out_list']:
                add_transaction("銷售出貨", date.today(), x['sku'], x['wh'], x['qty'], user, order_id, final_ship, ship_no)
            st.session_state['out_list'] = []; st.success("🎉 出貨完成"); time.sleep(1); st.rerun()
    render_history_table("銷售出貨")

# --- 🛒 訂單管理 ---
elif page == "🛒 訂單管理":
    st.subheader("🛒 訂單管理系統")
    tab_new, tab_list, tab_detail = st.tabs(["📝 新增訂單", "📋 訂單列表", "🔍 訂單明細"])

    with tab_new:
        if 'order_items' not in st.session_state:
            st.session_state['order_items'] = []

        st.markdown("##### 客戶資訊")
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
            o_price = oc4.number_input("單價", min_value=0.0, value=0.0, step=10.0, key="o_pprice")

            if st.button("⬇️ 加入訂單", key="o_add_item"):
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
            total = 0
            for i, item in enumerate(st.session_state['order_items']):
                ic1, ic2, ic3, ic4, ic5 = st.columns([3, 1, 1, 1, 0.5])
                ic1.write(f"**{item['product_name']}** ({item['sku']})")
                ic2.write(f"倉庫: {item['warehouse']}")
                ic3.write(f"x {item['qty']:.0f}")
                ic4.write(f"${item['subtotal']:,.0f}")
                if ic5.button("❌", key=f"o_rm_{i}"):
                    st.session_state['order_items'].pop(i)
                    st.rerun()
                total += item['subtotal']
            st.markdown(f"### 💰 訂單總金額: **${total:,.0f}**")

            if st.button("✅ 確認建立訂單", type="primary", use_container_width=True):
                if not cust_name:
                    st.error("❌ 請填寫客戶名稱")
                else:
                    ono = generate_order_no()
                    ok, msg = create_order(
                        ono, date.today(), cust_name, cust_phone,
                        cust_email, ship_addr,
                        st.session_state['order_items'], o_note, o_user
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
            fc1, fc2 = st.columns(2)
            status_filter = fc1.multiselect("篩選狀態", ORDER_STATUSES, default=["待處理", "處理中"])
            search_q = fc2.text_input("搜尋 (訂單號/客戶名)", key="o_search")

            filtered = df_orders.copy()
            if status_filter:
                filtered = filtered[filtered['status'].isin(status_filter)]
            if search_q:
                mask = (
                    filtered['order_no'].astype(str).str.contains(search_q, case=False, na=False) |
                    filtered['customer_name'].astype(str).str.contains(search_q, case=False, na=False)
                )
                filtered = filtered[mask]

            filtered = filtered.sort_index(ascending=False)
            st.markdown(f"共 **{len(filtered)}** 筆訂單")

            for _, row in filtered.iterrows():
                ono = str(row.get('order_no', ''))
                status = str(row.get('status', ''))
                icon = ORDER_STATUS_COLORS.get(status, "⚪")
                with st.expander(f"{icon} {ono} — {row.get('customer_name', '')} | ${row.get('total_amount', 0):,.0f} | {status}"):
                    dc1, dc2, dc3 = st.columns(3)
                    dc1.write(f"📅 日期: {row.get('order_date', '')}")
                    dc2.write(f"📞 電話: {row.get('customer_phone', '')}")
                    dc3.write(f"📧 Email: {row.get('customer_email', '')}")
                    st.write(f"📍 地址: {row.get('shipping_address', '')}")
                    if row.get('note', ''):
                        st.write(f"📝 備註: {row.get('note', '')}")

                    items = load_order_items(ono)
                    if not items.empty:
                        st.dataframe(
                            items[['sku', 'product_name', 'qty', 'unit_price', 'subtotal', 'warehouse']].rename(
                                columns={'sku': '貨號', 'product_name': '品名', 'qty': '數量',
                                         'unit_price': '單價', 'subtotal': '小計', 'warehouse': '倉庫'}
                            ),
                            use_container_width=True, hide_index=True
                        )

                    bc1, bc2, bc3, bc4, bc5 = st.columns(5)
                    if status == "待處理":
                        if bc1.button("🔵 處理中", key=f"os_proc_{ono}"):
                            update_order_status(ono, "處理中")
                            st.rerun()
                        if bc4.button("🔴 取消", key=f"os_cancel_{ono}"):
                            update_order_status(ono, "已取消")
                            st.rerun()
                    if status == "處理中":
                        ship_user = bc1.selectbox("出貨經手人", ["James", "Imeng", "小幫手"], key=f"os_ship_u_{ono}")
                        s_method = bc2.selectbox("寄送方式", SHIPPING_METHODS, key=f"os_ship_m_{ono}")
                        s_no = bc3.text_input("配送號碼", key=f"os_ship_n_{ono}")
                        if bc4.button("🚚 確認出貨", key=f"os_ship_{ono}"):
                            ok, msg = ship_order(ono, ship_user, s_method, s_no)
                            if ok:
                                st.success(msg)
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error(msg)
                        if bc5.button("🔴 取消", key=f"os_cancel2_{ono}"):
                            update_order_status(ono, "已取消")
                            st.rerun()
                    if status == "已出貨":
                        if bc1.button("🟢 完成", key=f"os_done_{ono}"):
                            update_order_status(ono, "已完成")
                            st.rerun()
                    if status in ["待處理", "已取消"]:
                        if bc5.button("🗑️ 刪除", key=f"os_del_{ono}"):
                            delete_order(ono)
                            st.rerun()

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
                mc2.metric("總金額", f"${row.get('total_amount', 0):,.0f}")
                mc3.metric("客戶", row.get('customer_name', ''))
                mc4.metric("日期", row.get('order_date', ''))
                st.write(f"📞 {row.get('customer_phone', '')} | 📧 {row.get('customer_email', '')}")
                st.write(f"📍 {row.get('shipping_address', '')}")
                if row.get('note', ''):
                    st.write(f"📝 {row.get('note', '')}")
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
        else:
            st.info("目前沒有任何訂單")

# --- 🔨 製造作業 ---
elif page == "🔨 製造作業":
    st.subheader("🔨 生產與拆解管理")
    if 'm_in_list' not in st.session_state: st.session_state['m_in_list'] = []
    prods = get_formatted_product_df()
    t1, t2, t3 = st.tabs(["領料清單", "完工入庫", "🔧 產品拆解"])
    with t1:
        m_note = st.text_input("領料備註", key="m_note")
        c1, c2, c3 = st.columns([3, 1, 1])
        sel = c1.selectbox("原料", prods['label'], key="msel")
        wh = c2.selectbox("發料倉庫", WAREHOUSES, key="mwh"); qty = c3.number_input("數量", 1.0, key="mqty")
        if st.button("⬇️ 加入清單", key="madd"):
            st.session_state['m_in_list'].append({'sku': sel.split(" | ")[0], 'name': sel.split(" | ")[1], 'wh': wh, 'qty': qty})
            st.rerun()
        if st.session_state['m_in_list']:
            for i, item in enumerate(st.session_state['m_in_list']):
                st.write(f"🔸 **{item['name']}** - {item['wh']} x{item['qty']}")
                if st.button("❌", key=f"rm_m_{i}"): st.session_state['m_in_list'].pop(i); st.rerun()
            if st.button("✅ 批次確認領料", type="primary"):
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
        st.info("💡 拆解：扣成品，回原料。")
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
        st.download_button("📥 下載 CSV", df.to_csv(index=False).encode('utf-8-sig'), f"Stock_{date.today()}.csv", "text/csv")

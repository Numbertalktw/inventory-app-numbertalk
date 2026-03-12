import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime
import time
import io

# ==========================================
# 1. 系統基礎設定
# ==========================================
PAGE_TITLE = "numbertalk 雲端庫存系統"
SPREADSHEET_NAME = "numbertalk-system" 

WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]
CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品", "數字珠", "數字串", "香料", "手作設備"]
SERIES = ["原料", "半成品", "成品", "包材", "生命數字能量項鍊", "數字手鍊", "貼紙", "小卡", "火漆章", "能量蠟燭", "香包", "水晶", "魔法鹽"]
KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]

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
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ 連線失敗: {e}")
        return None

def get_worksheet(sheet_name):
    client = get_client()
    if not client: return None
    try:
        sh = client.open(SPREADSHEET_NAME)
        return sh.worksheet(sheet_name)
    except Exception: return None

@st.cache_data(ttl=5) 
def load_data(sheet_name):
    ws = get_worksheet(sheet_name)
    if ws is None: return pd.DataFrame()
    try:
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        for col in ['sku', 'name', 'category', 'series', 'spec', 'color', 'note']:
            if col not in df.columns: df[col] = ""
        return df.fillna("")
    except Exception: return pd.DataFrame()

def clear_cache():
    load_data.clear()

# ==========================================
# 3. 核心邏輯功能
# ==========================================

def get_formatted_product_df():
    df = load_data("Products")
    if df.empty: return df
    df['spec'] = df['spec'].fillna('').astype(str)
    df['color'] = df['color'].fillna('').astype(str)
    df['label'] = df['sku'].astype(str) + " | " + df['name'].astype(str) + " (" + df['spec'] + " / " + df['color'] + ")"
    return df

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
        return True, "✅ 商品新增成功"
    except Exception as e:
        return False, f"新增失敗: {e}"

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
        return True, "✅ 更新成功"
    except Exception as e:
        return False, f"更新失敗: {e}"

def update_stock_qty(sku, warehouse, delta_qty):
    ws = get_worksheet("Stock")
    if not ws: return
    try:
        all_vals = ws.get_all_values()
        header = all_vals[0]
        sku_idx, wh_idx, qty_idx = header.index("sku"), header.index("warehouse"), header.index("qty")
        row_idx = -1
        current_val = 0.0
        for i, row in enumerate(all_vals[1:], 2):
            if str(row[sku_idx]) == str(sku) and str(row[wh_idx]) == str(warehouse):
                row_idx, current_val = i, float(row[qty_idx]) if row[qty_idx] else 0.0
                break
        if row_idx > 0:
            ws.update_cell(row_idx, qty_idx + 1, current_val + delta_qty)
        else:
            ws.append_row([str(sku), warehouse, delta_qty])
    except Exception as e:
        st.error(f"庫存更新失敗: {e}")

def add_transaction(doc_type, date_str, sku, wh, qty, user, note, cost=0):
    ws_hist = get_worksheet("History")
    prefix = {"進貨":"IN", "銷售出貨":"OUT", "製造領料":"MO", "製造入庫":"PD", "移庫(撥出)":"TR-O", "移庫(撥入)":"TR-I"}.get(doc_type, "ADJ")
    doc_no = f"{prefix}-{int(time.time())}"
    try:
        ws_hist.append_row([doc_type, doc_no, str(date_str), str(sku), wh, float(qty), user, note, float(cost), str(datetime.now())])
        factor = -1 if doc_type in ['銷售出貨', '製造領料', '移庫(撥出)'] else 1
        update_stock_qty(sku, wh, float(qty) * factor)
        clear_cache()
        return True
    except Exception as e:
        st.error(f"交易失敗: {e}"); return False

def delete_transaction(doc_no):
    ws_hist = get_worksheet("History")
    try:
        cells = ws_hist.findall(str(doc_no))
        if not cells: return False, "找不到該紀錄"
        for cell in reversed(cells):
            row_num = cell.row
            record = ws_hist.row_values(row_num)
            r_type, r_sku, r_wh, r_qty = record[0], record[3], record[4], float(record[5])
            reverse_factor = 1 if r_type in ['銷售出貨', '製造領料', '移庫(撥出)'] else -1
            update_stock_qty(r_sku, r_wh, r_qty * reverse_factor)
            ws_hist.delete_rows(row_num)
        clear_cache()
        return True, "✅ 已撤銷紀錄並還原庫存"
    except Exception as e:
        return False, f"撤銷失敗: {e}"

def transfer_stock(sku, from_wh, to_wh, qty, user, note):
    if from_wh == to_wh: return False, "❌ 來源與目標倉庫不能相同"
    today = str(date.today())
    add_transaction("移庫(撥出)", today, sku, from_wh, qty, user, f"移至 {to_wh} | {note}")
    add_transaction("移庫(撥入)", today, sku, to_wh, qty, user, f"來自 {from_wh} | {note}")
    return True, "移庫完成"

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

def render_history_table(doc_type_filter=None):
    st.markdown("#### 🕒 最近紀錄")
    df = load_data("History")
    if df.empty:
        st.info("尚無紀錄"); return
    df_prod = load_data("Products")
    sku_map = dict(zip(df_prod['sku'].astype(str), df_prod['name'])) if not df_prod.empty else {}
    if doc_type_filter:
        df = df[df['doc_type'].isin(doc_type_filter)] if isinstance(doc_type_filter, list) else df[df['doc_type'] == doc_type_filter]
    
    df = df.sort_index(ascending=False).head(15)
    cols = st.columns([1.5, 1.5, 3, 1, 1, 1, 2, 1])
    headers = ["單號", "日期", "品名 / SKU", "倉庫", "數量", "經手", "備註", "操作"]
    for col, h in zip(cols, headers): col.markdown(f"**{h}**")
    
    for idx, row in df.iterrows():
        c1, c2, c3, c4, c5, c6, c7, c8 = st.columns([1.5, 1.5, 3, 1, 1, 1, 2, 1])
        doc_no = str(row.get('doc_no', ''))
        c1.text(doc_no[-10:])
        c2.text(row.get('date', ''))
        sku = str(row.get('sku',''))
        c3.text(f"{sku_map.get(sku, '未知')}\n({sku})")
        c4.text(row.get('warehouse', ''))
        c5.text(row.get('qty', 0))
        c6.text(row.get('user', ''))
        c7.text(row.get('note', ''))
        if c8.button("🗑️", key=f"del_{doc_no}_{idx}"):
            with st.spinner("撤銷中..."):
                if delete_transaction(doc_no)[0]:
                    st.success("已撤銷"); time.sleep(1); st.rerun()
        st.divider()

# ==========================================
# 4. 主程式介面
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="💎")
st.title(f"💎 {PAGE_TITLE}")

if not get_client(): st.stop()

with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["📦 商品管理", "📦 移庫作業", "📥 進貨作業", "🚚 出貨作業", "🔨 製造作業", "📊 報表查詢"])
    if st.button("🔄 強制重新讀取"): clear_cache(); st.rerun()

# --- 📦 商品管理 (聯動式選單) ---
if page == "📦 商品管理":
    st.subheader("📦 商品資料維護")
    t1, t2 = st.tabs(["✨ 新增商品", "✏️ 修改/刪除商品"])
    with t1:
        current_df = load_data("Products")
        
        # 動態獲取現有分類
        existing_cats = sorted(list(set(current_df['category'].tolist()))) if not current_df.empty else []
        cat_list = sorted(list(set(CATEGORIES + existing_cats)))

        c_cat, c_ser = st.columns(2)
        
        # 1. 選擇分類
        cat_opt = c_cat.selectbox("1. 分類", cat_list + ["➕ 手動輸入新分類..."])
        final_cat = c_cat.text_input("✍️ 新分類名稱") if cat_opt == "➕ 手動輸入新分類..." else cat_opt
        
        # 🔄 ✨ 聯動選單邏輯 ✨ 🔄
        # 如果不是手動輸入，則根據所選分類篩選系列
        if cat_opt != "➕ 手動輸入新分類..." and not current_df.empty:
            # 找出試算表中對應當前分類的所有系列
            filtered_sers = current_df[current_df['category'] == cat_opt]['series'].unique().tolist()
            # 合併基礎清單 (過濾出原本可能屬於該分類的項目)
            final_ser_list = sorted(list(set(filtered_sers)))
            if not final_ser_list: # 如果該分類完全沒有紀錄，顯示預設清單
                final_ser_list = sorted(SERIES)
        else:
            final_ser_list = sorted(SERIES)

        # 2. 選擇系列 (會根據分類過濾)
        ser_opt = c_ser.selectbox("2. 系列 (已根據分類篩選)", final_ser_list + ["➕ 手動輸入新系列..."])
        final_ser = c_ser.text_input("✍️ 新系列名稱") if ser_opt == "➕ 手動輸入新系列..." else ser_opt
        
        auto_sku = generate_auto_sku(final_ser, final_cat, set(current_df['sku'].astype(str)) if not current_df.empty else set())
        c1, c2 = st.columns(2)
        sku = c1.text_input("3. 貨號", value=auto_sku)
        name = c2.text_input("4. 品名 *必填")
        v_spec = st.text_input("5. 規格 (形狀/尺寸)")
        v_color = st.text_input("6. 顏色")
        note = st.text_input("7. 備註")
        
        if st.button("✨ 確認新增商品", use_container_width=True):
            if sku and name and final_cat and final_ser:
                s, m = add_product(sku, name, final_cat, final_ser, v_spec, note, v_color)
                if s:
                    st.success(f"✅ 商品已成功建立！"); clear_cache(); time.sleep(1); st.rerun()
                else: st.error(m)
            else: st.error("❌ 請填寫必填欄位。")
            
    with t2:
        df_p = load_data("Products")
        if not df_p.empty:
            sel_sku = st.selectbox("🔍 選擇修改商品", df_p['sku'].astype(str))
            curr = df_p[df_p['sku'].astype(str) == sel_sku].iloc[0]
            with st.form("edit_prod"):
                n_name = st.text_input("品名", curr['name'])
                n_spec = st.text_input("規格", curr['spec'])
                n_color = st.text_input("顏色", curr['color'])
                n_note = st.text_input("備註", curr['note'])
                if st.form_submit_button("💾 儲存修改"):
                    s, m = update_product(sel_sku, {'name': n_name, 'spec': n_spec, 'color': n_color, 'note': n_note})
                    if s: st.success(m); time.sleep(1); st.rerun()

# --- 其餘頁面保持原本邏輯 ---
elif page == "📦 移庫作業":
    st.subheader("📦 倉庫間移庫")
    prods = get_formatted_product_df()
    if not prods.empty:
        with st.form("tr"):
            sel_p = st.selectbox("選擇商品", prods['label'], index=0)
            user = st.selectbox("經手人", KEYERS)
            w1, w2, q = st.columns(3)
            f_w = w1.selectbox("來源倉庫", WAREHOUSES, index=0)
            t_w = w2.selectbox("目標倉庫", WAREHOUSES, index=1)
            qty = q.number_input("數量", min_value=0.1, value=1.0)
            if st.form_submit_button("🚀 執行移庫"):
                s, m = transfer_stock(sel_p.split(" | ")[0], f_w, t_w, qty, user, "")
                if s: st.success(m); time.sleep(1); st.rerun()
    render_history_table(["移庫(撥出)", "移庫(撥入)"])

elif page == "📥 進貨作業":
    st.subheader("📥 進貨入庫")
    prods = get_formatted_product_df()
    if not prods.empty:
        with st.form("in"):
            c1, c2 = st.columns([2, 1])
            sel_p = c1.selectbox("商品", prods['label'])
            wh = c2.selectbox("倉庫", WAREHOUSES)
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", min_value=1.0, value=1.0)
            d_val = c4.date_input("日期", date.today())
            user = st.selectbox("經手人", KEYERS)
            if st.form_submit_button("執行進貨"):
                if add_transaction("進貨", str(d_val), sel_p.split(" | ")[0], wh, qty, user, ""):
                    st.success("成功"); time.sleep(1); st.rerun()
    render_history_table("進貨")

elif page == "🚚 出貨作業":
    st.subheader("🚚 銷售出貨 (多品項清單)")
    if 'out_list' not in st.session_state: st.session_state['out_list'] = []
    order_note = st.text_input("客戶備註 / 訂單號碼", placeholder="例如: #3918 魏 愛玲")
    user = st.selectbox("經手人", KEYERS, index=3)
    prods = get_formatted_product_df()
    if not prods.empty:
        col1, col2, col3 = st.columns([3, 1, 1])
        sel_p = col1.selectbox("挑選商品", prods['label'], index=0)
        wh = col2.selectbox("倉庫", WAREHOUSES, index=3)
        qty = col3.number_input("數量", min_value=1.0, value=1.0)
        if st.button("⬇️ 加入清單"):
            st.session_state['out_list'].append({'sku': sel_p.split(" | ")[0], 'name': sel_p.split(" | ")[1], 'wh': wh, 'qty': qty})
            st.rerun()
    if st.session_state['out_list']:
        st.markdown("#### 📋 待出貨清單")
        for i, item in enumerate(st.session_state['out_list']):
            c_t, c_d = st.columns([5, 1])
            c_t.write(f"🔸 **{item['name']}** ({item['sku']}) - {item['wh']} x{item['qty']}")
            if c_d.button("❌", key=f"rem_out_{i}"):
                st.session_state['out_list'].pop(i); st.rerun()
        if st.button("✅ 批次確認出貨", type="primary", use_container_width=True):
            for x in st.session_state['out_list']:
                add_transaction("銷售出貨", str(date.today()), x['sku'], x['wh'], x['qty'], user, order_note)
            st.session_state['out_list'] = []; st.success("批次出貨成功"); time.sleep(1); st.rerun()
    render_history_table("銷售出貨")

elif page == "🔨 製造作業":
    st.subheader("🔨 生產與拆解管理")
    if 'm_in_list' not in st.session_state: st.session_state['m_in_list'] = []
    prods = get_formatted_product_df()
    t1, t2, t3 = st.tabs(["領料", "完工", "🔧 產品拆解"])
    with t1:
        m_note = st.text_input("領料備註 (此單共用)", placeholder="例如: 製作鈦鋼7號人項鍊禮盒", key="m_note_batch")
        c1, c2, c3 = st.columns([3, 1, 1])
        sel = c1.selectbox("選擇原料", prods['label'], key="m_in_sel")
        wh = c2.selectbox("發料倉庫", WAREHOUSES, key="w_in_sel")
        qty = c3.number_input("數量", min_value=1.0, value=1.0, key="q_in_sel")
        if st.button("⬇️ 加入領料清單", use_container_width=True):
            st.session_state['m_in_list'].append({'sku': sel.split(" | ")[0], 'name': sel.split(" | ")[1], 'wh': wh, 'qty': qty})
            st.rerun()
        if st.session_state['m_in_list']:
            st.markdown("#### 📋 待領料清單")
            for i, item in enumerate(st.session_state['m_in_list']):
                col_txt, col_del = st.columns([5, 1])
                col_txt.write(f"🔸 **{item['name']}** ({item['sku']}) - {item['wh']} x{item['qty']}")
                if col_del.button("❌", key=f"rem_m_in_{i}"):
                    st.session_state['m_in_list'].pop(i); st.rerun()
            if st.button("✅ 批次確認領料", type="primary", use_container_width=True):
                for x in st.session_state['m_in_list']:
                    add_transaction("製造領料", str(date.today()), x['sku'], x['wh'], x['qty'], "工廠", m_note)
                st.session_state['m_in_list'] = []; st.success("批次領料完成"); time.sleep(1); st.rerun()
    with t2:
        with st.form("m2"):
            sel_out = st.selectbox("選擇成品", prods['label'], key="m_out_sel")
            wh_out = st.selectbox("入庫倉庫", WAREHOUSES, key="w_out_sel")
            qty_out = st.number_input("數量", min_value=1.0, value=1.0)
            note_out = st.text_input("完工備註", key="m_note_out")
            if st.form_submit_button("完工入庫"):
                add_transaction("製造入庫", str(date.today()), sel_out.split(" | ")[0], wh_out, qty_out, "工廠", note_out)
                st.success("完工入庫完成"); time.sleep(1); st.rerun()
    with t3:
        st.info("💡 拆解：先扣除成品，再加回原料。")
        c1, c2 = st.columns(2)
        with c1:
            with st.form("d1"):
                p = st.selectbox("成品", prods['label'], key="dp_sel")
                q = st.number_input("拆解量", 1.0)
                if st.form_submit_button("1. 扣除成品"):
                    add_transaction("製造入庫", str(date.today()), p.split(" | ")[0], "Wen", -q, "管理員", "拆解扣除")
                    st.success("已扣除"); time.sleep(1); st.rerun()
        with c2:
            with st.form("d2"):
                m = st.selectbox("原料", prods['label'], key="dm_sel")
                q = st.number_input("回庫量", 1.0)
                if st.form_submit_button("2. 回庫原料"):
                    add_transaction("製造領料", str(date.today()), m.split(" | ")[0], "Wen", -q, "管理員", "拆解回庫")
                    st.success("原料已回庫"); time.sleep(1); st.rerun()
    render_history_table(["製造領料", "製造入庫"])

elif page == "📊 報表查詢":
    st.subheader("📊 庫存報表")
    df = get_stock_overview()
    if not df.empty:
        st.dataframe(df, use_container_width=True)

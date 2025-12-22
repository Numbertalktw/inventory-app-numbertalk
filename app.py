import streamlit as st
import pandas as pd
import sqlite3
from datetime import date, datetime
import time
import io
import uuid

# ==========================================
# 1. 系統設定與固定參數
# ==========================================
PAGE_TITLE = "製造庫存系統 (分批認定版)"
DB_FILE = "inventory_system_batch.db"
ADMIN_PASSWORD = "8888"

WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]
CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品", "未分類"]
SERIES = ["原料", "半成品", "成品", "包材", "未分類"]
KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]
SHIPPING_METHODS = ["7-11", "全家", "萊爾富", "OK", "郵局", "順豐", "黑貓", "賣家宅配", "自取", "其他"]

# ==========================================
# 2. 資料庫核心 (SQLite)
# ==========================================
def get_connection():
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def init_db():
    conn = get_connection()
    c = conn.cursor()
    # 商品主檔
    c.execute('''CREATE TABLE IF NOT EXISTS products 
                 (sku TEXT PRIMARY KEY, name TEXT, category TEXT, series TEXT, spec TEXT)''')
    # 批次庫存表 (qty 為該批剩餘數量)
    c.execute('''CREATE TABLE IF NOT EXISTS stock 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, sku TEXT, warehouse TEXT, batch_id TEXT, 
                  supplier TEXT, unit_cost REAL, qty REAL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    # 進出流水帳
    c.execute('''CREATE TABLE IF NOT EXISTS history 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, doc_type TEXT, doc_no TEXT, date TEXT, 
                  sku TEXT, warehouse TEXT, qty REAL, user TEXT, note TEXT, supplier TEXT, 
                  unit_cost REAL, cost REAL, shipping_method TEXT, tracking_no TEXT, 
                  shipping_fee REAL, batch_id TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    conn.close()

def reset_db():
    conn = get_connection()
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS products")
    c.execute("DROP TABLE IF EXISTS stock")
    c.execute("DROP TABLE IF EXISTS history")
    conn.commit(); conn.close(); init_db()

# ==========================================
# 3. 核心邏輯函式
# ==========================================

def add_or_update_product(sku, name, category, series, spec=""):
    """確保商品主檔存在，若存在則更新資訊"""
    conn = get_connection()
    c = conn.cursor()
    c.execute('''INSERT INTO products (sku, name, category, series, spec) VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(sku) DO UPDATE SET name=excluded.name, category=excluded.category, 
                 series=excluded.series, spec=excluded.spec''', (sku, name, category, series, spec))
    conn.commit()
    conn.close()

def add_transaction_in(date_str, sku, wh, qty, user, note, supplier="", unit_cost=0, doc_type="進貨"):
    """分批認定法：每一筆進貨產生獨立 batch_id"""
    conn = get_connection()
    c = conn.cursor()
    try:
        batch_id = f"B{date_str.replace('-','')}-{uuid.uuid4().hex[:4].upper()}"
        c.execute("INSERT INTO stock (sku, warehouse, batch_id, supplier, unit_cost, qty) VALUES (?, ?, ?, ?, ?, ?)",
                  (sku, wh, batch_id, supplier, unit_cost, qty))
        
        doc_prefix = {"進貨": "IN", "期初建檔": "OPEN", "製造入庫": "PD", "庫存調整(加)": "ADJ+"}.get(doc_type, "DOC")
        doc_no = f"{doc_prefix}-{int(time.time()*1000)}"
        
        c.execute('''INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, 
                                          supplier, unit_cost, cost, batch_id) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                  (doc_type, doc_no, date_str, sku, wh, qty, user, note, supplier, unit_cost, qty * unit_cost, batch_id))
        conn.commit(); return True
    except: return False
    finally: conn.close()

def add_transaction_out(date_str, stock_id, qty, user, note, doc_type="銷售出貨", ship="", track="", fee=0):
    """從指定批次扣除庫存"""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("SELECT sku, warehouse, batch_id, supplier, unit_cost, qty FROM stock WHERE id=?", (stock_id,))
        row = c.fetchone()
        if not row or row[5] < qty: return False
        
        # 扣除庫存
        c.execute("UPDATE stock SET qty = qty - ? WHERE id = ?", (qty, stock_id))
        
        doc_prefix = {"銷售出貨": "OUT", "製造領料": "MO", "庫存調整(減)": "ADJ-"}.get(doc_type, "DOC")
        doc_no = f"{doc_prefix}-{int(time.time())}"
        
        c.execute('''INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, 
                                          supplier, unit_cost, cost, shipping_method, tracking_no, 
                                          shipping_fee, batch_id) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (doc_type, doc_no, date_str, row[0], row[1], qty, user, note, row[3], row[4], qty * row[4], ship, track, fee, row[2]))
        conn.commit(); return True
    except: return False
    finally: conn.close()

def process_batch_stock_update(file_obj):
    """支援橫向總表匯入 (自動識別 SKU 與 倉庫欄位)"""
    try:
        df = pd.read_csv(file_obj) if file_obj.name.endswith('.csv') else pd.read_excel(file_obj)
        df.columns = [str(c).strip() for c in df.columns]
        
        # 動態欄位識別
        mapping = {}
        for c in df.columns:
            c_low = c.lower()
            if 'sku' in c_low or '貨號' in c_low: mapping['sku'] = c
            if 'name' in c_low or '品名' in c_low: mapping['name'] = c
            if 'series' in c_low or '系列' in c_low: mapping['series'] = c
            if 'category' in c_low or '分類' in c_low: mapping['category'] = c
            if 'spec' in c_low or '規格' in c_low: mapping['spec'] = c

        if 'sku' not in mapping: return False, "Excel 必須包含 'SKU' 或 '貨號' 欄位"

        update_count = 0
        today = str(date.today())
        for _, row in df.iterrows():
            sku = str(row[mapping['sku']]).strip()
            if not sku or sku == 'nan': continue
            
            # 更新商品主檔
            add_or_update_product(
                sku, 
                str(row.get(mapping.get('name'), "未命名")),
                str(row.get(mapping.get('category'), "未分類")),
                str(row.get(mapping.get('series'), "未分類")),
                str(row.get(mapping.get('spec'), ""))
            )
            
            # 遍歷倉庫欄位匯入庫存
            for wh in WAREHOUSES:
                if wh in df.columns:
                    qty = row[wh]
                    if pd.notna(qty) and float(qty) > 0:
                        add_transaction_in(today, sku, wh, float(qty), "系統匯入", "總表期初匯入", doc_type="期初建檔")
                        update_count += 1
        return True, f"✅ 成功匯入 {update_count} 筆批次紀錄。"
    except Exception as e: return False, str(e)

def get_stock_overview():
    conn = get_connection()
    df_prod = pd.read_sql("SELECT * FROM products", conn)
    df_stock = pd.read_sql("SELECT sku, warehouse, SUM(qty) as qty FROM stock GROUP BY sku, warehouse", conn)
    conn.close()
    if df_prod.empty: return pd.DataFrame()
    pivot = df_stock.pivot(index='sku', columns='warehouse', values='qty').fillna(0)
    for wh in WAREHOUSES: 
        if wh not in pivot.columns: pivot[wh] = 0.0
    pivot['總庫存'] = pivot[WAREHOUSES].sum(axis=1)
    res = pd.merge(df_prod, pivot, on='sku', how='left').fillna(0)
    cols = ['sku', 'series', 'category', 'name', 'spec', '總庫存'] + WAREHOUSES
    return res[cols]

def get_batch_options(wh_filter=None):
    conn = get_connection()
    query = """SELECT s.id, s.sku, p.name, s.supplier, s.unit_cost, s.qty, s.batch_id, s.warehouse 
               FROM stock s LEFT JOIN products p ON s.sku = p.sku WHERE s.qty > 0"""
    if wh_filter: query += f" AND s.warehouse = '{wh_filter}'"
    df = pd.read_sql(query, conn)
    conn.close()
    return [(f"【{r['warehouse']}】{r['sku']} | {r['name']} | 餘:{r['qty']} ({r['batch_id']})", r['id'], r['qty']) for _, r in df.iterrows()]

# ==========================================
# 4. Streamlit UI 介面
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide")
init_db()

st.title(f"🏭 {PAGE_TITLE}")
is_manager = False 

with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["📦 庫存總覽與匯入", "📥 進貨入庫", "🚚 銷售出貨", "🔨 製造作業", "📊 報表查詢"])
    st.divider()
    with st.expander("🔐 主管權限"):
        if st.text_input("輸入管理密碼", type="password") == ADMIN_PASSWORD:
            is_manager = True; st.success("主管模式已開啟")
    if st.button("⚠️ 重置系統"):
        reset_db(); st.rerun()

# --- 頁面邏輯 ---

if page == "📦 庫存總覽與匯入":
    t1, t2 = st.tabs(["📊 現有庫存總表", "📥 批量匯入期初"])
    with t1:
        st.dataframe(get_stock_overview(), use_container_width=True)
    with t2:
        st.info("請上傳包含 SKU 與倉庫名 (Wen, 千畇, James, Imeng) 的 Excel 總表")
        up = st.file_uploader("選擇檔案", type=["xlsx", "csv"])
        if up and st.button("執行匯入"):
            ok, msg = process_batch_stock_update(up)
            st.success(msg) if ok else st.error(msg)

elif page == "📥 進貨入庫":
    st.subheader("新增進貨批次")
    prods = pd.read_sql("SELECT sku, name FROM products", get_connection())
    with st.form("in_form"):
        col1, col2 = st.columns(2)
        sku = col1.selectbox("選擇商品", [f"{r['sku']} | {r['name']}" for _, r in prods.iterrows()]).split(" | ")[0]
        wh = col2.selectbox("入庫倉庫", WAREHOUSES)
        qty = st.number_input("進貨數量", min_value=0.1)
        cost = st.number_input("進貨單價 (成本)", min_value=0.0) if is_manager else 0.0
        supp = st.text_input("供應商")
        user = st.selectbox("經手人", KEYERS)
        if st.form_submit_button("確認入庫"):
            if add_transaction_in(str(date.today()), sku, wh, qty, user, "手動進貨", supp, cost):
                st.success("入庫完成"); time.sleep(0.5); st.rerun()

elif page == "🚚 銷售出貨":
    st.subheader("指定批次出貨")
    wh_sel = st.selectbox("從哪個倉庫出貨？", WAREHOUSES)
    options = get_batch_options(wh_sel)
    if options:
        with st.form("out_form"):
            sel_idx = st.selectbox("選擇批次", range(len(options)), format_func=lambda x: options[x][0])
            qty = st.number_input("出貨數量", min_value=0.1, max_value=float(options[sel_idx][2]))
            ship = st.selectbox("運送方式", SHIPPING_METHODS)
            track = st.text_input("物流單號")
            user = st.selectbox("經手人", KEYERS)
            if st.form_submit_button("執行出貨"):
                if add_transaction_out(str(date.today()), options[sel_idx][1], qty, user, "銷售出貨", ship, track):
                    st.success("出貨完成"); time.sleep(0.5); st.rerun()
    else: st.warning("該倉庫目前無可用批次庫存")

elif page == "🔨 製造作業":
    st.subheader("生產領料與入庫")
    colA, colB = st.columns(2)
    with colA:
        st.write("### 1. 領用原料 (扣庫存)")
        wh_mo = st.selectbox("原料倉庫", WAREHOUSES, key="mo_wh")
        mo_options = get_batch_options(wh_mo)
        if mo_options:
            with st.form("mo_out"):
                idx = st.selectbox("選擇批次", range(len(mo_options)), format_func=lambda x: mo_options[x][0])
                qty = st.number_input("領用量", min_value=0.1, max_value=float(mo_options[idx][2]))
                if st.form_submit_button("確認領料"):
                    add_transaction_out(str(date.today()), mo_options[idx][1], qty, "工廠", "生產領料", "製造領料")
                    st.success("領料成功"); st.rerun()
    with colB:
        st.write("### 2. 成品入庫 (增庫存)")
        prods = pd.read_sql("SELECT sku, name FROM products", get_connection())
        with st.form("mo_in"):
            sku = st.selectbox("產出商品", [f"{r['sku']} | {r['name']}" for _, r in prods.iterrows()]).split(" | ")[0]
            wh = st.selectbox("存入倉庫", WAREHOUSES)
            qty = st.number_input("產出量", min_value=0.1)
            if st.form_submit_button("成品入庫"):
                add_transaction_in(str(date.today()), sku, wh, qty, "工廠", "生產完工", doc_type="製造入庫")
                st.success("成品已入庫"); st.rerun()

elif page == "📊 報表查詢":
    st.subheader("歷史交易流水帳")
    df_h = pd.read_sql("SELECT * FROM history ORDER BY id DESC", get_connection())
    if not is_manager:
        df_h = df_h.drop(columns=['unit_cost', 'cost'])
    st.dataframe(df_h, use_container_width=True)
    
    # 下載功能
    output = io.BytesIO()
    df_h.to_excel(output, index=False, engine='openpyxl')
    st.download_button("📥 下載 Excel 報表", output.getvalue(), f"inventory_report_{date.today()}.xlsx")

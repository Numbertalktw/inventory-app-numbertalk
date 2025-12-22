import streamlit as st
import pandas as pd
import sqlite3
from datetime import date, datetime
import time
import io
import uuid

# ==========================================
# 1. 系統設定
# ==========================================
PAGE_TITLE = "製造庫存系統 (分批認定完整版)"
DB_FILE = "inventory_system_batch.db"
ADMIN_PASSWORD = "8888"

# 固定參數
WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]
CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品", "未分類"]
SERIES = ["原料", "半成品", "成品", "包材", "未分類"]
KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]
SHIPPING_METHODS = ["7-11", "全家", "萊爾富", "OK", "郵局", "順豐", "黑貓", "賣家宅配", "自取", "其他"]
DEFAULT_REASONS = ["盤點差異", "報廢", "樣品借出", "系統修正", "其他"]

# ==========================================
# 2. 資料庫核心
# ==========================================
def get_connection():
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def init_db():
    conn = get_connection()
    c = conn.cursor()
    # 商品主檔
    c.execute('''CREATE TABLE IF NOT EXISTS products 
                 (sku TEXT PRIMARY KEY, name TEXT, category TEXT, series TEXT, spec TEXT)''')
    # 批次庫存
    c.execute('''CREATE TABLE IF NOT EXISTS stock 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, sku TEXT, warehouse TEXT, batch_id TEXT, 
                  supplier TEXT, unit_cost REAL, qty REAL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    # 流水帳
    c.execute('''CREATE TABLE IF NOT EXISTS history 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, doc_type TEXT, doc_no TEXT, date TEXT, 
                  sku TEXT, warehouse TEXT, qty REAL, user TEXT, note TEXT, supplier TEXT, 
                  unit_cost REAL, cost REAL, shipping_method TEXT, tracking_no TEXT, 
                  shipping_fee REAL, batch_id TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    conn.close()

# ==========================================
# 3. 功能函式
# ==========================================

def add_or_update_product(sku, name, category, series, spec=""):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''INSERT INTO products (sku, name, category, series, spec) VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(sku) DO UPDATE SET name=excluded.name, category=excluded.category, 
                 series=excluded.series, spec=excluded.spec''', (sku, name, category, series, spec))
    conn.commit(); conn.close()

def add_transaction_in(date_str, sku, wh, qty, user, note, supplier="", unit_cost=0, doc_type="進貨"):
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
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("SELECT sku, warehouse, batch_id, supplier, unit_cost, qty FROM stock WHERE id=?", (stock_id,))
        row = c.fetchone()
        if not row or row[5] < qty: return False
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

def get_stock_overview():
    conn = get_connection()
    df_prod = pd.read_sql("SELECT * FROM products", conn)
    df_stock = pd.read_sql("SELECT sku, warehouse, SUM(qty) as qty FROM stock GROUP BY sku, warehouse", conn)
    conn.close()
    if df_prod.empty: return pd.DataFrame()
    if df_stock.empty:
        res = df_prod.copy()
        for wh in WAREHOUSES: res[wh] = 0.0
        res['總庫存'] = 0.0
        return res
    pivot = df_stock.pivot(index='sku', columns='warehouse', values='qty').fillna(0)
    for wh in WAREHOUSES: 
        if wh not in pivot.columns: pivot[wh] = 0.0
    pivot['總庫存'] = pivot[WAREHOUSES].sum(axis=1)
    res = pd.merge(df_prod, pivot, on='sku', how='left').fillna(0)
    return res[['sku', 'series', 'category', 'name', 'spec', '總庫存'] + WAREHOUSES]

def get_batch_options(wh_filter=None):
    conn = get_connection()
    query = """SELECT s.id, s.sku, p.name, s.supplier, s.unit_cost, s.qty, s.batch_id, s.warehouse 
               FROM stock s LEFT JOIN products p ON s.sku = p.sku WHERE s.qty > 0"""
    if wh_filter: query += f" AND s.warehouse = '{wh_filter}'"
    df = pd.read_sql(query, conn); conn.close()
    return [(f"【{r['warehouse']}】{r['sku']} | {r['name']} | 餘:{r['qty']} ({r['batch_id']})", r['id'], r['qty']) for _, r in df.iterrows()]

def get_distinct_suppliers():
    conn = get_connection()
    df = pd.read_sql("SELECT DISTINCT supplier FROM history WHERE supplier != ''", conn); conn.close()
    return sorted(df['supplier'].tolist())

# ==========================================
# 4. Streamlit UI
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
        pwd = st.text_input("輸入管理密碼", type="password")
        if pwd == ADMIN_PASSWORD:
            is_manager = True; st.success("主管模式已開啟")
    if st.button("⚠️ 重置系統"):
        conn = get_connection(); c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS products"); c.execute("DROP TABLE IF EXISTS stock"); c.execute("DROP TABLE IF EXISTS history")
        conn.commit(); conn.close(); init_db(); st.rerun()

# --- 頁面 1：總覽與匯入 ---
if page == "📦 庫存總覽與匯入":
    t1, t2 = st.tabs(["📊 現有庫存總表", "📥 批量匯入期初"])
    with t1:
        st.dataframe(get_stock_overview(), use_container_width=True)
    with t2:
        st.info("💡 支援您的報表格式：需包含 SKU 欄位，以及 Wen, 千畇, James, Imeng 等倉庫欄位。")
        up = st.file_uploader("選擇 Excel 或 CSV 檔案", type=["xlsx", "csv"])
        if up and st.button("開始執行批量匯入"):
            try:
                df = pd.read_csv(up) if up.name.endswith('.csv') else pd.read_excel(up)
                df.columns = [str(c).strip() for c in df.columns]
                # 自動識別 SKU / 貨號
                sku_col = next((c for c in df.columns if 'sku' in c.lower() or '貨號' in c.lower()), None)
                if not sku_col: st.error("找不到 SKU 欄位"); st.stop()
                
                count = 0
                for _, row in df.iterrows():
                    sku = str(row[sku_col]).strip()
                    if not sku or sku == 'nan': continue
                    # 更新主檔
                    add_or_update_product(sku, str(row.get('Name', '未命名')), str(row.get('Category', '未分類')), str(row.get('Series', '未分類')), str(row.get('Spec', '')))
                    # 匯入各倉
                    for wh in WAREHOUSES:
                        if wh in df.columns and pd.notna(row[wh]) and float(row[wh]) > 0:
                            add_transaction_in(str(date.today()), sku, wh, float(row[wh]), "系統匯入", "期初匯入", doc_type="期初建檔")
                            count += 1
                st.success(f"✅ 成功匯入 {count} 筆批次記錄！"); time.sleep(1); st.rerun()
            except Exception as e: st.error(f"錯誤: {e}")

# --- 頁面 2：進貨 ---
elif page == "📥 進貨入庫":
    st.subheader("📥 新增進貨批次")
    prods = pd.read_sql("SELECT sku, name FROM products", get_connection())
    if prods.empty: st.warning("請先透過匯入功能建立商品資料。")
    else:
        with st.form("in_form"):
            col1, col2 = st.columns(2)
            sku = col1.selectbox("選擇商品", [f"{r['sku']} | {r['name']}" for _, r in prods.iterrows()]).split(" | ")[0]
            wh = col2.selectbox("入庫倉庫", WAREHOUSES)
            qty = st.number_input("進貨數量", min_value=0.01, step=1.0)
            cost = st.number_input("進貨單價 (成本)", min_value=0.0) if is_manager else 0.0
            supp_opts = [""] + get_distinct_suppliers()
            supp = st.selectbox("供應商", supp_opts) if supp_opts else st.text_input("供應商")
            user = st.selectbox("經手人", KEYERS)
            if st.form_submit_button("確認入庫"):
                if add_transaction_in(str(date.today()), sku, wh, qty, user, "手動進貨", str(supp), cost):
                    st.success("入庫完成"); time.sleep(0.5); st.rerun()

# --- 頁面 3：出貨 ---
elif page == "🚚 銷售出貨":
    st.subheader("🚚 指定批次出貨")
    wh_sel = st.selectbox("篩選倉庫", WAREHOUSES)
    options = get_batch_options(wh_sel)
    if options:
        with st.form("out_form"):
            sel_idx = st.selectbox("選擇出貨批次 (餘額)", range(len(options)), format_func=lambda x: options[x][0])
            qty = st.number_input("出貨數量", min_value=0.01, max_value=float(options[sel_idx][2]), step=1.0)
            c1, c2, c3 = st.columns(3)
            ship = c1.selectbox("運送方式", SHIPPING_METHODS)
            track = c2.text_input("物流單號")
            fee = c3.number_input("運費", min_value=0)
            user = st.selectbox("經手人", KEYERS)
            note = st.text_input("備註")
            if st.form_submit_button("確認執行出貨"):
                if add_transaction_out(str(date.today()), options[sel_idx][1], qty, user, note, "銷售出貨", ship, track, fee):
                    st.success("出貨成功"); time.sleep(0.5); st.rerun()
    else: st.warning("該倉庫目前無可用庫存。")

# --- 頁面 4：製造 ---
elif page == "🔨 製造作業":
    st.subheader("🔨 生產領料與入庫")
    colA, colB = st.columns(2)
    with colA:
        st.write("### 1. 生產領料 (扣除原料)")
        wh_mo = st.selectbox("原料倉", WAREHOUSES, key="m_wh")
        mo_opts = get_batch_options(wh_mo)
        if mo_opts:
            with st.form("mo_out"):
                idx = st.selectbox("選批次", range(len(mo_opts)), format_func=lambda x: mo_opts[x][0])
                qty = st.number_input("領用量", min_value=0.01, max_value=float(mo_opts[idx][2]))
                if st.form_submit_button("確認領料"):
                    add_transaction_out(str(date.today()), mo_opts[idx][1], qty, "生產", "製造領料", "製造領料")
                    st.success("領料完成"); st.rerun()
    with colB:
        st.write("### 2. 生產完工 (入庫成品)")
        prods = pd.read_sql("SELECT sku, name FROM products", get_connection())
        with st.form("mo_in"):
            sku = st.selectbox("完工商品", [f"{r['sku']} | {r['name']}" for _, r in prods.iterrows()]).split(" | ")[0]
            wh = st.selectbox("入庫倉", WAREHOUSES, key="mi_wh")
            qty = st.number_input("完工數量", min_value=0.01)
            if st.form_submit_button("確認入庫"):
                add_transaction_in(str(date.today()), sku, wh, qty, "生產", "完工入庫", doc_type="製造入庫")
                st.success("成品已入庫"); st.rerun()

# --- 頁面 5：報表 ---
elif page == "📊 報表查詢":
    st.subheader("📊 歷史交易流水帳")
    df_h = pd.read_sql("SELECT * FROM history ORDER BY id DESC", get_connection())
    if not is_manager: df_h = df_h.drop(columns=['unit_cost', 'cost'])
    st.dataframe(df_h, use_container_width=True)
    # 下載
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_h.to_excel(writer, index=False)
    st.download_button("📥 下載流水帳 Excel", output.getvalue(), f"history_{date.today()}.xlsx")

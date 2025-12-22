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
PAGE_TITLE = "製造庫存系統 (分流流水帳版)"
DB_FILE = "inventory_system_batch.db"
ADMIN_PASSWORD = "8888"

WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]
CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品", "未分類"]
SERIES = ["原料", "半成品", "成品", "包材", "未分類"]
KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]
SHIPPING_METHODS = ["7-11", "全家", "萊爾富", "OK", "郵局", "順豐", "黑貓", "賣家宅配", "自取", "其他"]

# ==========================================
# 2. 資料庫核心
# ==========================================
def get_connection():
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def init_db():
    conn = get_connection(); c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS products (sku TEXT PRIMARY KEY, name TEXT, category TEXT, series TEXT, spec TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS stock (id INTEGER PRIMARY KEY AUTOINCREMENT, sku TEXT, warehouse TEXT, batch_id TEXT, supplier TEXT, unit_cost REAL, qty REAL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, doc_type TEXT, doc_no TEXT, date TEXT, sku TEXT, warehouse TEXT, qty REAL, user TEXT, note TEXT, supplier TEXT, unit_cost REAL, cost REAL, shipping_method TEXT, tracking_no TEXT, shipping_fee REAL, batch_id TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit(); conn.close()

# --- 核心操作函式 ---
def add_or_update_product(sku, name, category, series, spec=""):
    conn = get_connection(); c = conn.cursor()
    c.execute('''INSERT INTO products (sku, name, category, series, spec) VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(sku) DO UPDATE SET name=excluded.name, category=excluded.category, series=excluded.series, spec=excluded.spec''', (sku, name, category, series, spec))
    conn.commit(); conn.close()

def add_transaction_in(date_str, sku, wh, qty, user, note, supplier="", unit_cost=0, doc_type="進貨"):
    conn = get_connection(); c = conn.cursor()
    try:
        batch_id = f"B{date_str.replace('-','')}-{uuid.uuid4().hex[:4].upper()}"
        c.execute("INSERT INTO stock (sku, warehouse, batch_id, supplier, unit_cost, qty) VALUES (?, ?, ?, ?, ?, ?)", (sku, wh, batch_id, supplier, unit_cost, qty))
        doc_prefix = {"進貨": "IN", "期初建檔": "OPEN", "製造入庫": "PD", "庫存調整(加)": "ADJ+"}.get(doc_type, "DOC")
        doc_no = f"{doc_prefix}-{int(time.time()*1000)}"
        c.execute("INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, supplier, unit_cost, cost, batch_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (doc_type, doc_no, date_str, sku, wh, qty, user, note, supplier, unit_cost, qty * unit_cost, batch_id))
        conn.commit(); return True
    except: return False
    finally: conn.close()

def add_transaction_out(date_str, stock_id, qty, user, note, doc_type="銷售出貨", ship="", track="", fee=0):
    conn = get_connection(); c = conn.cursor()
    try:
        c.execute("SELECT sku, warehouse, batch_id, supplier, unit_cost, qty FROM stock WHERE id=?", (stock_id,))
        row = c.fetchone()
        if not row or row[5] < qty: return False
        c.execute("UPDATE stock SET qty = qty - ? WHERE id = ?", (qty, stock_id))
        doc_prefix = {"銷售出貨": "OUT", "製造領料": "MO", "庫存調整(減)": "ADJ-"}.get(doc_type, "DOC")
        doc_no = f"{doc_prefix}-{int(time.time())}"
        c.execute("INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, supplier, unit_cost, cost, shipping_method, tracking_no, shipping_fee, batch_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (doc_type, doc_no, date_str, row[0], row[1], qty, user, note, row[3], row[4], qty * row[4], ship, track, fee, row[2]))
        conn.commit(); return True
    except: return False
    finally: conn.close()

def get_stock_overview():
    conn = get_connection(); df_prod = pd.read_sql("SELECT * FROM products", conn); df_stock = pd.read_sql("SELECT sku, warehouse, SUM(qty) as qty FROM stock GROUP BY sku, warehouse", conn); conn.close()
    if df_prod.empty: return pd.DataFrame()
    pivot = df_stock.pivot(index='sku', columns='warehouse', values='qty').fillna(0)
    for wh in WAREHOUSES: 
        if wh not in pivot.columns: pivot[wh] = 0.0
    pivot['總庫存'] = pivot[WAREHOUSES].sum(axis=1)
    res = pd.merge(df_prod, pivot, on='sku', how='left').fillna(0)
    return res[['sku', 'series', 'category', 'name', 'spec', '總庫存'] + WAREHOUSES]

# ==========================================
# 4. Streamlit UI
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide")
init_db()

st.title(f"🏭 {PAGE_TITLE}")
is_manager = False 

with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["📦 庫存總覽與匯入", "📥 進貨作業", "🚚 銷售出貨", "🔨 製造作業", "📊 流水帳報表"])
    st.divider()
    with st.expander("🔐 主管權限"):
        if st.text_input("輸入管理密碼", type="password") == ADMIN_PASSWORD:
            is_manager = True; st.success("主管模式開啟")
    if st.button("⚠️ 重置系統"):
        conn = get_connection(); c = conn.cursor(); c.execute("DROP TABLE IF EXISTS products"); c.execute("DROP TABLE IF EXISTS stock"); c.execute("DROP TABLE IF EXISTS history"); conn.commit(); conn.close(); init_db(); st.rerun()

# --- 頁面邏輯 ---

if page == "📦 庫存總覽與匯入":
    t1, t2 = st.tabs(["📊 現有庫存總表", "📥 批量匯入"])
    with t1: st.dataframe(get_stock_overview(), use_container_width=True)
    with t2:
        up = st.file_uploader("上傳 Excel 總表", type=["xlsx", "csv"])
        if up and st.button("執行匯入"):
            try:
                df = pd.read_excel(up) if up.name.endswith('.xlsx') else pd.read_csv(up)
                df.columns = [str(c).strip() for c in df.columns]
                sku_col = next((c for c in df.columns if 'sku' in c.lower() or '貨號' in c.lower()), None)
                for _, row in df.iterrows():
                    sku = str(row[sku_col]).strip()
                    if not sku or sku == 'nan': continue
                    add_or_update_product(sku, str(row.get('Name', '未命名')), str(row.get('Category', '未分類')), str(row.get('Series', '未分類')))
                    for wh in WAREHOUSES:
                        if wh in df.columns and pd.notna(row[wh]) and float(row[wh]) > 0:
                            add_transaction_in(str(date.today()), sku, wh, float(row[wh]), "系統匯入", "期初匯入", doc_type="期初建檔")
                st.success("匯入成功！"); st.rerun()
            except Exception as e: st.error(f"錯誤: {e}")

elif page == "📥 進貨作業":
    st.subheader("📥 進貨入庫")
    prods = pd.read_sql("SELECT sku, name FROM products", get_connection())
    with st.form("in_form"):
        sku = st.selectbox("選擇商品", [f"{r['sku']} | {r['name']}" for _, r in prods.iterrows()]).split(" | ")[0]
        wh = st.selectbox("倉庫", WAREHOUSES); qty = st.number_input("數量", min_value=0.01)
        cost = st.number_input("單價", min_value=0.0) if is_manager else 0.0
        supp = st.text_input("供應商"); user = st.selectbox("經手人", KEYERS)
        if st.form_submit_button("確認入庫"):
            if add_transaction_in(str(date.today()), sku, wh, qty, user, "手動進貨", supp, cost):
                st.success("入庫成功"); st.rerun()

elif page == "🚚 銷售出貨":
    st.subheader("🚚 銷售出貨")
    conn = get_connection(); df_s = pd.read_sql("SELECT s.id, s.sku, p.name, s.qty, s.warehouse, s.batch_id FROM stock s JOIN products p ON s.sku = p.sku WHERE s.qty > 0", conn); conn.close()
    if not df_s.empty:
        with st.form("out_form"):
            sel = st.selectbox("選擇批次庫存", range(len(df_s)), format_func=lambda x: f"【{df_s.iloc[x]['warehouse']}】{df_s.iloc[x]['sku']} | {df_s.iloc[x]['name']} | 餘:{df_s.iloc[x]['qty']}")
            qty = st.number_input("出貨數量", min_value=0.01, max_value=float(df_s.iloc[sel]['qty']))
            c1, c2 = st.columns(2); ship = c1.selectbox("貨運方式", SHIPPING_METHODS); track = c2.text_input("物流單號")
            user = st.selectbox("經手人", KEYERS); note = st.text_input("備註")
            if st.form_submit_button("執行出貨"):
                if add_transaction_out(str(date.today()), int(df_s.iloc[sel]['id']), qty, user, note, "銷售出貨", ship, track):
                    st.success("出貨成功"); st.rerun()
    else: st.warning("目前無庫存可出貨")

elif page == "🔨 製造作業":
    st.subheader("🔨 製造領料與完工")
    colA, colB = st.columns(2)
    with colA:
        st.write("### 1. 領用原料")
        # 領料邏輯與出貨類似，doc_type 改為 "製造領料"
        st.info("請參考銷售出貨邏輯選擇批次扣除")
    with colB:
        st.write("### 2. 完工入庫")
        # 完工入庫與進貨類似，doc_type 改為 "製造入庫"
        st.info("請參考進貨作業邏輯建立成品批次")

# ==========================================
# 5. 分流流水帳報表 (核心更新點)
# ==========================================
elif page == "📊 流水帳報表":
    st.subheader("📊 分流交易流水帳")
    
    # 定義查詢函式
    def get_filtered_history(doc_types):
        conn = get_connection()
        types_str = "','".join(doc_types)
        query = f"""
            SELECT h.date as '日期', h.doc_no as '單號', h.sku as '貨號', p.name as '品名', 
                   h.warehouse as '倉庫', h.qty as '數量', h.batch_id as '批號', 
                   h.supplier as '廠商/來源', h.unit_cost as '單價', h.cost as '總金額',
                   h.shipping_method as '貨運', h.tracking_no as '物流單號', h.shipping_fee as '運費',
                   h.user as '經手人', h.note as '備註'
            FROM history h
            LEFT JOIN products p ON h.sku = p.sku
            WHERE h.doc_type IN ('{types_str}')
            ORDER BY h.id DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        # 非管理員隱藏成本
        if not is_manager:
            df = df.drop(columns=['單價', '總金額'], errors='ignore')
        return df

    tab_in, tab_out, tab_mo = st.tabs(["📥 進貨流水帳", "🚚 出貨流水帳", "🔨 製造流水帳"])
    
    with tab_in:
        st.markdown("#### 顯示：進貨、期初建檔")
        df_in = get_filtered_history(["進貨", "期初建檔", "庫存調整(加)"])
        # 隱藏出貨相關欄位
        df_in = df_in.drop(columns=['貨運', '物流單號', '運費'], errors='ignore')
        st.dataframe(df_in, use_container_width=True)
        
    with tab_out:
        st.markdown("#### 顯示：銷售出貨")
        df_out = get_filtered_history(["銷售出貨", "庫存調整(減)"])
        # 銷售出貨通常不需要顯示「廠商」欄位
        df_out = df_out.drop(columns=['廠商/來源'], errors='ignore')
        st.dataframe(df_out, use_container_width=True)
        
    with tab_mo:
        st.markdown("#### 顯示：製造領料、製造入庫")
        df_mo = get_filtered_history(["製造領料", "製造入庫"])
        # 製造單據隱藏物流與廠商資訊
        df_mo = df_mo.drop(columns=['廠商/來源', '貨運', '物流單號', '運費'], errors='ignore')
        st.dataframe(df_mo, use_container_width=True)

    # 匯出全部報表功能
    if st.button("📥 匯出所有歷史紀錄 (Excel)"):
        conn = get_connection(); all_h = pd.read_sql("SELECT * FROM history ORDER BY id DESC", conn); conn.close()
        towrite = io.BytesIO()
        all_h.to_excel(towrite, index=False, engine='openpyxl')
        st.download_button("點此下載", towrite.getvalue(), f"all_history_{date.today()}.xlsx")

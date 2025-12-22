import streamlit as st
import pandas as pd
import sqlite3
from datetime import date, datetime
import time
import io

# ==========================================
# 1. 系統設定與初始化
# ==========================================
PAGE_TITLE = "製造庫存系統 (BOM & 成本管理版)"
DB_FILE = "inventory_bom_system.db"
ADMIN_PASSWORD = "8888"

WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]
CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品"]
SERIES = ["原料", "半成品", "成品", "包材"]
KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]
SHIPPING_METHODS = ["7-11", "全家", "萊爾富", "OK", "郵局", "順豐", "黑貓", "賣家宅配", "自取", "其他"]

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
    # 異動歷史
    c.execute('''CREATE TABLE IF NOT EXISTS history 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, doc_type TEXT, doc_no TEXT, date TEXT, 
                  sku TEXT, warehouse TEXT, qty REAL, user TEXT, note TEXT, supplier TEXT, 
                  unit_cost REAL, cost REAL, shipping_method TEXT, tracking_no TEXT, 
                  shipping_fee REAL, batch_id TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    # BOM 配方表
    c.execute('''CREATE TABLE IF NOT EXISTS bom 
                 (parent_sku TEXT, child_sku TEXT, quantity REAL, PRIMARY KEY (parent_sku, child_sku))''')
    conn.commit()
    conn.close()

# ==========================================
# 2. 資料運算核心 (邏輯層)
# ==========================================

def get_weighted_average_cost(sku):
    """計算原料目前的加權平均成本 (主管視角)"""
    conn = get_connection()
    res = conn.execute("SELECT SUM(qty * unit_cost), SUM(qty) FROM stock WHERE sku = ? AND qty > 0", (sku,)).fetchone()
    conn.close()
    if res and res[1] and res[1] > 0:
        return res[0] / res[1]
    return 0.0

def check_bom_shortage(parent_sku, target_qty, warehouse):
    """檢查生產所需的原料是否充足"""
    conn = get_connection()
    query = """
        SELECT b.child_sku, p.name, b.quantity, 
               (SELECT SUM(qty) FROM stock WHERE sku = b.child_sku AND warehouse = ?) as current_stock
        FROM bom b
        LEFT JOIN products p ON b.child_sku = p.sku
        WHERE b.parent_sku = ?
    """
    df = pd.read_sql(query, conn, params=(warehouse, parent_sku))
    conn.close()
    shortage = []
    for _, row in df.iterrows():
        needed = row['quantity'] * target_qty
        stock = row['current_stock'] if row['current_stock'] else 0
        if stock < needed:
            shortage.append({"原料": f"{row['child_sku']} {row['name']}", "需求": needed, "現有": stock, "缺口": needed - stock})
    return shortage

def auto_deduct_with_cost_calculation(parent_sku, produce_qty, warehouse, user):
    """執行 FIFO 自動扣料生產並計算成品成本"""
    conn = get_connection()
    c = conn.cursor()
    total_material_cost = 0.0
    try:
        c.execute("BEGIN TRANSACTION")
        c.execute("SELECT child_sku, quantity FROM bom WHERE parent_sku = ?", (parent_sku,))
        recipe = c.fetchall()
        
        for child_sku, unit_qty in recipe:
            needed = unit_qty * produce_qty
            c.execute("SELECT id, qty, unit_cost, batch_id, supplier FROM stock WHERE sku = ? AND warehouse = ? AND qty > 0 ORDER BY created_at ASC", (child_sku, warehouse))
            batches = c.fetchall()
            
            for b_id, b_qty, b_cost, b_batch_id, b_supp in batches:
                if needed <= 0: break
                take = min(needed, b_qty)
                total_material_cost += (take * b_cost)
                c.execute("UPDATE stock SET qty = qty - ? WHERE id = ?", (take, b_id))
                # 記錄領料歷史
                c.execute("INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, supplier, unit_cost, cost, batch_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                          ("製造領料", f"MO-AUTO-{int(time.time())}", str(date.today()), child_sku, warehouse, take, user, f"生產{parent_sku}自動領料", b_supp, b_cost, take*b_cost, b_batch_id))
                needed -= take
        
        # 成品入庫
        final_unit_cost = total_material_cost / produce_qty if produce_qty > 0 else 0
        batch_id = f"B-PROD-{int(time.time())}"
        c.execute("INSERT INTO stock (sku, warehouse, batch_id, supplier, unit_cost, qty) VALUES (?,?,?,?,?,?)",
                  (parent_sku, warehouse, batch_id, "內部生產", final_unit_cost, produce_qty))
        c.execute("INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, supplier, unit_cost, cost, batch_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                  ("製造入庫", f"PD-{int(time.time())}", str(date.today()), parent_sku, warehouse, produce_qty, user, "BOM自動化生產", "自製", final_unit_cost, total_material_cost, batch_id))
        
        conn.commit()
        return True, final_unit_cost
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

# ==========================================
# 3. UI 介面層
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide")
init_db()

st.title(f"🏭 {PAGE_TITLE}")

# 權限驗證
is_manager = False
with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["📦 商品建檔", "📥 進貨作業", "🚚 出貨作業", "🔨 製造與BOM", "📊 報表查詢"])
    st.divider()
    with st.expander("🔐 主管權限"):
        if st.text_input("密碼", type="password") == ADMIN_PASSWORD:
            is_manager = True
            st.success("主管模式已開啟")

# --- 1. 商品建檔 ---
if page == "📦 商品建檔":
    st.subheader("📦 商品資料維護")
    with st.form("add_prod"):
        c1, c2, c3, c4 = st.columns(4)
        sku = c1.text_input("貨號 (SKU)")
        name = c2.text_input("品名")
        cat = c3.selectbox("分類", CATEGORIES)
        ser = c4.selectbox("系列", SERIES)
        if st.form_submit_button("新增商品"):
            conn = get_connection()
            try:
                conn.execute("INSERT INTO products VALUES (?,?,?,?,?)", (sku, name, cat, ser, ""))
                conn.commit()
                st.success("新增成功")
            except: st.error("貨號重複")
            finally: conn.close()
    
    df_p = pd.read_sql("SELECT * FROM products", get_connection())
    st.dataframe(df_p, use_container_width=True)

# --- 2. 進貨作業 ---
elif page == "📥 進貨作業":
    st.subheader("📥 進貨入庫 (建立新批次)")
    prods = pd.read_sql("SELECT sku, name FROM products", get_connection())
    prod_opts = [f"{r['sku']} | {r['name']}" for _, r in prods.iterrows()]
    
    with st.form("in_form"):
        c1, c2, c3 = st.columns([2,1,1])
        sel_p = c1.selectbox("選擇商品", prod_opts)
        wh = c2.selectbox("入庫倉庫", WAREHOUSES)
        qty = c3.number_input("數量", min_value=0.1)
        
        c4, c5 = st.columns(2)
        supp = c4.text_input("供應商")
        cost = c5.number_input("進貨單價 (成本)", min_value=0.0) if is_manager else 0.0
        
        user = st.selectbox("經手人", KEYERS)
        if st.form_submit_button("執行入庫"):
            sku = sel_p.split(" | ")[0]
            batch_id = f"IN-{int(time.time())}"
            conn = get_connection()
            conn.execute("INSERT INTO stock (sku, warehouse, batch_id, supplier, unit_cost, qty) VALUES (?,?,?,?,?,?)",
                         (sku, wh, batch_id, supp, cost, qty))
            conn.execute("INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, supplier, unit_cost, cost, batch_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                         ("進貨", batch_id, str(date.today()), sku, wh, qty, user, "", supp, cost, qty*cost, batch_id))
            conn.commit()
            conn.close()
            st.success("入庫成功")

# --- 4. 製造與 BOM (核心) ---
elif page == "🔨 製造與BOM":
    st.subheader("🔨 生產與配方管理")
    t1, t2 = st.tabs(["🚀 一鍵生產 (自動扣料)", "📋 BOM 配方設定"])

    with t2:
        st.markdown("### 📋 產品配方設定")
        all_prods = pd.read_sql("SELECT sku, name FROM products", get_connection())
        p_list = [f"{r['sku']} | {r['name']}" for _, r in all_prods.iterrows()]
        
        with st.form("bom_form"):
            c1, c2, c3 = st.columns([2, 2, 1])
            b_parent = c1.selectbox("選擇成品", p_list, key="bp")
            b_child = c2.selectbox("選擇原料", p_list, key="bc")
            b_qty = c3.number_input("單位用量", min_value=0.01)
            if st.form_submit_button("儲存配方"):
                p_sku = b_parent.split(" | ")[0]
                c_sku = b_child.split(" | ")[0]
                conn = get_connection()
                conn.execute("INSERT OR REPLACE INTO bom VALUES (?,?,?)", (p_sku, c_sku, b_qty))
                conn.commit()
                conn.close()
                st.rerun()

        # 顯示 BOM 與成本 (主管限定)
        bom_data = pd.read_sql("""SELECT b.*, p1.name as p_name, p2.name as c_name 
                                  FROM bom b 
                                  JOIN products p1 ON b.parent_sku = p1.sku 
                                  JOIN products p2 ON b.child_sku = p2.sku""", get_connection())
        for p_sku in bom_data['parent_sku'].unique():
            p_name = bom_data[bom_data['parent_sku']==p_sku]['p_name'].iloc[0]
            with st.expander(f"📦 {p_sku} - {p_name}"):
                items = bom_data[bom_data['parent_sku']==p_sku]
                total_est = 0.0
                for _, row in items.iterrows():
                    c1, c2, c3 = st.columns([3, 1, 2])
                    c1.text(f"└─ {row['child_sku']} {row['c_name']}")
                    c2.text(f"x {row['quantity']}")
                    if is_manager:
                        avg = get_weighted_average_cost(row['child_sku'])
                        sub = avg * row['quantity']
                        total_est += sub
                        c3.text(f"單價:${avg:,.1f} (小計:${sub:,.1f})")
                if is_manager:
                    st.markdown(f"**預估生產單價：:red[${total_est:,.2f}]**")

    with t1:
        st.markdown("### 🚀 自動化生產")
        sel_p = st.selectbox("要生產的成品", p_list, key="prod_p")
        wh = st.selectbox("生產倉庫", WAREHOUSES, key="prod_w")
        p_qty = st.number_input("生產數量", min_value=1)
        
        target_sku = sel_p.split(" | ")[0]
        shortages = check_bom_shortage(target_sku, p_qty, wh)
        
        if shortages:
            st.error("⚠️ 原料不足，無法生產")
            st.table(pd.DataFrame(shortages))
        else:
            st.success("✅ 原料充足")
            if st.button("確認執行自動扣料生產"):
                ok, res = auto_deduct_with_cost_calculation(target_sku, p_qty, wh, "系統")
                if ok:
                    st.balloons()
                    msg = f"生產成功！成品單位成本為: ${res:,.2f}" if is_manager else "生產成功，庫存已更新。"
                    st.success(msg)
                    time.sleep(2); st.rerun()
                else: st.error(f"失敗: {res}")

# --- 5. 報表查詢 ---
elif page == "📊 報表查詢":
    st.subheader("📊 庫存動態報表")
    # 這裡顯示歷史紀錄，根據 is_manager 過濾成本
    query = """SELECT h.date, h.doc_type, h.sku, p.name, h.warehouse, h.qty, h.unit_cost, h.cost, h.user 
               FROM history h LEFT JOIN products p ON h.sku = p.sku ORDER BY h.id DESC"""
    df_h = pd.read_sql(query, get_connection())
    
    if not is_manager:
        df_h = df_h.drop(columns=['unit_cost', 'cost'])
    
    st.write("### 異動流水帳")
    st.dataframe(df_h, use_container_width=True)

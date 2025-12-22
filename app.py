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
DB_FILE = "inventory_erp_system.db"
ADMIN_PASSWORD = "8888"

# 下拉選單預設值
WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]
CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品", "數字珠", "材料"]
SERIES = ["原料", "半成品", "成品", "包材", "生命數字能量項鍊", "水晶", "魔法鹽"]
KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]
SHIPPING_METHODS = ["7-11", "全家", "萊爾富", "OK", "郵局", "順豐", "黑貓", "賣家宅配", "自取", "其他"]
DEFAULT_REASONS = ["盤點差異", "報廢", "樣品借出", "系統修正", "其他"]

def get_connection():
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def init_db():
    conn = get_connection()
    c = conn.cursor()
    # 1. 商品主檔
    c.execute('''CREATE TABLE IF NOT EXISTS products 
                 (sku TEXT PRIMARY KEY, name TEXT, category TEXT, series TEXT, spec TEXT)''')
    # 2. 批次庫存 (Batch Stock)
    c.execute('''CREATE TABLE IF NOT EXISTS stock 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, sku TEXT, warehouse TEXT, batch_id TEXT, 
                  supplier TEXT, unit_cost REAL, qty REAL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    # 3. 異動歷史 (History)
    c.execute('''CREATE TABLE IF NOT EXISTS history 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, doc_type TEXT, doc_no TEXT, date TEXT, 
                  sku TEXT, warehouse TEXT, qty REAL, user TEXT, note TEXT, supplier TEXT, 
                  unit_cost REAL, cost REAL, shipping_method TEXT, tracking_no TEXT, 
                  shipping_fee REAL, batch_id TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    # 4. BOM 配方表
    c.execute('''CREATE TABLE IF NOT EXISTS bom 
                 (parent_sku TEXT, child_sku TEXT, quantity REAL, PRIMARY KEY (parent_sku, child_sku))''')
    conn.commit()
    conn.close()

def reset_db():
    conn = get_connection()
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS products")
    c.execute("DROP TABLE IF EXISTS stock")
    c.execute("DROP TABLE IF EXISTS history")
    c.execute("DROP TABLE IF EXISTS bom")
    conn.commit()
    conn.close()
    init_db()

# ==========================================
# 2. 核心邏輯函式
# ==========================================

def add_product(sku, name, category, series, spec):
    conn = get_connection()
    try:
        conn.execute("INSERT INTO products (sku, name, category, series, spec) VALUES (?, ?, ?, ?, ?)",
                     (sku, name, category, series, spec))
        conn.commit()
        return True, "成功"
    except sqlite3.IntegrityError:
        return False, "貨號已存在"
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def get_all_products():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    return df

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
    if not df.empty:
        for _, row in df.iterrows():
            needed = row['quantity'] * target_qty
            stock = row['current_stock'] if pd.notna(row['current_stock']) else 0
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
        
        if not recipe:
            conn.rollback()
            return False, "未設定 BOM 配方"

        for child_sku, unit_qty in recipe:
            needed = unit_qty * produce_qty
            # FIFO: 抓取最早的庫存
            c.execute("SELECT id, qty, unit_cost, batch_id, supplier FROM stock WHERE sku = ? AND warehouse = ? AND qty > 0 ORDER BY created_at ASC", (child_sku, warehouse))
            batches = c.fetchall()
            
            for b_id, b_qty, b_cost, b_batch_id, b_supp in batches:
                if needed <= 0: break
                take = min(needed, b_qty)
                total_material_cost += (take * b_cost)
                c.execute("UPDATE stock SET qty = qty - ? WHERE id = ?", (take, b_id))
                # 記錄領料歷史
                doc_no = f"MO-{int(time.time())}"
                c.execute("INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, supplier, unit_cost, cost, batch_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                          ("製造領料", doc_no, str(date.today()), child_sku, warehouse, take, user, f"生產{parent_sku}", b_supp, b_cost, take*b_cost, b_batch_id))
                needed -= take
            
            if needed > 0.000001: # 浮點數容錯
                raise Exception(f"原料 {child_sku} 庫存不足，尚缺 {needed}")
        
        # 成品入庫
        final_unit_cost = total_material_cost / produce_qty if produce_qty > 0 else 0
        batch_id = f"B-PROD-{int(time.time())}"
        c.execute("INSERT INTO stock (sku, warehouse, batch_id, supplier, unit_cost, qty) VALUES (?,?,?,?,?,?)",
                  (parent_sku, warehouse, batch_id, "內部生產", final_unit_cost, produce_qty))
        c.execute("INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, supplier, unit_cost, cost, batch_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                  ("製造入庫", f"PD-{int(time.time())}", str(date.today()), parent_sku, warehouse, produce_qty, user, "BOM自動生產", "自製", final_unit_cost, total_material_cost, batch_id))
        
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
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="🏭")
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
    st.divider()
    if st.button("🔴 重置資料庫 (清空所有資料)"):
        reset_db()
        st.cache_data.clear()
        st.success("已重置！")
        time.sleep(1); st.rerun()

# --- 1. 商品建檔 ---
if page == "📦 商品建檔":
    st.subheader("📦 商品資料維護")
    t1, t2 = st.tabs(["單筆建檔", "批量匯入 (Excel/CSV)"])
    
    with t1:
        with st.form("add_prod"):
            c1, c2, c3, c4, c5 = st.columns(5)
            sku = c1.text_input("貨號 (SKU)")
            name = c2.text_input("品名")
            cat = c3.selectbox("分類", CATEGORIES)
            ser = c4.selectbox("系列", SERIES)
            spec = c5.text_input("規格")
            if st.form_submit_button("新增商品"):
                ok, msg = add_product(sku, name, cat, ser, spec)
                if ok: st.success("新增成功"); st.rerun()
                else: st.error(msg)
    
    with t2:
        st.info("支援欄位：貨號, 品名, 分類, 系列, 規格")
        up = st.file_uploader("上傳商品清單")
        if up and st.button("開始匯入"):
            try:
                df = pd.read_csv(up) if up.name.endswith('.csv') else pd.read_excel(up)
                df.columns = [str(c).strip() for c in df.columns] # 清除欄位空白
                
                success_count = 0
                for _, row in df.iterrows():
                    # 智慧對應欄位
                    r_sku = str(row['貨號']).strip() if '貨號' in df.columns else ""
                    # 若無貨號欄位，嘗試抓取常見欄位名
                    if not r_sku and 'SKU' in df.columns: r_sku = str(row['SKU']).strip()
                    
                    if not r_sku or r_sku == "nan": continue
                    
                    r_name = str(row['品名']).strip() if '品名' in df.columns else ""
                    r_cat = str(row['分類']).strip() if '分類' in df.columns else "未分類"
                    r_ser = str(row['系列']).strip() if '系列' in df.columns else "未分類"
                    r_spec = str(row['規格']).strip() if '規格' in df.columns else ""
                    
                    ok, _ = add_product(r_sku, r_name, r_cat, r_ser, r_spec)
                    if ok: success_count += 1
                
                st.success(f"匯入完成！成功新增 {success_count} 筆商品。")
            except Exception as e:
                st.error(f"匯入失敗: {e}")

    st.divider()
    st.dataframe(get_all_products(), use_container_width=True)

# --- 2. 進貨作業 ---
elif page == "📥 進貨作業":
    st.subheader("📥 進貨入庫 (建立新批次)")
    prods = get_all_products()
    if not prods.empty:
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
                st.success("入庫成功"); time.sleep(0.5); st.rerun()

# --- 3. 出貨作業 ---
elif page == "🚚 出貨作業":
    st.subheader("🚚 銷售出貨")
    wh_filter = st.selectbox("篩選倉庫", WAREHOUSES)
    conn = get_connection()
    df_s = pd.read_sql("SELECT * FROM stock WHERE qty > 0 AND warehouse = ? ORDER BY created_at", conn, params=(wh_filter,))
    conn.close()
    
    if not df_s.empty:
        # 建立選項
        batch_opts = []
        for _, r in df_s.iterrows():
            label = f"{r['sku']} | 餘量:{r['qty']} | 批號:{r['batch_id']}"
            if is_manager: label += f" | 成本:${r['unit_cost']}"
            batch_opts.append((label, r['id'], r['qty']))
            
        with st.form("out_form"):
            sel_idx = st.selectbox("選擇批次", range(len(batch_opts)), format_func=lambda x: batch_opts[x][0])
            s_id, s_qty = batch_opts[sel_idx][1], batch_opts[sel_idx][2]
            
            out_qty = st.number_input("出貨數量", min_value=1.0, max_value=float(s_qty))
            user = st.selectbox("經手人", KEYERS)
            
            if st.form_submit_button("確認出貨"):
                conn = get_connection()
                c = conn.cursor()
                # 扣庫存
                c.execute("UPDATE stock SET qty = qty - ? WHERE id = ?", (out_qty, s_id))
                # 寫紀錄
                # 這裡為了簡化，省略查詢 product name 的步驟，直接寫入
                c.execute("SELECT sku, warehouse, batch_id, unit_cost, supplier FROM stock WHERE id = ?", (s_id,))
                b_data = c.fetchone()
                if b_data:
                    sku, wh, bid, u_cost, supp = b_data
                    doc_no = f"OUT-{int(time.time())}"
                    c.execute("INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, supplier, unit_cost, cost, batch_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                              ("銷售出貨", doc_no, str(date.today()), sku, wh, out_qty, user, "", supp, u_cost, out_qty*u_cost, bid))
                    conn.commit()
                    st.success("出貨成功"); time.sleep(0.5); st.rerun()
                conn.close()
    else:
        st.info("此倉庫無庫存")

# --- 4. 製造與 BOM (核心) ---
elif page == "🔨 製造與BOM":
    st.subheader("🔨 生產與配方管理")
    t1, t2 = st.tabs(["🚀 一鍵生產 (自動扣料)", "📋 BOM 配方設定"])

    with t2:
        st.markdown("### 📋 產品配方設定")
        all_prods = get_all_products()
        if not all_prods.empty:
            p_list = [f"{r['sku']} | {r['name']}" for _, r in all_prods.iterrows()]
            
            with st.form("bom_form"):
                c1, c2, c3 = st.columns([2, 2, 1])
                b_parent = c1.selectbox("選擇成品", p_list, key="bp")
                b_child = c2.selectbox("選擇原料", p_list, key="bc")
                b_qty = c3.number_input("單位用量", min_value=0.01)
                if st.form_submit_button("儲存配方"):
                    p_sku = b_parent.split(" | ")[0]
                    c_sku = b_child.split(" | ")[0]
                    if p_sku == c_sku:
                        st.error("成品與原料不能相同")
                    else:
                        conn = get_connection()
                        conn.execute("INSERT OR REPLACE INTO bom VALUES (?,?,?)", (p_sku, c_sku, b_qty))
                        conn.commit()
                        conn.close()
                        st.success(f"已設定 {p_sku} 配方")
                        time.sleep(0.5); st.rerun()

            # 顯示 BOM 與成本 (主管限定)
            bom_data = pd.read_sql("""SELECT b.*, p1.name as p_name, p2.name as c_name 
                                      FROM bom b 
                                      JOIN products p1 ON b.parent_sku = p1.sku 
                                      JOIN products p2 ON b.child_sku = p2.sku""", get_connection())
            if not bom_data.empty:
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
            else:
                st.info("尚無配方資料")

    with t1:
        st.markdown("### 🚀 自動化生產")
        if not all_prods.empty:
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
    conn = get_connection()
    # 庫存總表
    df_stock = pd.read_sql("""
        SELECT s.sku, p.name, s.warehouse, SUM(s.qty) as total_qty 
        FROM stock s LEFT JOIN products p ON s.sku = p.sku 
        GROUP BY s.sku, s.warehouse HAVING total_qty > 0
    """, conn)
    
    st.markdown("#### 📦 現有庫存彙總")
    st.dataframe(df_stock, use_container_width=True)
    
    st.markdown("#### 📜 異動流水帳")
    query = """SELECT h.date, h.doc_type, h.doc_no, h.sku, p.name, h.warehouse, h.qty, h.unit_cost, h.cost, h.user 
               FROM history h LEFT JOIN products p ON h.sku = p.sku ORDER BY h.id DESC LIMIT 100"""
    df_h = pd.read_sql(query, conn)
    
    if not is_manager:
        df_h = df_h.drop(columns=['unit_cost', 'cost'])
    
    st.dataframe(df_h, use_container_width=True)
    conn.close()

import streamlit as st
import pandas as pd
import sqlite3
from datetime import date, datetime, timedelta
import os
import time
import io
import re
import uuid

# ==========================================
# 1. 系統設定
# ==========================================
PAGE_TITLE = "製造庫存系統 (批次管理版)"
DB_FILE = "inventory_system_batch.db"
ADMIN_PASSWORD = "8888"

# 固定選項
WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]
CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品"]
SERIES = ["原料", "半成品", "成品", "包材"]
KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]
SHIPPING_METHODS = ["7-11", "全家", "萊爾富", "OK", "郵局", "順豐", "黑貓", "賣家宅配", "自取", "其他"]
DEFAULT_REASONS = ["盤點差異", "報廢", "樣品借出", "系統修正", "其他"]

# ==========================================
# 2. 資料庫核心 (SQLite)
# ==========================================

def get_connection():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    return conn

def init_db():
    conn = get_connection()
    c = conn.cursor()
    
    # 1. 商品主檔
    c.execute('''
        CREATE TABLE IF NOT EXISTS products (
            sku TEXT PRIMARY KEY,
            name TEXT,
            category TEXT,
            series TEXT,
            spec TEXT
        )
    ''')
    
    # 2. 庫存表 (批次管理)
    c.execute('''
        CREATE TABLE IF NOT EXISTS stock (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT,
            warehouse TEXT,
            batch_id TEXT,
            supplier TEXT,
            unit_cost REAL,
            qty REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 3. 流水帳
    c.execute('''
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_type TEXT,
            doc_no TEXT,
            date TEXT,
            sku TEXT,
            warehouse TEXT,
            qty REAL,
            user TEXT,
            note TEXT,
            supplier TEXT,
            unit_cost REAL,
            cost REAL, 
            shipping_method TEXT,
            tracking_no TEXT,
            shipping_fee REAL,
            batch_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def reset_db():
    conn = get_connection()
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS products")
    c.execute("DROP TABLE IF EXISTS stock")
    c.execute("DROP TABLE IF EXISTS history")
    conn.commit()
    conn.close()
    init_db()

# --- 資料操作函式 ---

def add_product(sku, name, category, series, spec):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("INSERT INTO products (sku, name, category, series, spec) VALUES (?, ?, ?, ?, ?)",
                  (sku, name, category, series, spec))
        conn.commit()
        return True, "成功"
    except sqlite3.IntegrityError:
        return False, "貨號已存在，無法重複建立"
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def get_all_products():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    return df

def get_stock_overview():
    conn = get_connection()
    df_prod = pd.read_sql("SELECT * FROM products", conn)
    query_stock = """
        SELECT sku, warehouse, SUM(qty) as qty 
        FROM stock 
        GROUP BY sku, warehouse
    """
    df_stock = pd.read_sql(query_stock, conn)
    conn.close()
    
    if df_prod.empty: return pd.DataFrame()
    
    if df_stock.empty:
        result = df_prod.copy()
        for wh in WAREHOUSES: result[wh] = 0.0
        result['總庫存'] = 0.0
        return result

    pivot = df_stock.pivot(index='sku', columns='warehouse', values='qty').fillna(0)
    for wh in WAREHOUSES:
        if wh not in pivot.columns: pivot[wh] = 0.0
            
    pivot['總庫存'] = pivot[WAREHOUSES].sum(axis=1)
    result = pd.merge(df_prod, pivot, on='sku', how='left').fillna(0)
    
    cols = ['sku', 'series', 'category', 'name', 'spec', '總庫存'] + WAREHOUSES
    final_cols = [c for c in cols if c in result.columns]
    
    return result[final_cols]

def get_batch_options(warehouse_filter=None, sku_filter=None):
    conn = get_connection()
    query = """
        SELECT s.id, s.sku, p.name, s.supplier, s.unit_cost, s.qty, s.batch_id, s.warehouse
        FROM stock s
        LEFT JOIN products p ON s.sku = p.sku
        WHERE s.qty > 0
    """
    params = []
    if warehouse_filter:
        query += " AND s.warehouse = ?"
        params.append(warehouse_filter)
    if sku_filter:
        query += " AND s.sku = ?"
        params.append(sku_filter)
        
    query += " ORDER BY s.created_at ASC"
    
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    
    if df.empty:
        return []
    
    options = []
    for _, row in df.iterrows():
        label = f"【{row['warehouse']}】{row['sku']} | {row['name']} | 廠商:{row['supplier']} | 單價:${row['unit_cost']:,.0f} | 餘量:{row['qty']} | ({row['batch_id']})"
        value = row['id']
        options.append((label, value, row['sku'], row['qty'], row['unit_cost'], row['supplier'], row['batch_id']))
    return options

def get_distinct_suppliers():
    conn = get_connection()
    try:
        df1 = pd.read_sql("SELECT DISTINCT supplier FROM stock WHERE supplier != ''", conn)
        df2 = pd.read_sql("SELECT DISTINCT supplier FROM history WHERE supplier != ''", conn)
        suppliers = sorted(list(set(df1['supplier'].tolist() + df2['supplier'].tolist())))
        return suppliers
    except:
        return []
    finally:
        conn.close()

def get_distinct_reasons():
    conn = get_connection()
    try:
        df = pd.read_sql("SELECT DISTINCT note FROM history WHERE doc_type LIKE '庫存調整%' AND note != ''", conn)
        reasons = df['note'].tolist()
        return sorted(list(set(DEFAULT_REASONS + reasons)))
    except:
        return DEFAULT_REASONS
    finally:
        conn.close()

def add_transaction_in(date_str, sku, wh, qty, user, note, supplier="", unit_cost=0, doc_type="進貨"):
    conn = get_connection()
    c = conn.cursor()
    try:
        # 檢查該 SKU 是否存在於 products 主檔中，若不存在則自動建立一個簡單主檔，避免 Foreign Key 關聯問題
        c.execute("SELECT sku FROM products WHERE sku=?", (sku,))
        if not c.fetchone():
            c.execute("INSERT INTO products (sku, name, category, series) VALUES (?, ?, ?, ?)", (sku, "未定義商品", "未分類", "未分類"))

        timestamp = int(time.time() * 1000) # 使用毫秒級增加唯一性
        batch_id = f"B{date_str.replace('-','')}-{uuid.uuid4().hex[:4].upper()}"
        
        c.execute('''
            INSERT INTO stock (sku, warehouse, batch_id, supplier, unit_cost, qty)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (sku, wh, batch_id, supplier, unit_cost, qty))
        
        doc_prefix = {"進貨": "IN", "期初建檔": "OPEN", "製造入庫": "PD", "庫存調整(加)": "ADJ+"}.get(doc_type, "DOC")
        doc_no = f"{doc_prefix}-{timestamp}"
        total_cost = qty * unit_cost
        
        c.execute('''
            INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, supplier, unit_cost, cost, batch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (doc_type, doc_no, date_str, sku, wh, qty, user, note, supplier, unit_cost, total_cost, batch_id))
        
        conn.commit()
        return True
    except Exception as e:
        print(f"Error in add_transaction_in: {e}")
        return False
    finally:
        conn.close()

def add_transaction_out(date_str, stock_id, qty, user, note, doc_type="銷售出貨", shipping_method="", tracking_no="", shipping_fee=0):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("SELECT sku, warehouse, batch_id, supplier, unit_cost, qty FROM stock WHERE id=?", (stock_id,))
        row = c.fetchone()
        if not row:
            st.error("找不到該批次庫存！")
            return False
        
        sku, wh, batch_id, supplier, unit_cost, current_qty = row
        
        if current_qty < qty:
            st.error(f"庫存不足！該批次只剩 {current_qty}，您試圖扣除 {qty}。")
            return False
            
        new_qty = current_qty - qty
        c.execute("UPDATE stock SET qty=? WHERE id=?", (new_qty, stock_id))
        
        timestamp = int(time.time())
        doc_prefix = {"銷售出貨": "OUT", "製造領料": "MO", "庫存調整(減)": "ADJ-"}.get(doc_type, "DOC")
        doc_no = f"{doc_prefix}-{timestamp}"
        total_cost = qty * unit_cost
        
        c.execute('''
            INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, supplier, unit_cost, cost, shipping_method, tracking_no, shipping_fee, batch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (doc_type, doc_no, date_str, sku, wh, qty, user, note, supplier, unit_cost, total_cost, shipping_method, tracking_no, shipping_fee, batch_id))
        
        conn.commit()
        return True
    except Exception as e:
        st.error(f"出貨失敗: {e}")
        return False
    finally:
        conn.close()

def process_batch_stock_update(file_obj, default_wh):
    """
    優化後的匯入邏輯：支援多倉庫橫向欄位與單一數量縱向欄位
    """
    try:
        # 1. 讀取 Excel/CSV
        df = pd.read_csv(file_obj) if file_obj.name.endswith('.csv') else pd.read_excel(file_obj)
        df.columns = [str(c).strip() for c in df.columns]
        
        # 2. 欄位正規化映射
        rename_map = {}
        for c in df.columns:
            if c in ['SKU', '編號', '料號', '貨號']: rename_map[c] = '貨號'
            if c in ['數量', '盤點數量', 'Qty']: rename_map[c] = '數量'
            if c in ['成本', '單價', 'Cost']: rename_map[c] = '成本'
            if c in ['廠商', 'Supplier']: rename_map[c] = '廠商'
            if c in ['倉庫', 'Warehouse']: rename_map[c] = '倉庫'
        df = df.rename(columns=rename_map)
        
        if '貨號' not in df.columns:
            return False, "Excel 必須包含 `貨號` (或 SKU) 欄位"

        # 3. 檢查是否存在與系統定義一致的倉庫欄位 (橫向模式)
        found_wh_cols = [c for c in WAREHOUSES if c in df.columns]
        
        update_count = 0
        today_str = str(date.today())

        for _, row in df.iterrows():
            sku = str(row['貨號']).strip()
            if not sku or sku == 'nan': continue
            
            # 讀取成本與廠商
            cost = 0.0
            if '成本' in df.columns:
                try: cost = float(row['成本'])
                except: cost = 0.0
            
            supp = str(row['廠商']) if '廠商' in df.columns and pd.notna(row['廠商']) else ""

            # 模式 A: 多欄位倉庫模式 (橫向總表)
            if found_wh_cols:
                for wh_name in found_wh_cols:
                    try:
                        val = row[wh_name]
                        if pd.notna(val) and float(val) > 0:
                            qty = float(val)
                            add_transaction_in(today_str, sku, wh_name, qty, "系統匯入", "總表批量匯入", supplier=supp, unit_cost=cost, doc_type="期初建檔")
                            update_count += 1
                    except: continue
            
            # 模式 B: 單一數量欄位模式 (縱向明細)
            elif '數量' in df.columns:
                try:
                    qty = float(row['數量'])
                    wh = str(row['倉庫']).strip() if '倉庫' in df.columns and str(row['倉庫']).strip() in WAREHOUSES else default_wh
                    add_transaction_in(today_str, sku, wh, qty, "系統匯入", "清單批量匯入", supplier=supp, unit_cost=cost, doc_type="期初建檔")
                    update_count += 1
                except: continue

        if update_count == 0:
            return False, "未偵測到有效數據，請確認欄位包含『貨號』以及『數量』或『倉庫名稱』"
            
        return True, f"✅ 已成功建立 {update_count} 筆批次庫存記錄"
    except Exception as e: 
        return False, str(e)

def get_history(is_manager=False, doc_type_filter=None):
    conn = get_connection()
    query = """
    SELECT h.date as '日期', h.doc_type as '類型', h.doc_no as '單號',
           p.name as '品名', h.sku as '貨號', h.warehouse as '倉庫', 
           h.qty as '數量', h.supplier as '廠商', h.batch_id as '批號',
           h.unit_cost as '單價/成本', h.cost as '總金額',
           h.user as '經手人', h.note as '備註'
    FROM history h
    LEFT JOIN products p ON h.sku = p.sku
    WHERE 1=1
    """
    params = []
    if doc_type_filter:
        query += " AND h.doc_type = ?"
        params.append(doc_type_filter)
    
    query += " ORDER BY h.id DESC LIMIT 50"
    
    try:
        df = pd.read_sql(query, conn, params=params)
        if not is_manager:
            df = df.drop(columns=['單價/成本', '總金額'], errors='ignore')
    except:
        df = pd.DataFrame()
    conn.close()
    return df

def to_excel_download(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# ==========================================
# 3. 初始化
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="🏭")
init_db()

# ==========================================
# 4. 介面邏輯
# ==========================================

st.title(f"🏭 {PAGE_TITLE}")

is_manager = False 
with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", ["📦 商品建檔", "📥 進貨作業", "🚚 出貨作業", "🔨 製造作業", "⚖️ 庫存盤點", "📊 報表查詢"])
    st.divider()
    with st.expander("🔐 主管權限"):
        pwd = st.text_input("密碼", type="password")
        if pwd == ADMIN_PASSWORD:
            is_manager = True
            st.success("已登入主管模式")
    st.divider()
    if st.button("🔴 初始化/重置資料庫"):
        reset_db()
        st.cache_data.clear()
        st.success("資料庫已重置！")
        time.sleep(1); st.rerun()

# 1. 商品建檔
if page == "📦 商品建檔":
    st.subheader("📦 商品資料維護")
    t1, t2, t3 = st.tabs(["✨ 單筆建檔", "📂 匯入商品資料", "📥 匯入庫存/期初"])
    
    with t1:
        with st.form("add"):
            c1, c2 = st.columns(2)
            sku = c1.text_input("貨號 (SKU)")
            name = c2.text_input("品名")
            cat = st.selectbox("分類", CATEGORIES)
            ser = st.selectbox("系列", SERIES)
            if st.form_submit_button("新增"):
                if sku and name:
                    ok, msg = add_product(sku, name, cat, ser, "")
                    if ok: st.success("成功"); time.sleep(1); st.rerun()
                    else: st.error(msg)
                else: st.warning("請填寫貨號與品名")
    
    with t2:
        up = st.file_uploader("上傳商品清單 (xlsx/csv)", key="up_prod")
        if up and st.button("執行匯入商品"):
            try:
                df = pd.read_csv(up) if up.name.endswith('.csv') else pd.read_excel(up)
                count = 0
                for _, r in df.iterrows():
                    add_product(str(r.iloc[0]), str(r.iloc[1]), "未分類", "未分類", "")
                    count += 1
                st.success(f"匯入 {count} 筆")
            except Exception as e: st.error(f"錯誤: {e}")

    with t3:
        st.markdown("""
        ### 📥 批量匯入庫存
        系統支援以下兩種類型的 Excel 格式：
        1. **多倉總表**：包含 `貨號` 以及各倉庫欄位 (`Wen`, `千畇`, `James`, `Imeng`)。
        2. **單一清單**：包含 `貨號`、`數量`、`倉庫` (若無倉庫欄位則使用下方預設倉)。
        """)
        wh = st.selectbox("預設入庫倉庫", WAREHOUSES)
        up2 = st.file_uploader("上傳庫存表 (xlsx/csv)", key="up_stock")
        if up2 and st.button("開始匯入庫存"):
            ok, msg = process_batch_stock_update(up2, wh)
            if ok: st.success(msg)
            else: st.error(msg)
    
    st.divider()
    st.dataframe(get_all_products(), use_container_width=True)

# 2. 進貨作業
elif page == "📥 進貨作業":
    st.subheader("📥 進貨 (建立新批次)")
    prods = get_all_products()
    if not prods.empty:
        prod_opts = [f"{r['sku']} | {r['name']}" for _, r in prods.iterrows()]
        
        with st.form("in"):
            c1, c2 = st.columns([2,1])
            sel_prod = c1.selectbox("選擇商品", prod_opts)
            wh = c2.selectbox("入庫倉庫", WAREHOUSES)
            
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", min_value=0.0, step=1.0)
            date_val = c4.date_input("日期", date.today())
            
            supp_opts = [""] + get_distinct_suppliers() + ["➕ 新增廠商"]
            sel_supp = st.selectbox("廠商", supp_opts)
            if sel_supp == "➕ 新增廠商":
                final_supp = st.text_input("輸入新廠商名稱")
            else:
                final_supp = sel_supp
            
            cost = 0.0
            if is_manager:
                cost = st.number_input("進貨單價 (成本)", 0.0)
            
            user = st.selectbox("經手人", KEYERS)
            note = st.text_input("備註")
            
            if st.form_submit_button("確認進貨"):
                target_sku = sel_prod.split(" | ")[0]
                if qty <= 0:
                    st.error("數量必須大於 0")
                elif add_transaction_in(str(date_val), target_sku, wh, qty, user, note, final_supp, cost):
                    st.success("進貨成功 (已建立獨立批號)")
                    time.sleep(0.5); st.rerun()
        
        st.divider()
        st.dataframe(get_history(is_manager, "進貨"), use_container_width=True)
    else:
        st.warning("請先至「商品建檔」建立商品")

# 3. 出貨作業
elif page == "🚚 出貨作業":
    st.subheader("🚚 銷售出貨 (指定批次)")
    
    c_filter1, c_filter2 = st.columns(2)
    wh_filter = c_filter1.selectbox("篩選倉庫", WAREHOUSES)
    
    batch_opts = get_batch_options(warehouse_filter=wh_filter)
    
    if not batch_opts:
        st.warning("該倉庫目前無庫存")
    else:
        with st.form("out"):
            selected_idx = st.selectbox("選擇出貨批次 (庫存)", range(len(batch_opts)), format_func=lambda x: batch_opts[x][0])
            selected_data = batch_opts[selected_idx]
            stock_id = selected_data[1] 
            max_qty = selected_data[3]
            
            c1, c2 = st.columns(2)
            qty = c1.number_input(f"出貨數量 (最大 {max_qty})", 0.0, max_qty, 1.0)
            date_val = c2.date_input("日期", date.today())
            
            st.divider()
            c3, c4, c5 = st.columns(3)
            ship = c3.selectbox("貨運", SHIPPING_METHODS)
            fee = c4.number_input("運費", 0)
            track = c5.text_input("單號")
            
            user = st.selectbox("經手人", KEYERS)
            note = st.text_input("備註")
            
            if st.form_submit_button("確認出貨"):
                if qty <= 0:
                    st.error("數量必須大於 0")
                elif add_transaction_out(str(date_val), stock_id, qty, user, note, "銷售出貨", ship, track, fee):
                    st.success("出貨成功 (已扣除該批次)")
                    time.sleep(0.5); st.rerun()
                    
        st.divider()
        st.dataframe(get_history(is_manager, "銷售出貨"), use_container_width=True)

# 4. 製造作業
elif page == "🔨 製造作業":
    st.subheader("🔨 生產管理")
    t1, t2 = st.tabs(["領料 (扣批次)", "完工 (產新批次)"])
    
    with t1:
        wh_filter = st.selectbox("領料倉庫", WAREHOUSES, key="mo_wh")
        batch_opts = get_batch_options(warehouse_filter=wh_filter)
        if batch_opts:
            with st.form("mo_out_form"):
                idx = st.selectbox("選擇原料批次", range(len(batch_opts)), format_func=lambda x: batch_opts[x][0])
                s_data = batch_opts[idx]
                qty = st.number_input("領用量", 0.0, s_data[3])
                if st.form_submit_button("確認領料"):
                    if qty > 0:
                        if add_transaction_out(str(date.today()), s_data[1], qty, "工廠", "生產領料", "製造領料"):
                            st.success("領料完成"); time.sleep(0.5); st.rerun()
                    else: st.error("數量必須大於 0")
        else: st.warning("無庫存可領")

    with t2:
        prods = get_all_products()
        if not prods.empty:
            p_opts = [f"{r['sku']} | {r['name']}" for _, r in prods.iterrows()]
            with st.form("mo_in_form"):
                sel = st.selectbox("產出成品", p_opts)
                wh = st.selectbox("入庫倉", WAREHOUSES)
                qty = st.number_input("產出量", min_value=0.0)
                cost = 0.0
                if is_manager: cost = st.number_input("成品單位成本", 0.0)
                if st.form_submit_button("完工入庫"):
                    sku = sel.split(" | ")[0]
                    if qty > 0:
                        if add_transaction_in(str(date.today()), sku, wh, qty, "工廠", "生產完工", "自製", cost, "製造入庫"):
                            st.success("完工入庫"); time.sleep(0.5); st.rerun()
                    else: st.error("數量必須大於 0")

# 5. 庫存盤點
elif page == "⚖️ 庫存盤點":
    st.subheader("⚖️ 庫存調整")
    t1, t2 = st.tabs(["單筆調整 (針對批次)", "批量盤點說明"])
    
    with t1:
        wh_filter = st.selectbox("調整倉庫", WAREHOUSES, key="adj_wh")
        batch_opts = get_batch_options(warehouse_filter=wh_filter)
        if batch_opts:
            with st.form("adj_form"):
                idx = st.selectbox("選擇調整批次", range(len(batch_opts)), format_func=lambda x: batch_opts[x][0])
                s_data = batch_opts[idx]
                
                action = st.radio("動作", ["減少 (-)", "增加 (+)"])
                
                if action == "增加 (+)":
                    st.info("批次管理模式下，發現盤盈(增加)建議至「進貨作業」建立新批次。")
                else:
                    qty = st.number_input("減少數量", 0.0, s_data[3])
                    reason = st.selectbox("原因", get_distinct_reasons())
                    if st.form_submit_button("執行調整"):
                        if qty > 0:
                            if add_transaction_out(str(date.today()), s_data[1], qty, "管理員", reason, "庫存調整(減)"):
                                st.success("調整完成"); time.sleep(0.5); st.rerun()
                        else: st.error("數量必須大於 0")
        else: st.info("無庫存可調整")

    with t2:
        st.info("批量盤點請利用「商品建檔 > 匯入期初庫存」功能。匯入時系統會自動將每一行視為新的批次進入系統。")

    st.divider()
    st.markdown("### 📦 庫存總表 (加總)")
    st.dataframe(get_stock_overview(), use_container_width=True)

# 6. 報表
elif page == "📊 報表查詢":
    st.subheader("📊 報表中心")
    if is_manager: st.success("🔓 主管模式：顯示成本單價")
    
    tab_stock, tab_history = st.tabs(["現有庫存總表", "進出貨歷史流水帳"])
    
    with tab_stock:
        df_stock = get_stock_overview()
        st.dataframe(df_stock, use_container_width=True)
        if not df_stock.empty:
            st.download_button("📥 下載庫存總表", to_excel_download(df_stock), "stock_report.xlsx")
            
    with tab_history:
        type_filter = st.selectbox("篩選單據類型", ["全部", "進貨", "銷售出貨", "製造領料", "製造入庫", "期初建檔", "庫存調整(減)"])
        filter_val = None if type_filter == "全部" else type_filter
        df_hist = get_history(is_manager, filter_val)
        st.dataframe(df_hist, use_container_width=True)
        if not df_hist.empty:
            st.download_button("📥 下載流水帳", to_excel_download(df_hist), "history_report.xlsx")

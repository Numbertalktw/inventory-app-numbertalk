import streamlit as st
import pandas as pd
import sqlite3
from datetime import date, datetime
import io
import time

# ==========================================
# 1. 系統設定
# ==========================================
PAGE_TITLE = "製造庫存系統 (DB專業版)"
DB_FILE = "inventory_system.db"
ADMIN_PASSWORD = "8888"

# 固定選項 (4個倉庫)
WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]
CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品"]
SERIES = ["原料", "半成品", "成品", "包材"]

# ==========================================
# 2. 資料庫核心 (SQLite)
# ==========================================

def get_connection():
    """建立資料庫連線"""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    return conn

def init_db():
    """初始化資料庫表格"""
    conn = get_connection()
    c = conn.cursor()
    
    # 1. 商品主檔 (Products)
    c.execute('''
        CREATE TABLE IF NOT EXISTS products (
            sku TEXT PRIMARY KEY,
            name TEXT,
            category TEXT,
            series TEXT,
            spec TEXT
        )
    ''')
    
    # 2. 庫存表 (Stock) - 紀錄每個倉庫的每個商品數量
    c.execute('''
        CREATE TABLE IF NOT EXISTS stock (
            sku TEXT,
            warehouse TEXT,
            qty REAL,
            PRIMARY KEY (sku, warehouse)
        )
    ''')
    
    # 3. 流水帳 (History)
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
            cost REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def reset_db():
    """強制重置資料庫"""
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
        # 插入商品
        c.execute("INSERT INTO products (sku, name, category, series, spec) VALUES (?, ?, ?, ?, ?)",
                  (sku, name, category, series, spec))
        # 初始化各倉庫庫存為 0 (確保報表有數據)
        for wh in WAREHOUSES:
            c.execute("INSERT OR IGNORE INTO stock (sku, warehouse, qty) VALUES (?, ?, 0)", (sku, wh))
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
    """取得庫存總表 (樞紐分析型式，顯示4個倉庫)"""
    conn = get_connection()
    # 讀取商品與庫存
    df_prod = pd.read_sql("SELECT * FROM products", conn)
    df_stock = pd.read_sql("SELECT * FROM stock", conn)
    conn.close()
    
    if df_prod.empty: return pd.DataFrame()
    
    # 如果庫存表是空的，先補 0
    if df_stock.empty:
        result = df_prod.copy()
        for wh in WAREHOUSES: result[wh] = 0.0
        result['總庫存'] = 0.0
        return result

    # 轉置庫存表：將倉庫 (warehouse) 變成欄位
    pivot = df_stock.pivot(index='sku', columns='warehouse', values='qty').fillna(0)
    
    # 確保 4 個倉庫欄位都存在 (即使某倉庫沒庫存也要顯示)
    for wh in WAREHOUSES:
        if wh not in pivot.columns:
            pivot[wh] = 0.0
            
    # 計算總庫存
    pivot['總庫存'] = pivot[WAREHOUSES].sum(axis=1)
    
    # 合併商品資料 (Left Join)
    result = pd.merge(df_prod, pivot, on='sku', how='left').fillna(0)
    
    # 整理欄位順序
    cols = ['sku', 'series', 'category', 'name', 'spec', '總庫存'] + WAREHOUSES
    # 只取存在的欄位
    final_cols = [c for c in cols if c in result.columns]
    
    return result[final_cols]

def add_transaction(doc_type, date_str, sku, wh, qty, user, note, cost=0):
    """新增交易並更新庫存"""
    conn = get_connection()
    c = conn.cursor()
    try:
        # 1. 寫入流水帳
        doc_prefix = {
            "進貨": "IN", "銷售出貨": "OUT", "製造領料": "MO", "製造入庫": "PD",
            "庫存調整(加)": "ADJ+", "庫存調整(減)": "ADJ-"
        }.get(doc_type, "DOC")
        
        doc_no = f"{doc_prefix}-{int(time.time())}"
        
        c.execute('''
            INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, cost)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (doc_type, doc_no, date_str, sku, wh, qty, user, note, cost))
        
        # 2. 更新庫存 (增減邏輯)
        factor = 1
        if doc_type in ['銷售出貨', '製造領料', '庫存調整(減)']:
            factor = -1
        
        change_qty = qty * factor
        
        # 更新該倉庫庫存 (Upsert: 若存在則更新，若不存在則插入)
        c.execute('''
            INSERT INTO stock (sku, warehouse, qty) VALUES (?, ?, ?)
            ON CONFLICT(sku, warehouse) DO UPDATE SET qty = qty + ?
        ''', (sku, wh, change_qty, change_qty))
        
        conn.commit()
        return True
    except Exception as e:
        st.error(f"交易失敗: {e}")
        return False
    finally:
        conn.close()

def get_history():
    conn = get_connection()
    # 關聯 products 表以取得品名
    query = """
    SELECT h.date, h.doc_type, h.doc_no, 
           p.series, p.category, p.name, p.spec, 
           h.sku, h.warehouse, h.qty, h.user, h.note, h.cost
    FROM history h
    LEFT JOIN products p ON h.sku = p.sku
    ORDER BY h.id DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ==========================================
# 3. 初始化
# ==========================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="🏭")
init_db() # 確保資料庫存在

# ==========================================
# 4. 介面邏輯
# ==========================================

st.title(f"🏭 {PAGE_TITLE}")

with st.sidebar:
    st.header("功能選單")
    page = st.radio("前往", [
        "📦 商品管理 (建檔/匯入)", 
        "📥 進貨作業", 
        "🚚 出貨作業", 
        "🔨 製造作業",
        "⚖️ 庫存盤點",
        "📊 報表查詢"
    ])
    
    st.divider()
    # 🔴 重置按鈕
    if st.button("🔴 初始化/重置資料庫"):
        reset_db()
        st.cache_data.clear()
        st.success("資料庫已重置！請重新建檔。")
        time.sleep(1)
        st.rerun()

# ------------------------------------------------------------------
# 1. 商品管理
# ------------------------------------------------------------------
if page == "📦 商品管理 (建檔/匯入)":
    st.subheader("📦 商品資料維護")
    
    tab1, tab2 = st.tabs(["✨ 單筆建檔", "📂 Excel 匯入"])
    
    with tab1:
        with st.form("add_prod"):
            c1, c2 = st.columns(2)
            sku = c1.text_input("貨號 (SKU) *必填", placeholder="例如: ST-001")
            name = c2.text_input("品名 *必填")
            
            c3, c4, c5 = st.columns(3)
            cat = c3.selectbox("分類", CATEGORIES)
            ser = c4.selectbox("系列", SERIES)
            spec = c5.text_input("規格/尺寸")
            
            if st.form_submit_button("新增商品"):
                if sku and name:
                    success, msg = add_product(sku, name, cat, ser, spec)
                    if success: st.success(f"商品 {name} 建立成功！"); time.sleep(1); st.rerun()
                    else: st.error(msg)
                else:
                    st.error("貨號與品名為必填！")

    with tab2:
        st.info("請上傳 Excel。系統會自動對應 `貨號`, `品名`, `分類`, `系列`, `規格` 欄位。")
        up = st.file_uploader("上傳商品清單", type=['xlsx', 'csv'])
        if up and st.button("開始匯入"):
            try:
                df = pd.read_csv(up) if up.name.endswith('.csv') else pd.read_excel(up)
                
                # 欄位模糊對應
                df.columns = [str(c).strip() for c in df.columns]
                rename_map = {}
                for c in df.columns:
                    if c in ['SKU', '編號', '料號']: rename_map[c] = '貨號'
                    if c in ['名稱', '商品名稱']: rename_map[c] = '品名'
                    if c in ['類別', 'Category']: rename_map[c] = '分類'
                    if c in ['Series']: rename_map[c] = '系列'
                    if c in ['尺寸', 'Spec']: rename_map[c] = '規格'
                df = df.rename(columns=rename_map)
                
                count = 0
                if '貨號' in df.columns and '品名' in df.columns:
                    for _, row in df.iterrows():
                        s = str(row.get('貨號', '')).strip()
                        n = str(row.get('品名', '')).strip()
                        if s and n:
                            add_product(
                                s, n, 
                                str(row.get('分類', '未分類')), 
                                str(row.get('系列', '未分類')), 
                                str(row.get('規格', ''))
                            )
                            count += 1
                    st.success(f"成功匯入 {count} 筆商品！")
                    time.sleep(1); st.rerun()
                else:
                    st.error("Excel 缺少 `貨號` 或 `品名` 欄位")
            except Exception as e:
                st.error(f"匯入失敗: {e}")

    # 顯示目前商品
    st.divider()
    st.markdown("#### 目前商品清單")
    df_prod = get_all_products()
    if not df_prod.empty:
        st.dataframe(df_prod, use_container_width=True)
    else:
        st.info("尚無商品資料")

# ------------------------------------------------------------------
# 2. 進貨作業
# ------------------------------------------------------------------
elif page == "📥 進貨作業":
    st.subheader("📥 進貨入庫")
    
    prods = get_all_products()
    if prods.empty:
        st.warning("請先建立商品資料！")
    else:
        # 製作選單：貨號 | 品名
        prods['label'] = prods['sku'] + " | " + prods['name']
        
        with st.form("in_stock"):
            c1, c2 = st.columns([2, 1])
            sel_prod = c1.selectbox("選擇商品", prods['label'])
            wh = c2.selectbox("入庫倉庫", WAREHOUSES, index=0)
            
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", min_value=1, value=1)
            date_val = c4.date_input("日期", date.today())
            
            user = st.text_input("經手人", "User")
            note = st.text_input("備註")
            
            if st.form_submit_button("確認進貨", type="primary"):
                target_sku = sel_prod.split(" | ")[0]
                if add_transaction("進貨", str(date_val), target_sku, wh, qty, user, note):
                    st.success("進貨成功！")
                    time.sleep(1); st.rerun()

# ------------------------------------------------------------------
# 3. 出貨作業
# ------------------------------------------------------------------
elif page == "🚚 出貨作業":
    st.subheader("🚚 銷售出貨")
    prods = get_all_products()
    if prods.empty:
        st.warning("無商品資料")
    else:
        prods['label'] = prods['sku'] + " | " + prods['name']
        with st.form("out_stock"):
            c1, c2 = st.columns([2, 1])
            sel_prod = c1.selectbox("選擇商品", prods['label'])
            wh = c2.selectbox("出貨倉庫", WAREHOUSES, index=2)
            
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", min_value=1, value=1)
            date_val = c4.date_input("日期", date.today())
            
            note = st.text_input("訂單編號 / 備註")
            
            if st.form_submit_button("確認出貨", type="primary"):
                target_sku = sel_prod.split(" | ")[0]
                # 檢查庫存 (選擇性)
                # 這裡直接允許扣成負數，符合工廠彈性
                if add_transaction("銷售出貨", str(date_val), target_sku, wh, qty, "User", note):
                    st.success("出貨成功！")
                    time.sleep(1); st.rerun()

# ------------------------------------------------------------------
# 4. 製造作業
# ------------------------------------------------------------------
elif page == "🔨 製造作業":
    st.subheader("🔨 生產管理")
    prods = get_all_products()
    if not prods.empty:
        prods['label'] = prods['sku'] + " | " + prods['name']
        
        t1, t2 = st.tabs(["領料 (扣庫存)", "完工 (加庫存)"])
        
        with t1:
            with st.form("mo_out"):
                sel = st.selectbox("原料", prods['label'], key='m1')
                wh = st.selectbox("領料倉", WAREHOUSES, key='m2')
                qty = st.number_input("領用量", 1, key='m3')
                if st.form_submit_button("確認領料"):
                    sku = sel.split(" | ")[0]
                    add_transaction("製造領料", str(date.today()), sku, wh, qty, "工廠", "領料")
                    st.success("已扣除原料庫存")
                    time.sleep(1); st.rerun()

        with t2:
             with st.form("mo_in"):
                sel = st.selectbox("成品", prods['label'], key='p1')
                wh = st.selectbox("入庫倉", WAREHOUSES, key='p2')
                qty = st.number_input("產出量", 1, key='p3')
                if st.form_submit_button("完工入庫"):
                    sku = sel.split(" | ")[0]
                    add_transaction("製造入庫", str(date.today()), sku, wh, qty, "工廠", "完工")
                    st.success("成品已入庫")
                    time.sleep(1); st.rerun()

# ------------------------------------------------------------------
# 5. 庫存盤點
# ------------------------------------------------------------------
elif page == "⚖️ 庫存盤點":
    st.subheader("⚖️ 庫存調整")
    
    # 顯示目前庫存
    df_stock = get_stock_overview()
    if not df_stock.empty:
        st.dataframe(df_stock, use_container_width=True)
    
    st.divider()
    st.markdown("### 新增調整單")
    
    prods = get_all_products()
    if not prods.empty:
        prods['label'] = prods['sku'] + " | " + prods['name']
        with st.form("adj"):
            c1, c2 = st.columns(2)
            sel = c1.selectbox("商品", prods['label'])
            wh = c2.selectbox("倉庫", WAREHOUSES)
            
            c3, c4 = st.columns(2)
            action = c3.radio("動作", ["增加 (+)", "減少 (-)"], horizontal=True)
            qty = c4.number_input("調整數量", 1)
            reason = st.text_input("原因", "盤點差異")
            
            if st.form_submit_button("提交調整"):
                sku = sel.split(" | ")[0]
                type_name = "庫存調整(加)" if action == "增加 (+)" else "庫存調整(減)"
                add_transaction(type_name, str(date.today()), sku, wh, qty, "管理員", reason)
                st.success("調整完成！")
                time.sleep(1)
                st.rerun()

# ------------------------------------------------------------------
# 6. 報表查詢
# ------------------------------------------------------------------
elif page == "📊 報表查詢":
    st.subheader("📊 數據報表中心")
    
    tab1, tab2 = st.tabs(["📦 即時庫存表", "📜 歷史流水帳"])
    
    with tab1:
        df = get_stock_overview()
        st.dataframe(df, use_container_width=True)
        if not df.empty:
            # Excel 下載
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False)
            st.download_button("📥 下載庫存表", output.getvalue(), "Stock.xlsx")

    with tab2:
        df_hist = get_history()
        st.dataframe(df_hist, use_container_width=True)
        if not df_hist.empty:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_hist.to_excel(writer, index=False)
            st.download_button("📥 下載流水帳", output.getvalue(), "History.xlsx")

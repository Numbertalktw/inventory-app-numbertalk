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

# 固定選項
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
        c.execute("INSERT INTO products (sku, name, category, series, spec) VALUES (?, ?, ?, ?, ?)",
                  (sku, name, category, series, spec))
        # 初始化各倉庫庫存為 0
        for wh in WAREHOUSES:
            c.execute("INSERT OR IGNORE INTO stock (sku, warehouse, qty) VALUES (?, ?, 0)", (sku, wh))
        conn.commit()
        return True, "成功"
    except sqlite3.IntegrityError:
        return False, "貨號已存在"
    finally:
        conn.close()

def get_all_products():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    return df

def get_stock_overview():
    """取得庫存總表 (樞紐分析型式)"""
    conn = get_connection()
    # 讀取商品與庫存
    df_prod = pd.read_sql("SELECT * FROM products", conn)
    df_stock = pd.read_sql("SELECT * FROM stock", conn)
    conn.close()
    
    if df_prod.empty: return pd.DataFrame()
    
    # 轉置庫存表：將倉庫變成欄位
    if not df_stock.empty:
        pivot = df_stock.pivot(index='sku', columns='warehouse', values='qty').fillna(0)
        pivot['總庫存'] = pivot.sum(axis=1)
        # 合併商品資料
        result = df_prod.join(pivot, on='sku', how='left').fillna(0)
    else:
        result = df_prod
        for wh in WAREHOUSES: result[wh] = 0
        result['總庫存'] = 0
        
    return result

def add_transaction(doc_type, date_str, sku, wh, qty, user, note, cost=0):
    """新增交易並更新庫存"""
    conn = get_connection()
    c = conn.cursor()
    try:
        # 1. 寫入流水帳
        doc_no = f"{doc_type[0]}N-{int(time.time())}"
        c.execute('''
            INSERT INTO history (doc_type, doc_no, date, sku, warehouse, qty, user, note, cost)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (doc_type, doc_no, date_str, sku, wh, qty, user, note, cost))
        
        # 2. 更新庫存 (增減邏輯)
        # 進貨、製造入庫、盤點加 -> 增加
        # 出貨、領料、盤點減 -> 減少
        factor = 1
        if doc_type in ['銷售出貨', '製造領料', '庫存調整(減)']:
            factor = -1
        
        change_qty = qty * factor
        
        # 更新該倉庫庫存
        c.execute('''
            INSERT INTO stock (sku, warehouse, qty) VALUES (?, ?, ?)
            ON CONFLICT(sku, warehouse) DO UPDATE SET qty = qty + ?
        ''', (sku, wh, change_qty, change_qty))
        
        conn.commit()
        return True
    except Exception as e:
        return False
    finally:
        conn.close()

def get_history(filters=None):
    conn = get_connection()
    query = "SELECT * FROM history ORDER BY id DESC"
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
    if st.button("🔴 初始化/重置資料庫"):
        reset_db()
        st.success("資料庫已重置！所有資料已清空。")
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
            c3, c4 = st.columns(2)
            cat = c3.selectbox("分類", CATEGORIES)
            ser = c4.selectbox("系列", SERIES)
            spec = st.text_input("規格")
            
            if st.form_submit_button("新增商品"):
                if sku and name:
                    success, msg = add_product(sku, name, cat, ser, spec)
                    if success: st.success(f"商品 {name} 建立成功！"); time.sleep(1); st.rerun()
                    else: st.error(msg)
                else:
                    st.error("貨號與品名為必填！")

    with tab2:
        st.info("請上傳 Excel，必需欄位：`貨號`、`品名`。選填：`分類`、`系列`、`規格`。")
        up = st.file_uploader("上傳商品清單", type=['xlsx', 'csv'])
        if up and st.button("開始匯入"):
            try:
                df = pd.read_csv(up) if up.name.endswith('.csv') else pd.read_excel(up)
                # 欄位對應
                rename = {'SKU':'貨號', '名稱':'品名', '類別':'分類'}
                df = df.rename(columns=rename)
                
                count = 0
                for _, row in df.iterrows():
                    # 容錯處理
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
            except Exception as e:
                st.error(f"匯入失敗: {e}")

    # 顯示目前商品
    st.divider()
    st.caption("目前商品清單：")
    st.dataframe(get_all_products(), use_container_width=True)

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
            wh = c2.selectbox("入庫倉庫", WAREHOUSES)
            
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", min_value=1, value=1)
            date_val = c4.date_input("日期", date.today())
            
            user = st.text_input("經手人/Key單者", "User")
            note = st.text_input("備註")
            
            if st.form_submit_button("確認進貨", type="primary"):
                target_sku = sel_prod.split(" | ")[0]
                if add_transaction("進貨", str(date_val), target_sku, wh, qty, user, note):
                    st.success("進貨成功！")
                else:
                    st.error("系統錯誤")

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
            wh = c2.selectbox("出貨倉庫", WAREHOUSES)
            
            c3, c4 = st.columns(2)
            qty = c3.number_input("數量", min_value=1, value=1)
            date_val = c4.date_input("日期", date.today())
            
            note = st.text_input("訂單編號 / 備註")
            
            if st.form_submit_button("確認出貨", type="primary"):
                target_sku = sel_prod.split(" | ")[0]
                if add_transaction("銷售出貨", str(date_val), target_sku, wh, qty, "User", note):
                    st.success("出貨成功！")
                else:
                    st.error("系統錯誤")

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

        with t2:
             with st.form("mo_in"):
                sel = st.selectbox("成品", prods['label'], key='p1')
                wh = st.selectbox("入庫倉", WAREHOUSES, key='p2')
                qty = st.number_input("產出量", 1, key='p3')
                if st.form_submit_button("完工入庫"):
                    sku = sel.split(" | ")[0]
                    add_transaction("製造入庫", str(date.today()), sku, wh, qty, "工廠", "完工")
                    st.success("成品已入庫")

# ------------------------------------------------------------------
# 5. 庫存盤點
# ------------------------------------------------------------------
elif page == "⚖️ 庫存盤點":
    st.subheader("⚖️ 庫存調整")
    # 直接顯示目前的庫存表供參考
    df_stock = get_stock_overview()
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
            with pd.ExcelWriter(output) as writer:
                df.to_excel(writer, index=False)
            st.download_button("📥 下載庫存表", output.getvalue(), "Stock.xlsx")

    with tab2:
        df_hist = get_history()
        st.dataframe(df_hist, use_container_width=True)
        if not df_hist.empty:
            output = io.BytesIO()
            with pd.ExcelWriter(output) as writer:
                df_hist.to_excel(writer, index=False)
            st.download_button("📥 下載流水帳", output.getvalue(), "History.xlsx")

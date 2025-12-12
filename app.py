import streamlit as st
import pandas as pd
import sqlite3
import os
import io
import time
from datetime import date, datetime
from pandas.api.types import (
    is_numeric_dtype, is_datetime64_any_dtype, is_categorical_dtype, is_object_dtype
)

# ================================
# 1. 系統基本設定
# ================================
PAGE_TITLE = "製造庫存系統（V6.1 Stable）"

DB_FILE = "inventory_v6.db"
INVENTORY_CSV = "inventory_backup.csv"
HISTORY_CSV = "history_backup.csv"

WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]

HISTORY_COLUMNS = [
    '單據類型','單號','日期','系列','分類','品名','規格',
    '貨號','批號','倉庫','數量','Key單者','廠商','訂單單號',
    '出貨日期','貨號備註','運費','款項結清','工資','發票','備註','進貨總成本'
]

INVENTORY_COLUMNS = [
    '貨號','系列','分類','品名','規格','總庫存','均價',
    '庫存_Wen','庫存_千畇','庫存_James','庫存_Imeng'
]


# ================================
# 2. SQLite 初始化
# ================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory (
            貨號 TEXT PRIMARY KEY,
            系列 TEXT,
            分類 TEXT,
            品名 TEXT,
            規格 TEXT,
            總庫存 REAL,
            均價 REAL,
            庫存_Wen REAL,
            庫存_千畇 REAL,
            庫存_James REAL,
            庫存_Imeng REAL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            單據類型 TEXT,
            單號 TEXT,
            日期 TEXT,
            系列 TEXT,
            分類 TEXT,
            品名 TEXT,
            規格 TEXT,
            貨號 TEXT,
            批號 TEXT,
            倉庫 TEXT,
            數量 REAL,
            Key單者 TEXT,
            廠商 TEXT,
            訂單單號 TEXT,
            出貨日期 TEXT,
            貨號備註 TEXT,
            運費 REAL,
            款項結清 TEXT,
            工資 REAL,
            發票 TEXT,
            備註 TEXT,
            進貨總成本 REAL
        )
    """)

    conn.commit()
    conn.close()

init_db()


# ================================
# 3. DB 共用存取函式
# ================================
def load_inventory():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql("SELECT * FROM inventory", conn)
    conn.close()
    if df.empty:
        return pd.DataFrame(columns=INVENTORY_COLUMNS)
    return df


def load_history():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql("SELECT * FROM history", conn)
    conn.close()
    if df.empty:
        return pd.DataFrame(columns=HISTORY_COLUMNS)
    return df


def save_inventory(df):
    conn = sqlite3.connect(DB_FILE)
    df.to_sql("inventory", conn, if_exists="replace", index=False)
    conn.commit()
    conn.close()
    df.to_csv(INVENTORY_CSV, index=False, encoding="utf-8-sig")


def save_history(df):
    conn = sqlite3.connect(DB_FILE)
    df.to_sql("history", conn, if_exists="replace", index=False, index_label="id")
    conn.commit()
    conn.close()
    df.to_csv(HISTORY_CSV, index=False, encoding="utf-8-sig")


# ================================
# 4. 安全數字轉換
# ================================
def safe_float(x):
    try:
        if x is None or x == "":
            return 0.0
        return float(str(x).replace(",", ""))
    except:
        return 0.0


# ================================
# 5. 庫存重新計算
# ================================
def recalc_inventory(history_df, inventory_df):

    inv = inventory_df.copy()

    # reset
    for col in ['總庫存','均價'] + [f"庫存_{w}" for w in WAREHOUSES]:
        inv[col] = 0.0

    for sku in inv['貨號'].unique():
        hist = history_df[history_df['貨號'] == sku]

        total_qty = 0
        total_cost = 0
        wh_qty = {w: 0 for w in WAREHOUSES}

        for _, row in hist.iterrows():
            qty = safe_float(row['數量'])
            cost = safe_float(row['進貨總成本'])
            t = row['單據類型']
            wh = row['倉庫'] if row['倉庫'] in WAREHOUSES else "Wen"

            if t in ['進貨','製造入庫','庫存調整(加)','期初建檔']:
                total_qty += qty
                wh_qty[wh] += qty
                total_cost += cost

            elif t in ['銷售出貨','製造領料','庫存調整(減)']:
                avg = total_cost / total_qty if total_qty > 0 else 0
                total_qty -= qty
                total_cost -= qty * avg
                wh_qty[wh] -= qty

        inv.loc[inv['貨號'] == sku, '總庫存'] = total_qty
        inv.loc[inv['貨號'] == sku, '均價'] = total_cost / total_qty if total_qty > 0 else 0

        for w in WAREHOUSES:
            inv.loc[inv['貨號'] == sku, f"庫存_{w}"] = wh_qty[w]

    return inv

# ================================
# Streamlit 頁面設定
# ================================
st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon="🏭")
st.title(f"🏭 {PAGE_TITLE}")

# ================================
# 載入資料
# ================================
if "inventory" not in st.session_state:
    st.session_state["inventory"] = load_inventory()
if "history" not in st.session_state:
    st.session_state["history"] = load_history()


# ================================
# Sidebar 選單
# ================================
with st.sidebar:
    st.header("📌 功能選單")

    page = st.radio(
        "選擇作業",
        [
            "📦 商品建檔與維護",
            "📥 進貨庫存",
            "🚚 銷售出貨",
            "🔨 製造生產",
            "⚖️ 庫存盤點與調整",
            "📊 總表監控",
            "📄 報表下載中心",
        ]
    )

    st.divider()

    if st.button("🔴 重置所有快取"):
        st.cache_data.clear()
        st.success("快取已清除，將重新整理")
        st.rerun()

# ================================
# 通用篩選器（已驗證穩定）
# ================================
def filter_dataframe(df):
    if df.empty:
        return df

    st.write("🔍 **資料篩選器**")

    modify = st.checkbox("啟用篩選器", key=f"flt_{time.time()}")

    if not modify:
        return df

    df = df.copy()

    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass

    cols = st.multiselect("選擇篩選欄位", df.columns)

    for col in cols:

        if is_numeric_dtype(df[col]):
            _min = float(df[col].min())
            _max = float(df[col].max())
            val = st.slider(
                f"{col} 範圍",
                min_value=_min, max_value=_max,
                value=(_min, _max),
                key=f"rng_{col}"
            )
            df = df[df[col].between(val[0], val[1])]

        elif is_datetime64_any_dtype(df[col]):
            min_dt = df[col].min()
            max_dt = df[col].max()
            dt_range = st.date_input(
                f"{col} 日期範圍",
                value=(min_dt, max_dt),
                key=f"dt_{col}"
            )
            if len(dt_range) == 2:
                df = df[(df[col] >= pd.to_datetime(dt_range[0])) &
                        (df[col] <= pd.to_datetime(dt_range[1]))]

        else:
            txt = st.text_input(f"搜尋文字：{col}", key=f"txt_{col}")
            if txt:
                df = df[df[col].astype(str).str.contains(txt, case=False)]

    return df


# ================================
# 📄 報表下載中心
# ================================
if page == "📄 報表下載中心":
    st.subheader("📄 報表下載中心")

    inv = st.session_state["inventory"]
    hist = st.session_state["history"]

    tab1, tab2, tab3 = st.tabs(["📦 庫存報表", "📜 流水帳報表", "📘 下載全部"])

    # ---------------------------------------
    # 庫存報表
    with tab1:
        st.write("### 📦 庫存現況")
        df = filter_dataframe(inv)
        st.dataframe(df, use_container_width=True)

        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="庫存")

        st.download_button(
            "⬇️ 下載庫存報表 Excel",
            data=out.getvalue(),
            file_name=f"庫存報表_{date.today()}.xlsx"
        )

    # ---------------------------------------
    # 流水帳
    with tab2:
        st.write("### 📜 流水帳紀錄")
        df2 = filter_dataframe(hist)
        st.dataframe(df2, use_container_width=True)

        out2 = io.BytesIO()
        with pd.ExcelWriter(out2, engine="openpyxl") as writer:
            df2.to_excel(writer, index=False, sheet_name="流水帳")

        st.download_button(
            "⬇️ 下載流水帳 Excel",
            data=out2.getvalue(),
            file_name=f"流水帳報表_{date.today()}.xlsx"
        )

    # ---------------------------------------
    # 下載全部
    with tab3:
        st.write("### 📘 下載完整備份")

        out3 = io.BytesIO()
        with pd.ExcelWriter(out3, engine="openpyxl") as writer:
            inv.to_excel(writer, index=False, sheet_name="庫存")
            hist.to_excel(writer, index=False, sheet_name="流水帳")

        st.download_button(
            "⬇️ 下載完整系統 Excel（含所有資料）",
            data=out3.getvalue(),
            file_name=f"完整系統備份_{date.today()}.xlsx"
        )

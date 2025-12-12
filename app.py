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

# ================================================
# 📦 商品建檔與維護
# ================================================
if page == "📦 商品建檔與維護":

    st.subheader("📦 商品建檔與維護")

    inv = st.session_state["inventory"]

    tab1, tab2 = st.tabs(["✨ 新增商品", "📋 商品清單"])

    # -----------------------------
    # ✨ 新增商品
    # -----------------------------
    with tab1:
        col1, col2 = st.columns(2)

        series = col1.text_input("系列")
        category = col2.text_input("分類")

        name = st.text_input("品名")
        spec = st.text_input("規格")

        sku = st.text_input("貨號", value=f"AUTO-{int(time.time())}")

        if st.button("➕ 建立商品", type="primary"):
            if not sku or not name:
                st.error("🚨 貨號與品名必填")
            else:
                if sku in inv["貨號"].values:
                    st.warning("⚠️ 此貨號已存在")
                else:
                    new_row = {
                        "貨號": sku,
                        "系列": series,
                        "分類": category,
                        "品名": name,
                        "規格": spec,
                        "總庫存": 0,
                        "均價": 0,
                        "庫存_Wen": 0,
                        "庫存_千畇": 0,
                        "庫存_James": 0,
                        "庫存_Imeng": 0,
                    }
                    inv = pd.concat([inv, pd.DataFrame([new_row])], ignore_index=True)

                    st.session_state["inventory"] = inv
                    save_inventory(inv)

                    st.success(f"✨ 已新增商品：{sku} - {name}")
                    st.rerun()

    # -----------------------------
    # 📋 商品清單
    # -----------------------------
    with tab2:

        df = st.session_state["inventory"]
        df_show = filter_dataframe(df)

        st.dataframe(df_show, use_container_width=True)

        edited = st.data_editor(
            df_show,
            use_container_width=True,
            num_rows="dynamic",
            key="inv_edit",
        )

        if st.button("💾 儲存商品修改"):
            # 依照索引同步回 inventory
            for idx, row in edited.iterrows():
                original_index = df.index[df["貨號"] == row["貨號"]].tolist()[0]
                df.loc[original_index] = row

            st.session_state["inventory"] = df
            save_inventory(df)
            st.success("已更新商品資料！")

# ================================================
# 📥 進貨庫存（無金額）
# ================================================
if page == "📥 進貨庫存":

    st.subheader("📥 進貨庫存（無金額）")

    inv = st.session_state["inventory"]
    hist = st.session_state["history"]

    if inv.empty:
        st.info("目前沒有商品，請先前往「📦 商品建檔與維護」新增商品。")
    else:
        inv["label"] = inv["貨號"] + " | " + inv["品名"]

        with st.expander("➕ 新增進貨單", expanded=True):

            c1, c2 = st.columns([2, 1])
            sel_item = c1.selectbox("選擇商品", inv["label"])
            sel_wh = c2.selectbox("進貨入庫倉庫", WAREHOUSES)

            c3, c4, c5 = st.columns(3)
            qty = c3.number_input("進貨數量", min_value=1, value=1)
            dt = c4.date_input("進貨日期", value=date.today())
            keyer = c5.selectbox("經手人", ["Wen", "千畇", "James", "Imeng", "小幫手"])

            c6, c7 = st.columns(2)
            supplier = c6.text_input("廠商（可留空）")
            note = c7.text_input("備註")

            # ---- 儲存進貨記錄 ----
            if st.button("📥 確認進貨", type="primary"):
                row = inv[inv["label"] == sel_item].iloc[0]
                sku = row["貨號"]

                new_rec = {
                    "單據類型": "進貨",
                    "單號": f"IN-{int(time.time())}",
                    "日期": str(dt),
                    "系列": row["系列"],
                    "分類": row["分類"],
                    "品名": row["品名"],
                    "規格": row["規格"],
                    "貨號": sku,
                    "批號": f"IN-{date.today():%Y%m%d}",
                    "倉庫": sel_wh,
                    "數量": qty,
                    "Key單者": keyer,
                    "廠商": supplier,
                    "訂單單號": "",
                    "出貨日期": "",
                    "貨號備註": "",
                    "運費": 0,
                    "款項結清": "",
                    "工資": 0,
                    "發票": "",
                    "備註": note,
                    "進貨總成本": 0,
                }

                hist = pd.concat([hist, pd.DataFrame([new_rec])], ignore_index=True)

                # 重新計算庫存
                new_inv = recalc_inventory(hist, inv)

                # 存回 session + DB
                st.session_state["history"] = hist
                st.session_state["inventory"] = new_inv
                save_history(hist)
                save_inventory(new_inv)

                st.success(f"已成功新增進貨紀錄：{sku}  (+{qty})")
                st.rerun()

    # ========== 進貨紀錄表 ==============
    st.write("### 📄 進貨紀錄列表")

    df_view = hist[hist["單據類型"] == "進貨"].copy()

    if df_view.empty:
        st.info("目前尚無進貨紀錄。")
    else:
        df_filtered = filter_dataframe(df_view)
        st.dataframe(df_filtered, use_container_width=True)

# ================================================
# 🚚 銷售出貨
# ================================================
if page == "🚚 銷售出貨":

    st.subheader("🚚 銷售出貨")

    inv = st.session_state["inventory"]
    hist = st.session_state["history"]

    if inv.empty:
        st.info("目前沒有商品，請先前往「📦 商品建檔與維護」新增商品。")
    else:
        inv["label"] = inv["貨號"] + " | " + inv["品名"]

        with st.expander("➖ 新增出貨單", expanded=True):

            c1, c2 = st.columns([2, 1])
            sel_item = c1.selectbox("出貨商品", inv["label"])
            sel_wh = c2.selectbox("從哪個倉庫出貨", WAREHOUSES)

            c3, c4, c5 = st.columns(3)
            qty = c3.number_input("出貨數量", min_value=1, value=1)
            fee = c4.number_input("運費（可留 0）", min_value=0.0, value=0.0)
            dt = c5.date_input("出貨日期", value=date.today())

            c6, c7 = st.columns(2)
            ord_no = c6.text_input("訂單單號（可留空）")
            keyer = c7.selectbox("經手人", ["Wen", "千畇", "James", "Imeng", "小幫手"])

            note = st.text_input("備註（可留空）")

            # ---- 出貨動作 ----
            if st.button("📤 確認出貨", type="primary"):
                row = inv[inv["label"] == sel_item].iloc[0]
                sku = row["貨號"]

                # *** 庫存不足警示 ***
                curr_qty = float(row[f"庫存_{sel_wh}"])
                if qty > curr_qty:
                    st.error(f"❌ {sel_wh} 庫存不足！目前庫存：{curr_qty}")
                else:
                    new_rec = {
                        "單據類型": "銷售出貨",
                        "單號": f"OUT-{int(time.time())}",
                        "日期": str(dt),
                        "系列": row["系列"],
                        "分類": row["分類"],
                        "品名": row["品名"],
                        "規格": row["規格"],
                        "貨號": sku,
                        "批號": "",
                        "倉庫": sel_wh,
                        "數量": qty,
                        "Key單者": keyer,
                        "廠商": "",
                        "訂單單號": ord_no,
                        "出貨日期": str(dt),
                        "貨號備註": "",
                        "運費": fee,
                        "款項結清": "",
                        "工資": 0,
                        "發票": "",
                        "備註": note,
                        "進貨總成本": 0,
                    }

                    hist = pd.concat([hist, pd.DataFrame([new_rec])], ignore_index=True)

                    # 重新計算庫存
                    new_inv = recalc_inventory(hist, inv)

                    # 存回 session + DB
                    st.session_state["history"] = hist
                    st.session_state["inventory"] = new_inv
                    save_history(hist)
                    save_inventory(new_inv)

                    st.success(f"已成功出貨：{sku}  (-{qty})")
                    st.rerun()


    # =======================
    # 出貨紀錄
    # =======================
    st.write("### 📄 出貨紀錄列表")

    df_view = hist[hist["單據類型"] == "銷售出貨"].copy()

    if df_view.empty:
        st.info("目前尚無出貨紀錄。")
    else:
        df_filtered = filter_dataframe(df_view)
        st.dataframe(df_filtered, use_container_width=True)

import streamlit as st
import pandas as pd
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)
from datetime import date, datetime
import os
import time
import io
import sqlite3

# =========================================================
# 0. 基本設定
# =========================================================

st.set_page_config(page_title="製造庫存系統", layout="wide", page_icon="🏭")

PAGE_TITLE = "製造庫存系統"

# 檔案名稱
INVENTORY_CSV = "inventory_secure_v5.csv"
HISTORY_CSV = "history_secure_v5.csv"
DB_FILE = "inventory_secure_v5.db"

ADMIN_PASSWORD = "8888"  # 管理員/主管密碼

# 倉庫 (人員)
WAREHOUSES = ["Wen", "千畇", "James", "Imeng"]

# --- 核心流水帳 ---
HISTORY_COLUMNS = [
    "單據類型",
    "單號",
    "日期",
    "系列",
    "分類",
    "品名",
    "貨號",
    "批號",
    "倉庫",
    "數量",
    "Key單者",
    "廠商",
    "訂單單號",
    "出貨日期",
    "貨號備註",
    "運費",
    "款項結清",
    "工資",
    "發票",
    "備註",
    "進貨總成本",
]

NUM_HISTORY_COLS = ["數量", "進貨總成本", "運費", "工資"]

# --- 庫存狀態表 ---
INVENTORY_COLUMNS = [
    "貨號",
    "系列",
    "分類",
    "品名",
    "總庫存",
    "均價",
    "庫存_Wen",
    "庫存_千畇",
    "庫存_James",
    "庫存_Imeng",
]

DEFAULT_SERIES = ["原料", "半成品", "成品", "包材"]
DEFAULT_CATEGORIES = ["天然石", "金屬配件", "線材", "包裝材料", "完成品"]
DEFAULT_KEYERS = ["Wen", "千畇", "James", "Imeng", "小幫手"]

PREFIX_MAP = {
    "天然石": "ST",
    "金屬配件": "MT",
    "線材": "WR",
    "包裝材料": "PK",
    "完成品": "PD",
    "耗材": "OT",
}

# =========================================================
# 1. 共用工具函式
# =========================================================


def safe_float(x, default=0.0):
    """將任意值安全轉為 float，轉不出來就回 default"""
    try:
        if x is None:
            return float(default)
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "":
            return float(default)
        return float(s)
    except Exception:
        return float(default)


def normalize_inventory_df(df: pd.DataFrame) -> pd.DataFrame:
    """確保庫存表欄位齊全、型別正確，移除多餘欄位（例如 label）"""
    df = df.copy()

    # 先做舊欄位名稱轉換
    rename_map = {
        "庫存_原物料倉": "庫存_Wen",
        "庫存_半成品倉": "庫存_千畇",
        "庫存_成品倉": "庫存_James",
        "庫存_報廢倉": "庫存_Imeng",
    }
    df = df.rename(columns=rename_map)

    # 僅保留定義好的欄位
    for col in INVENTORY_COLUMNS:
        if col not in df.columns:
            # 數量 / 均價 / 庫存欄位統一設為 0
            if col in ["總庫存", "均價"] or col.startswith("庫存_"):
                df[col] = 0.0
            else:
                df[col] = ""

    df = df[INVENTORY_COLUMNS].copy()

    # 型別處理
    df["貨號"] = df["貨號"].astype(str)
    num_cols = ["總庫存", "均價"] + [f"庫存_{w}" for w in WAREHOUSES]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).astype(float)

    return df


def normalize_history_df(df: pd.DataFrame) -> pd.DataFrame:
    """確保流水帳欄位齊全、型別正確，並只保留定義好的欄位"""
    df = df.copy()

    # 倉庫舊名稱轉換
    if "倉庫" in df.columns:
        replace_map = {
            "原物料倉": "Wen",
            "半成品倉": "千畇",
            "成品倉": "James",
            "報廢倉": "Imeng",
        }
        df["倉庫"] = df["倉庫"].replace(replace_map)

    # 補欄位
    for col in HISTORY_COLUMNS:
        if col not in df.columns:
            if col in NUM_HISTORY_COLS:
                df[col] = 0
            else:
                df[col] = ""

    df = df[HISTORY_COLUMNS].copy()

    # 數字欄位轉 float
    for c in NUM_HISTORY_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).astype(float)

    # 日期欄位轉字串（避免 SQLite / CSV 格式混亂）
    if "日期" in df.columns:
        df["日期"] = df["日期"].astype(str)
    if "出貨日期" in df.columns:
        df["出貨日期"] = df["出貨日期"].astype(str)

    # 其他欄位轉字串
    for col in df.columns:
        if col not in NUM_HISTORY_COLS:
            df[col] = df[col].astype(str)

    return df


# =========================================================
# 2. SQLite + CSV 儲存 / 載入
# =========================================================


def load_from_db():
    """若 DB 存在則從 SQLite 讀資料，否則回傳 (None, None)"""
    if not os.path.exists(DB_FILE):
        return None, None
    try:
        conn = sqlite3.connect(DB_FILE)
        inv = pd.read_sql_query("SELECT * FROM inventory", conn)
        hist = pd.read_sql_query("SELECT * FROM history", conn)
        conn.close()
        return inv, hist
    except Exception:
        return None, None


def save_to_db(inv_df: pd.DataFrame, hist_df: pd.DataFrame):
    """儲存到 SQLite（整張表覆蓋），若失敗只不丟出錯誤"""
    try:
        conn = sqlite3.connect(DB_FILE)
        if inv_df is not None:
            inv_df.to_sql("inventory", conn, if_exists="replace", index=False)
        if hist_df is not None:
            hist_df.to_sql("history", conn, if_exists="replace", index=False)
        conn.close()
    except Exception as e:
        st.warning(f"⚠️ 寫入 SQLite 失敗：{e}")


def load_data():
    """
    優先從 SQLite 載入，若無則讀 CSV。
    無論來源，都會進行 normalize。
    """
    inv_df, hist_df = load_from_db()

    # --- 庫存 ---
    if inv_df is None:
        if os.path.exists(INVENTORY_CSV):
            try:
                inv_df = pd.read_csv(INVENTORY_CSV)
            except Exception:
                inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)
        else:
            inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)

    inv_df = normalize_inventory_df(inv_df)

    # --- 歷史 ---
    if hist_df is None:
        if os.path.exists(HISTORY_CSV):
            try:
                hist_df = pd.read_csv(HISTORY_CSV)
            except Exception:
                hist_df = pd.DataFrame(columns=HISTORY_COLUMNS)
        else:
            hist_df = pd.DataFrame(columns=HISTORY_COLUMNS)

    hist_df = normalize_history_df(hist_df)

    return inv_df, hist_df


def save_data():
    """同時寫入 CSV + SQLite"""
    inv_df = st.session_state.get("inventory", pd.DataFrame(columns=INVENTORY_COLUMNS))
    hist_df = st.session_state.get("history", pd.DataFrame(columns=HISTORY_COLUMNS))

    inv_df = normalize_inventory_df(inv_df)
    hist_df = normalize_history_df(hist_df)

    # CSV
    inv_df.to_csv(INVENTORY_CSV, index=False, encoding="utf-8-sig")
    hist_df.to_csv(HISTORY_CSV, index=False, encoding="utf-8-sig")

    # SQLite
    save_to_db(inv_df, hist_df)


# =========================================================
# 3. 篩選工具 & Excel 匯出
# =========================================================


def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """通用篩選器 UI 元件 (含全選功能)"""
    if df is None or df.empty:
        return df

    modify = st.checkbox("🔍 開啟資料篩選器 (Filter Data)")

    if not modify:
        return df

    df = df.copy()

    # 嘗試轉換日期欄位格式
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

    modification_container = st.container()

    with modification_container:
        to_filter_columns = st.multiselect("選擇要篩選的欄位", df.columns)

        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            left.write("↳")

            if is_categorical_dtype(df[column]) or df[column].nunique() < 50:
                options = sorted(df[column].astype(str).unique().tolist())
                use_all = right.checkbox(
                    f"全選 (Select All) - {column}",
                    value=True,
                    key=f"chk_{column}",
                )

                if use_all:
                    user_cat_input = options
                    right.caption(f"✅ 已顯示所有內容 ({len(options)} 項)")
                else:
                    user_cat_input = right.multiselect(
                        f"請選擇 {column} 的內容", options, default=[]
                    )

                if user_cat_input:
                    df = df[df[column].astype(str).isin(user_cat_input)]
                else:
                    if not use_all:
                        df = df[df[column].astype(str).isin([])]

            elif is_numeric_dtype(df[column]):
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100 if _max != _min else 1.0
                user_num_input = right.slider(
                    f"設定 {column} 的範圍",
                    min_value=_min,
                    max_value=_max,
                    value=(_min, _max),
                    step=step,
                )
                df = df[df[column].between(*user_num_input)]

            elif is_datetime64_any_dtype(df[column]):
                user_date_input = right.date_input(
                    f"選擇 {column} 的範圍", value=(df[column].min(), df[column].max())
                )
                if len(user_date_input) == 2:
                    user_date_input = tuple(map(pd.to_datetime, user_date_input))
                    start_date, end_date = user_date_input
                    df = df.loc[df[column] >= start_date]
                    df = df.loc[df[column] <= end_date]
            else:
                user_text_input = right.text_input(f"搜尋 {column} 包含的字串")
                if user_text_input:
                    df = df[
                        df[column]
                        .astype(str)
                        .str.contains(user_text_input, case=False, na=False)
                    ]

    return df


def get_safe_view(df: pd.DataFrame) -> pd.DataFrame:
    """隱藏敏感金額欄位"""
    if df is None or df.empty:
        return df
    sensitive_cols = ["進貨總成本", "均價", "工資", "款項結清"]
    safe_cols = [c for c in df.columns if c not in sensitive_cols]
    return df[safe_cols]


def convert_to_excel_all_sheets(inv_df, hist_df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        inv_df.to_excel(writer, index=False, sheet_name="庫存總表")

        if hist_df is not None and not hist_df.empty:
            df = hist_df.copy()
            if "單據類型" in df.columns:
                df_in = df[df["單據類型"] == "進貨"]
                df_in.to_excel(writer, index=False, sheet_name="進貨紀錄")

                df_mfg = df[df["單據類型"].str.contains("製造", na=False)]
                df_mfg.to_excel(writer, index=False, sheet_name="製造紀錄")

                df_out = df[df["單據類型"].isin(["銷售出貨", "製造領料"])]
                df_out.to_excel(writer, index=False, sheet_name="出貨紀錄")

            df.to_excel(writer, index=False, sheet_name="完整流水帳")

    return output.getvalue()


def convert_single_sheet_to_excel(df, sheet_name="Sheet1"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


# =========================================================
# 4. 庫存重算（穩定版）
# =========================================================


def recalculate_inventory(hist_df: pd.DataFrame, current_inv_df: pd.DataFrame):
    """
    由完整流水帳重算庫存：
    - 不會因為空白 / 非數字噴錯
    - 價格採加權平均法（有進貨總成本才會影響均價）
    """
    if hist_df is None:
        hist_df = pd.DataFrame(columns=HISTORY_COLUMNS)
    if current_inv_df is None:
        current_inv_df = pd.DataFrame(columns=INVENTORY_COLUMNS)

    hist_df = normalize_history_df(hist_df)
    new_inv = normalize_inventory_df(current_inv_df)

    # 1) 自動補齊歷史中有、庫存表沒有的貨號
    if not hist_df.empty:
        existing_skus = set(new_inv["貨號"].astype(str))
        hist_skus = set(hist_df["貨號"].astype(str))
        new_skus = hist_skus - existing_skus
        if new_skus:
            temp_df = (
                hist_df[hist_df["貨號"].astype(str).isin(new_skus)][
                    ["貨號", "系列", "分類", "品名"]
                ]
                .drop_duplicates("貨號")
                .copy()
            )
            for col in INVENTORY_COLUMNS:
                if col not in temp_df.columns:
                    if col in ["總庫存", "均價"] or col.startswith("庫存_"):
                        temp_df[col] = 0.0
                    else:
                        temp_df[col] = ""
            temp_df = temp_df[INVENTORY_COLUMNS]
            new_inv = pd.concat([new_inv, temp_df], ignore_index=True)

    # 2) 重設數量 / 均價
    cols_reset = ["總庫存", "均價"] + [f"庫存_{w}" for w in WAREHOUSES]
    for col in cols_reset:
        new_inv[col] = 0.0

    # 3) 逐貨號計算
    for idx, row in new_inv.iterrows():
        sku = str(row["貨號"])
        target_hist = hist_df[hist_df["貨號"].astype(str) == sku]

        total_qty = 0.0
        total_value = 0.0
        w_stock = {w: 0.0 for w in WAREHOUSES}

        for _, h_row in target_hist.iterrows():
            qty = safe_float(h_row.get("數量", 0))
            cost_total = safe_float(h_row.get("進貨總成本", 0))
            doc_type = str(h_row.get("單據類型", "")).strip()
            w_name = str(h_row.get("倉庫", "")).strip()
            if w_name not in WAREHOUSES:
                w_name = WAREHOUSES[0]

            if doc_type in ["進貨", "製造入庫", "調整入庫", "期初建檔", "庫存調整(加)"]:
                # 入庫：數量增加，若有成本則計入 total_value
                if cost_total > 0:
                    total_value += cost_total
                total_qty += qty
                w_stock[w_name] += qty

            elif doc_type in ["銷售出貨", "製造領料", "調整出庫", "庫存調整(減)"]:
                # 出庫：用目前加權平均成本扣除
                current_avg = (total_value / total_qty) if total_qty > 0 else 0.0
                total_qty -= qty
                if total_qty < 0:
                    total_qty = 0
                total_value -= qty * current_avg
                if total_value < 0:
                    total_value = 0
                w_stock[w_name] -= qty

        new_inv.at[idx, "總庫存"] = total_qty
        new_inv.at[idx, "均價"] = (total_value / total_qty) if total_qty > 0 else 0.0
        for w in WAREHOUSES:
            new_inv.at[idx, f"庫存_{w}"] = w_stock[w]

    return normalize_inventory_df(new_inv)


# =========================================================
# 5. 其他工具：SKU / 批號 / 匯入處理
# =========================================================


def gen_batch_number(prefix="BAT"):
    return f"{prefix}-{datetime.now().strftime('%y%m%d%H%M')}"


def gen_mo_number():
    return f"MO-{datetime.now().strftime('%y%m%d-%H%M')}"


def get_dynamic_options(column_name, default_list):
    options = set(default_list)
    inv_df = st.session_state.get("inventory", pd.DataFrame(columns=INVENTORY_COLUMNS))
    if not inv_df.empty and column_name in inv_df.columns:
        existing = inv_df[column_name].dropna().unique().tolist()
        options.update([str(x) for x in existing if str(x).strip() != ""])
    # 讓「手動新增」永遠在最後
    return sorted(list(options)) + ["➕ 手動輸入新資料"]


def auto_generate_sku(category):
    prefix = PREFIX_MAP.get(category, "XX")
    df = st.session_state.get("inventory", pd.DataFrame(columns=INVENTORY_COLUMNS))
    df = normalize_inventory_df(df)
    if df.empty:
        return f"{prefix}0001"
    same_prefix = df[df["貨號"].astype(str).str.startswith(prefix)]
    if same_prefix.empty:
        return f"{prefix}0001"
    try:
        max_num = (
            same_prefix["貨號"]
            .str.replace(prefix, "", regex=False)
            .str.extract(r"(\d+)")
            .iloc[:, 0]
            .astype(float)
            .max()
        )
        if pd.isna(max_num):
            return f"{prefix}0001"
        next_num = int(max_num) + 1
        return f"{prefix}{next_num:04d}"
    except Exception:
        return f"{prefix}-{int(time.time())}"


def process_product_upload(file_obj):
    try:
        if file_obj.name.endswith(".csv"):
            df = pd.read_csv(file_obj)
        else:
            df = pd.read_excel(file_obj)

        rename_map = {"名稱": "品名", "商品名稱": "品名", "類別": "分類", "SKU": "貨號"}
        df = df.rename(columns=rename_map)

        if "貨號" not in df.columns or "品名" not in df.columns:
            return None, "缺少必要欄位：'貨號' 或 '品名'"

        target_cols = ["貨號", "系列", "分類", "品名"]
        for col in target_cols:
            if col not in df.columns:
                if col in ["系列", "分類"]:
                    df[col] = "未分類"
                else:
                    df[col] = ""

        new_products = df[target_cols].copy()
        new_products["貨號"] = new_products["貨號"].astype(str)
        return new_products, "OK"
    except Exception as e:
        return None, str(e)


def process_opening_stock_upload(file_obj, default_warehouse):
    try:
        if file_obj.name.endswith(".csv"):
            df = pd.read_csv(file_obj)
        else:
            df = pd.read_excel(file_obj)

        rename_map = {
            "名稱": "品名",
            "商品名稱": "品名",
            "SKU": "貨號",
            "庫存": "數量",
            "現有庫存": "數量",
            "成本": "進貨總成本",
            "總成本": "進貨總成本",
        }
        df = df.rename(columns=rename_map)

        if "貨號" not in df.columns or "數量" not in df.columns:
            return None, "Excel 必須包含「貨號」與「數量」欄位"

        new_records = []
        batch_no = f"INIT-{date.today().strftime('%Y%m%d')}"

        inv_ref = st.session_state.get(
            "inventory", pd.DataFrame(columns=INVENTORY_COLUMNS)
        )
        inv_ref = normalize_inventory_df(inv_ref)

        for _, row in df.iterrows():
            sku = str(row["貨號"])
            qty = safe_float(row.get("數量", 0))
            if qty <= 0:
                continue

            wh = (
                row["倉庫"]
                if "倉庫" in df.columns and pd.notna(row["倉庫"])
                else default_warehouse
            )
            cost = safe_float(row.get("進貨總成本", 0))

            ref_row = inv_ref[inv_ref["貨號"] == sku]

            if not ref_row.empty:
                series = ref_row.iloc[0]["系列"]
                category = ref_row.iloc[0]["分類"]
                name = ref_row.iloc[0]["品名"]
            else:
                series = row.get("系列", "期初匯入")
                category = row.get("分類", "期初匯入")
                name = row.get("品名", f"未知品名-{sku}")

            rec = {
                "單據類型": "期初建檔",
                "單號": f"OPEN-{int(time.time())}-{sku}",
                "日期": str(date.today()),
                "系列": series,
                "分類": category,
                "品名": name,
                "貨號": sku,
                "批號": batch_no,
                "倉庫": wh,
                "數量": qty,
                "Key單者": "系統匯入",
                "進貨總成本": cost,
                "備註": "Excel期初庫存匯入",
            }

            for c in HISTORY_COLUMNS:
                if c not in rec:
                    rec[c] = 0 if c in NUM_HISTORY_COLS else ""

            new_records.append(rec)

        if not new_records:
            return pd.DataFrame(columns=HISTORY_COLUMNS), "OK"

        df_new = pd.DataFrame(new_records)
        df_new = normalize_history_df(df_new)
        return df_new, "OK"

    except Exception as e:
        return None, str(e)


def process_restore_upload(file_obj):
    try:
        df_res = pd.read_excel(file_obj, sheet_name="完整流水帳")
        df_res = normalize_history_df(df_res)
        return df_res
    except Exception as e:
        st.error(f"還原失敗: {e}")
        return None


# =========================================================
# 6. 初始化 Session State
# =========================================================

if "inventory" not in st.session_state or "history" not in st.session_state:
    inv, hist = load_data()
    st.session_state["inventory"] = inv
    st.session_state["history"] = hist

# =========================================================
# 7. 介面佈局
# =========================================================

st.title(f"🏭 {PAGE_TITLE}")

with st.sidebar:
    st.header("部門功能導航")
    page = st.radio(
        "選擇作業",
        [
            "📦 商品建檔與維護",
            "⚖️ 庫存盤點與調整",
            "📥 進貨庫存 (無金額)",
            "🔨 製造生產 (工廠)",
            "🚚 銷售出貨 (業務/出貨)",
            "📊 總表監控 (主管專用)",
            "💰 成本與財務管理 (加密)",
        ],
    )

    st.divider()
    st.markdown("### 💾 資料管理")

    if not st.session_state["history"].empty:
        with st.expander("📥 下載單獨報表", expanded=False):
            st.download_button(
                "📊 庫存現況表.xlsx",
                data=convert_single_sheet_to_excel(
                    normalize_inventory_df(st.session_state["inventory"]), "庫存表"
                ),
                file_name=f"Stock_{date.today()}.xlsx",
            )

            df_in = st.session_state["history"][
                st.session_state["history"]["單據類型"] == "進貨"
            ]
            st.download_button(
                "📥 進貨紀錄表.xlsx",
                data=convert_single_sheet_to_excel(df_in, "進貨紀錄"),
                file_name=f"Purchase_{date.today()}.xlsx",
            )

            df_out = st.session_state["history"][
                st.session_state["history"]["單據類型"].isin(["銷售出貨"])
            ]
            st.download_button(
                "🚚 銷貨紀錄表.xlsx",
                data=convert_single_sheet_to_excel(df_out, "銷貨紀錄"),
                file_name=f"Sales_{date.today()}.xlsx",
            )

            df_mfg = st.session_state["history"][
                st.session_state["history"]["單據類型"].str.contains("製造", na=False)
            ]
            st.download_button(
                "🔨 製造紀錄表.xlsx",
                data=convert_single_sheet_to_excel(df_mfg, "製造紀錄"),
                file_name=f"Mfg_{date.today()}.xlsx",
            )

        excel_data = convert_to_excel_all_sheets(
            normalize_inventory_df(st.session_state["inventory"]),
            st.session_state["history"],
        )
        st.download_button(
            label="📥 下載完整總表 (Excel)",
            data=excel_data,
            file_name=f"Report_Full_{date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    with st.expander("⚙️ 系統還原 (上傳備份)", expanded=False):
        restore_file = st.file_uploader("上傳備份檔", type=["xlsx"], key="restore")
        if restore_file and st.button("確認還原並重算"):
            df_new_hist = process_restore_upload(restore_file)
            if df_new_hist is not None:
                st.session_state["history"] = df_new_hist
                st.session_state["inventory"] = recalculate_inventory(
                    df_new_hist, st.session_state["inventory"]
                )
                save_data()
                st.success("還原成功！")
                time.sleep(1)
                st.experimental_rerun()

# =========================================================
# 8. 各頁面功能
# =========================================================

# ---------------------------------------------------------
# 頁面 1: 商品建檔與維護
# ---------------------------------------------------------
if page == "📦 商品建檔與維護":
    st.subheader("📦 商品資料庫管理")
    tab_single, tab_batch, tab_opening, tab_list = st.tabs(
        ["✨ 單筆建檔", "📂 批次匯入 (基本資料)", "📥 匯入期初庫存", "📋 檢視/修改商品"]
    )

    # === 單筆建檔 ===
    with tab_single:
        st.caption("智慧建檔：自動學習分類、自動產生貨號。")
        cat_opts = get_dynamic_options("分類", DEFAULT_CATEGORIES)
        cat_sel = st.selectbox("商品分類", cat_opts)
        final_cat = (
            st.text_input("↳ 請輸入新分類名稱") if cat_sel == "➕ 手動輸入新資料" else cat_sel
        )

        ser_opts = get_dynamic_options("系列", DEFAULT_SERIES)
        ser_sel = st.selectbox("商品系列", ser_opts)
        final_ser = (
            st.text_input("↳ 請輸入新系列名稱") if ser_sel == "➕ 手動輸入新資料" else ser_sel
        )

        name = st.text_input("商品品名")
        auto_sku = auto_generate_sku(final_cat) if final_cat else ""
        sku = st.text_input("商品貨號 (預設自動產生)", value=auto_sku)

        if st.button("確認建立新商品", type="primary"):
            if not name or not final_cat or not final_ser:
                st.error("品名、分類、系列為必填")
            else:
                inv_df = normalize_inventory_df(st.session_state["inventory"])
                if not inv_df.empty and sku in inv_df["貨號"].values:
                    st.warning(f"⚠️ 貨號 {sku} 已存在")
                else:
                    new_row = {
                        "貨號": sku,
                        "系列": final_ser,
                        "分類": final_cat,
                        "品名": name,
                        "總庫存": 0.0,
                        "均價": 0.0,
                    }
                    for w in WAREHOUSES:
                        new_row[f"庫存_{w}"] = 0.0
                    inv_df = pd.concat(
                        [inv_df, pd.DataFrame([new_row])], ignore_index=True
                    )
                    st.session_state["inventory"] = inv_df
                    save_data()
                    st.success(f"✅ 已建立：{name} ({sku})")
                    time.sleep(1)
                    st.experimental_rerun()

    # === 批次匯入 (商品資料) ===
    with tab_batch:
        st.info("僅匯入商品資料 (貨號、品名、分類)，不影響庫存數量。")
        up_prod = st.file_uploader("選擇 Excel / CSV", type=["xlsx", "xls", "csv"])
        if up_prod and st.button("開始匯入商品資料"):
            new_prods, msg = process_product_upload(up_prod)
            if new_prods is None:
                st.error(msg)
            else:
                old_inv = normalize_inventory_df(st.session_state["inventory"])
                for _, row in new_prods.iterrows():
                    sku = str(row["貨號"])
                    mask = old_inv["貨號"] == sku
                    if mask.any():
                        idx = old_inv[mask].index[0]
                        old_inv.at[idx, "品名"] = row["品名"]
                        old_inv.at[idx, "分類"] = row["分類"]
                        old_inv.at[idx, "系列"] = row["系列"]
                    else:
                        new_row = row.to_dict()
                        new_row["總庫存"] = 0.0
                        new_row["均價"] = 0.0
                        for w in WAREHOUSES:
                            new_row[f"庫存_{w}"] = 0.0
                        old_inv = pd.concat(
                            [old_inv, pd.DataFrame([new_row])], ignore_index=True
                        )
                st.session_state["inventory"] = old_inv
                save_data()
                st.success("匯入完成！")
                time.sleep(1)
                st.experimental_rerun()

    # === 匯入期初庫存 ===
    with tab_opening:
        st.markdown("### 📥 匯入現有庫存 (Excel / CSV)")
        target_wh = st.selectbox("若 Excel 無倉庫欄位，預設匯入至：", WAREHOUSES)
        up_stock = st.file_uploader("上傳庫存盤點表", type=["xlsx", "xls", "csv"])
        if up_stock and st.button("確認匯入庫存"):
            df_opening_hist, msg = process_opening_stock_upload(up_stock, target_wh)
            if df_opening_hist is None:
                st.error(msg)
            elif df_opening_hist.empty:
                st.warning("無效庫存資料")
            else:
                st.session_state["history"] = pd.concat(
                    [st.session_state["history"], df_opening_hist], ignore_index=True
                )
                st.session_state["inventory"] = recalculate_inventory(
                    st.session_state["history"], st.session_state["inventory"]
                )
                save_data()
                st.success(f"✅ 成功匯入 {len(df_opening_hist)} 筆庫存資料！")
                time.sleep(1)
                st.experimental_rerun()

    # === 檢視 / 修改商品 ===
    with tab_list:
        st.info("此處可直接修改品名、分類或系列。修改後請務必按下「儲存修改」按鈕。")
        df_safe = get_safe_view(normalize_inventory_df(st.session_state["inventory"]))
        df_safe = filter_dataframe(df_safe)

        edited_products = st.data_editor(
            df_safe,
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "貨號": st.column_config.TextColumn(disabled=True),
                "總庫存": st.column_config.NumberColumn(disabled=True),
                "庫存_Wen": st.column_config.NumberColumn(disabled=True),
                "庫存_千畇": st.column_config.NumberColumn(disabled=True),
                "庫存_James": st.column_config.NumberColumn(disabled=True),
                "庫存_Imeng": st.column_config.NumberColumn(disabled=True),
            },
        )
        if st.button("💾 儲存商品資料修改"):
            current_inv = normalize_inventory_df(st.session_state["inventory"])
            # 依 index 更新（data_editor 會保留原來 index）
            for idx, row in edited_products.iterrows():
                if idx in current_inv.index:
                    current_inv.at[idx, "品名"] = row["品名"]
                    current_inv.at[idx, "分類"] = row["分類"]
                    current_inv.at[idx, "系列"] = row["系列"]
            st.session_state["inventory"] = current_inv
            save_data()
            st.success("✅ 商品資料已更新！")

# ---------------------------------------------------------
# 頁面 2: 庫存盤點與調整
# ---------------------------------------------------------
elif page == "⚖️ 庫存盤點與調整":
    st.subheader("⚖️ 快速修正庫存 (盤點調整)")
    inv_df = normalize_inventory_df(st.session_state["inventory"])
    if inv_df.empty:
        st.warning("無商品資料")
    else:
        df_label = inv_df.copy()
        df_label["label"] = df_label["貨號"] + " | " + df_label["品名"]

        c1, c2 = st.columns([2, 1])
        with c1:
            sel_item = st.selectbox("選擇要調整的商品", df_label["label"].tolist())
            sel_sku = (
                df_label.loc[df_label["label"] == sel_item, "貨號"].iloc[0]
            )  # 取貨號
            row = inv_df[inv_df["貨號"] == sel_sku].iloc[0]
        with c2:
            sel_wh = st.selectbox("調整哪個倉庫的庫存？", WAREHOUSES)

        curr_qty = safe_float(row[f"庫存_{sel_wh}"])
        st.metric(f"目前 {sel_wh} 系統庫存", f"{int(curr_qty)}")

        st.divider()

        with st.form("adj_form"):
            new_qty = st.number_input(
                "🔴 請輸入正確的【盤點實際數量】",
                min_value=0,
                value=int(curr_qty),
            )
            adj_reason = st.text_input(
                "調整原因 (例如：盤點差異、遺失、破損)", value="庫存盤點修正"
            )

            if st.form_submit_button("✅ 確認修正庫存"):
                diff = new_qty - curr_qty

                if diff == 0:
                    st.warning("數量未變動，無需調整。")
                else:
                    action = "庫存調整(加)" if diff > 0 else "庫存調整(減)"
                    final_qty = abs(diff)

                    rec = {
                        "單據類型": action,
                        "單號": f"ADJ-{int(time.time())}",
                        "日期": str(date.today()),
                        "系列": row["系列"],
                        "分類": row["分類"],
                        "品名": row["品名"],
                        "貨號": row["貨號"],
                        "批號": "",
                        "倉庫": sel_wh,
                        "數量": final_qty,
                        "Key單者": "盤點調整",
                        "備註": f"{adj_reason} (原:{int(curr_qty)} -> 新:{int(new_qty)})",
                    }

                    for c in HISTORY_COLUMNS:
                        if c not in rec:
                            rec[c] = 0 if c in NUM_HISTORY_COLS else ""

                    st.session_state["history"] = pd.concat(
                        [st.session_state["history"], pd.DataFrame([rec])],
                        ignore_index=True,
                    )
                    st.session_state["history"] = normalize_history_df(
                        st.session_state["history"]
                    )
                    st.session_state["inventory"] = recalculate_inventory(
                        st.session_state["history"], st.session_state["inventory"]
                    )
                    save_data()
                    st.success(f"已修正！庫存已更新為 {new_qty}。")
                    time.sleep(1)
                    st.experimental_rerun()

# ---------------------------------------------------------
# 頁面 3: 進貨 (無金額)
# ---------------------------------------------------------
elif page == "📥 進貨庫存 (無金額)":
    st.subheader("📥 進貨點收")
    with st.expander("➕ 新增進貨單", expanded=True):
        inv_df = normalize_inventory_df(st.session_state["inventory"])
        if inv_df.empty:
            st.warning("請先至「商品建檔」建立資料")
        else:
            df_label = inv_df.copy()
            df_label["label"] = df_label["貨號"] + " | " + df_label["品名"]
            c1, c2, c3 = st.columns([2, 1, 1])
            p_sel = c1.selectbox("進貨商品", df_label["label"].tolist())
            sel_sku = df_label.loc[df_label["label"] == p_sel, "貨號"].iloc[0]
            p_row = inv_df[inv_df["貨號"] == sel_sku].iloc[0]

            p_wh = c2.selectbox("入庫倉庫", WAREHOUSES, index=0)
            p_qty = c3.number_input("進貨數量", 1)

            c4, c5, c6 = st.columns(3)
            p_date = c4.date_input("進貨日期", date.today())
            p_user = c5.selectbox("Key單者", DEFAULT_KEYERS)
            p_sup = c6.text_input("廠商名稱 (Supplier)")
            p_note = st.text_input("備註")

            if st.button("確認進貨"):
                rec = {
                    "單據類型": "進貨",
                    "單號": datetime.now().strftime("%Y%m%d%H%M%S"),
                    "日期": str(p_date),
                    "系列": p_row["系列"],
                    "分類": p_row["分類"],
                    "品名": p_row["品名"],
                    "貨號": p_row["貨號"],
                    "批號": gen_batch_number("IN"),
                    "倉庫": p_wh,
                    "數量": p_qty,
                    "Key單者": p_user,
                    "廠商": p_sup,
                    "備註": p_note,
                    "進貨總成本": 0.0,
                }
                for c in HISTORY_COLUMNS:
                    if c not in rec:
                        rec[c] = 0 if c in NUM_HISTORY_COLS else ""
                st.session_state["history"] = pd.concat(
                    [st.session_state["history"], pd.DataFrame([rec])],
                    ignore_index=True,
                )
                st.session_state["history"] = normalize_history_df(
                    st.session_state["history"]
                )
                st.session_state["inventory"] = recalculate_inventory(
                    st.session_state["history"], st.session_state["inventory"]
                )
                save_data()
                st.success("進貨單已建立！")
                time.sleep(1)
                st.experimental_rerun()

    df = st.session_state["history"]
    if not df.empty:
        df_view = df[df["單據類型"] == "進貨"].copy()
        purchase_cols = [
            "單號",
            "日期",
            "廠商",
            "系列",
            "分類",
            "品名",
            "貨號",
            "批號",
            "倉庫",
            "數量",
            "Key單者",
            "備註",
        ]
        valid_cols = [c for c in purchase_cols if c in df_view.columns]
        st.write("---")
        df_filtered = filter_dataframe(df_view[valid_cols])
        st.dataframe(df_filtered, use_container_width=True)

# ---------------------------------------------------------
# 頁面 4: 製造生產 (工廠)
# ---------------------------------------------------------
elif page == "🔨 製造生產 (工廠)":
    st.subheader("🔨 製造生產紀錄")
    tab1, tab2 = st.tabs(["📤 領料", "📥 完工"])
    inv_df = normalize_inventory_df(st.session_state["inventory"])
    df_label = inv_df.copy()
    df_label["label"] = (
        df_label["貨號"] + " | " + df_label["品名"] + " | 總存:" + df_label["總庫存"].astype(str)
    )

    # 領料
    with tab1:
        with st.form("mfg_out"):
            c_date, c_mo = st.columns(2)
            m_date = c_date.date_input("領料日期", value=date.today())
            m_mo = c_mo.text_input("工單單號", value=gen_mo_number())
            c1, c2 = st.columns([2, 1])
            m_sel = c1.selectbox("原料", df_label["label"].tolist())
            sel_sku = df_label.loc[df_label["label"] == m_sel, "貨號"].iloc[0]
            m_row = inv_df[inv_df["貨號"] == sel_sku].iloc[0]
            m_wh = c2.selectbox("從誰領料", WAREHOUSES, index=0)
            c3, c4 = st.columns(2)
            m_qty = c3.number_input("領用量", 1)
            m_user = c4.selectbox("領料人", DEFAULT_KEYERS)
            if st.form_submit_button("確認領料"):
                rec = {
                    "單據類型": "製造領料",
                    "單號": datetime.now().strftime("%Y%m%d%H%M%S"),
                    "日期": str(m_date),
                    "系列": m_row["系列"],
                    "分類": m_row["分類"],
                    "品名": m_row["品名"],
                    "貨號": m_row["貨號"],
                    "批號": "",
                    "倉庫": m_wh,
                    "數量": m_qty,
                    "Key單者": m_user,
                    "訂單單號": m_mo,
                }
                for c in HISTORY_COLUMNS:
                    if c not in rec:
                        rec[c] = 0 if c in NUM_HISTORY_COLS else ""
                st.session_state["history"] = pd.concat(
                    [st.session_state["history"], pd.DataFrame([rec])],
                    ignore_index=True,
                )
                st.session_state["history"] = normalize_history_df(
                    st.session_state["history"]
                )
                st.session_state["inventory"] = recalculate_inventory(
                    st.session_state["history"], st.session_state["inventory"]
                )
                save_data()
                st.success(f"已領料 {m_qty}")
                time.sleep(1)
                st.experimental_rerun()

    # 完工入庫
    with tab2:
        with st.form("mfg_in"):
            c_date, c_mo = st.columns(2)
            f_date = c_date.date_input("完工日期", value=date.today())
            f_mo = c_mo.text_input("工單單號", value=gen_mo_number())
            c1, c2 = st.columns([2, 1])
            f_sel = c1.selectbox("成品", df_label["label"].tolist())
            sel_sku = df_label.loc[df_label["label"] == f_sel, "貨號"].iloc[0]
            f_row = inv_df[inv_df["貨號"] == sel_sku].iloc[0]
            f_wh = c2.selectbox("入庫給誰", WAREHOUSES, index=1)
            c3, c4, c5 = st.columns(3)
            f_qty = c3.number_input("產出量", 1)
            f_batch = c4.text_input("成品批號", value=gen_batch_number("PD"))
            f_user = c5.selectbox("Key單者", DEFAULT_KEYERS)
            if st.form_submit_button("完工入庫"):
                rec = {
                    "單據類型": "製造入庫",
                    "單號": datetime.now().strftime("%Y%m%d%H%M%S"),
                    "日期": str(f_date),
                    "系列": f_row["系列"],
                    "分類": f_row["分類"],
                    "品名": f_row["品名"],
                    "貨號": f_row["貨號"],
                    "批號": f_batch,
                    "倉庫": f_wh,
                    "數量": f_qty,
                    "Key單者": f_user,
                    "訂單單號": f_mo,
                }
                for c in HISTORY_COLUMNS:
                    if c not in rec:
                        rec[c] = 0 if c in NUM_HISTORY_COLS else ""
                st.session_state["history"] = pd.concat(
                    [st.session_state["history"], pd.DataFrame([rec])],
                    ignore_index=True,
                )
                st.session_state["history"] = normalize_history_df(
                    st.session_state["history"]
                )
                st.session_state["inventory"] = recalculate_inventory(
                    st.session_state["history"], st.session_state["inventory"]
                )
                save_data()
                st.success("完工入庫成功")
                time.sleep(1)
                st.experimental_rerun()

    df = st.session_state["history"]
    if not df.empty:
        mask = df["單據類型"].astype(str).str.contains("製造")
        df_view = get_safe_view(df[mask])
        st.write("---")
        df_filtered = filter_dataframe(df_view)
        st.dataframe(df_filtered, use_container_width=True)

# ---------------------------------------------------------
# 頁面 5: 銷售出貨
# ---------------------------------------------------------
elif page == "🚚 銷售出貨 (業務/出貨)":
    st.subheader("🚚 出貨紀錄表")
    with st.expander("➖ 新增銷售出貨單", expanded=True):
        inv_df = normalize_inventory_df(st.session_state["inventory"])
        df_label = inv_df.copy()
        df_label["label"] = (
            df_label["貨號"]
            + " | "
            + df_label["品名"]
            + " | 總存:"
            + df_label["總庫存"].astype(str)
        )
        with st.form("sales"):
            c1, c2 = st.columns([2, 1])
            s_sel = c1.selectbox("商品", df_label["label"].tolist())
            sel_sku = df_label.loc[df_label["label"] == s_sel, "貨號"].iloc[0]
            s_row = inv_df[inv_df["貨號"] == sel_sku].iloc[0]

            s_wh = c2.selectbox("從誰出貨", WAREHOUSES, index=2)
            c3, c4, c5 = st.columns(3)
            s_qty = c3.number_input("數量", 1)
            s_fee = c4.number_input("運費", 0)
            s_date = c5.date_input("出貨日期", date.today())
            c6, c7 = st.columns(2)
            s_ord = c6.text_input("訂單單號")
            s_user = c7.selectbox("Key單者", DEFAULT_KEYERS)
            s_note = st.text_area("備註")
            if st.form_submit_button("確認出貨"):
                rec = {
                    "單據類型": "銷售出貨",
                    "單號": datetime.now().strftime("%Y%m%d%H%M%S"),
                    "日期": str(s_date),
                    "系列": s_row["系列"],
                    "分類": s_row["分類"],
                    "品名": s_row["品名"],
                    "貨號": s_row["貨號"],
                    "批號": "",
                    "倉庫": s_wh,
                    "數量": s_qty,
                    "Key單者": s_user,
                    "訂單單號": s_ord,
                    "運費": s_fee,
                    "備註": s_note,
                }
                for c in HISTORY_COLUMNS:
                    if c not in rec:
                        rec[c] = 0 if c in NUM_HISTORY_COLS else ""
                st.session_state["history"] = pd.concat(
                    [st.session_state["history"], pd.DataFrame([rec])],
                    ignore_index=True,
                )
                st.session_state["history"] = normalize_history_df(
                    st.session_state["history"]
                )
                st.session_state["inventory"] = recalculate_inventory(
                    st.session_state["history"], st.session_state["inventory"]
                )
                save_data()
                st.success("出貨成功！")
                time.sleep(1)
                st.experimental_rerun()

    df = st.session_state["history"]
    if not df.empty:
        mask = df["單據類型"].isin(["銷售出貨", "製造領料"])
        df_view = df[mask].copy()
        sales_cols = [
            "單號",
            "訂單單號",
            "出貨日期",
            "系列",
            "分類",
            "品名",
            "貨號",
            "倉庫",
            "數量",
            "運費",
            "Key單者",
            "備註",
        ]
        valid_cols = [c for c in sales_cols if c in df_view.columns]
        st.write("---")
        df_filtered = filter_dataframe(df_view[valid_cols])
        st.dataframe(df_filtered, use_container_width=True)

# ---------------------------------------------------------
# 頁面 6: 總表監控 (主管專用)
# ---------------------------------------------------------
elif page == "📊 總表監控 (主管專用)":
    st.subheader("📊 總表監控與資料維護")
    st.info("此區僅供主管進入，進行資料修改或刪除。")
    pwd = st.text_input("🔒 請輸入主管密碼", type="password", key="admin_pwd")
    if pwd == ADMIN_PASSWORD:
        st.success("✅ 驗證成功")
        tab_inv, tab_hist = st.tabs(["📦 庫存總表 (狀態)", "📜 完整流水帳 (可刪除/修正)"])

        with tab_inv:
            df_inv = normalize_inventory_df(st.session_state["inventory"])
            if not df_inv.empty:
                df_filtered_inv = filter_dataframe(df_inv)
                edited_inv = st.data_editor(
                    df_filtered_inv,
                    use_container_width=True,
                    num_rows="dynamic",
                    column_config={
                        "總庫存": st.column_config.NumberColumn(disabled=True),
                    },
                )
                if st.button("💾 儲存商品資料變更"):
                    # 直接以編輯結果覆蓋（已經是完整表）
                    st.session_state["inventory"] = normalize_inventory_df(edited_inv)
                    save_data()
                    st.success("商品資料已更新")

        with tab_hist:
            df_hist = st.session_state["history"]
            if not df_hist.empty:
                df_filtered_hist = filter_dataframe(df_hist)
                edited_hist = st.data_editor(
                    df_filtered_hist,
                    use_container_width=True,
                    num_rows="dynamic",
                    height=600,
                    column_config={
                        "倉庫": st.column_config.SelectboxColumn(
                            "倉庫", options=WAREHOUSES
                        ),
                        "單據類型": st.column_config.SelectboxColumn(
                            "單據類型",
                            options=[
                                "進貨",
                                "銷售出貨",
                                "製造領料",
                                "製造入庫",
                                "期初建檔",
                                "庫存調整(加)",
                                "庫存調整(減)",
                            ],
                        ),
                    },
                )

                if st.button("💾 儲存修正並重算"):
                    st.session_state["history"] = normalize_history_df(edited_hist)
                    st.session_state["inventory"] = recalculate_inventory(
                        st.session_state["history"], st.session_state["inventory"]
                    )
                    save_data()
                    st.success("已修正")
    elif pwd != "":
        st.error("密碼錯誤")

# ---------------------------------------------------------
# 頁面 7: 成本與財務管理
# ---------------------------------------------------------
elif page == "💰 成本與財務管理 (加密)":
    st.subheader("💰 成本與財務中心")
    pwd = st.text_input("請輸入管理員密碼", type="password")

    if pwd == ADMIN_PASSWORD:
        st.success("身分驗證成功")
        tab_fix, tab_full = st.tabs(["💸 補登進貨成本", "📜 完整流水帳 (含金額)"])

        with tab_fix:
            df = st.session_state["history"]
            mask = (df["單據類型"] == "進貨") & (df["進貨總成本"] == 0)
            df_fix = df[mask].copy()
            if df_fix.empty:
                st.info("✅ 無待補登單據")
            else:
                df_fix_filtered = filter_dataframe(df_fix)
                edited = st.data_editor(
                    df_fix_filtered,
                    column_config={
                        "進貨總成本": st.column_config.NumberColumn(required=True)
                    },
                )
                if st.button("💾 儲存"):
                    df.update(edited)
                    st.session_state["history"] = normalize_history_df(df)
                    st.session_state["inventory"] = recalculate_inventory(
                        st.session_state["history"], st.session_state["inventory"]
                    )
                    save_data()
                    st.success("已更新")

        with tab_full:
            df_all_filtered = filter_dataframe(st.session_state["history"])
            edited_all = st.data_editor(
                df_all_filtered, use_container_width=True, num_rows="dynamic"
            )
            if st.button("💾 儲存修正"):
                st.session_state["history"] = normalize_history_df(edited_all)
                st.session_state["inventory"] = recalculate_inventory(
                    st.session_state["history"], st.session_state["inventory"]
                )
                save_data()
                st.success("已更新")

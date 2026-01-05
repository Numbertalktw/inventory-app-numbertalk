@st.cache_data(ttl=600)
def load_inventory_from_gsheet():
    """
    從 Google Sheets 讀取庫存資料 (支援英文欄位 name, spec)
    """
    client = get_google_sheet_client()
    if not client: return pd.DataFrame()

    try:
        sh = client.open(SPREADSHEET_NAME)
        worksheet = sh.sheet1 
        data = worksheet.get_all_records()

        if not data:
            st.warning("⚠️ 雲端試算表是空的，請確認內容。")
            return pd.DataFrame(columns=['名稱', '規格', '平均成本'])

        df = pd.DataFrame(data)

        # --- 關鍵修正：欄位名稱對應 (Mapping) ---
        # 讓程式能看懂你的英文欄位
        column_mapping = {
            'name': '名稱',
            'spec': '規格',
            'Name': '名稱',
            'Spec': '規格',
            'cost': '平均成本',   # 如果你有成本欄位，請確認英文是 cost 或 price
            'price': '平均成本',
            '平均成本': '平均成本'
        }
        
        # 重新命名 DataFrame 欄位
        df = df.rename(columns=column_mapping)
        
        # 防呆：確保必要欄位存在
        if '名稱' not in df.columns:
            # 如果找不到 '名稱'，嘗試看看是否因為欄位是空的，補救一下
            st.error(f"❌ 找不到「名稱」或「name」欄位。目前的欄位是：{list(df.columns)}")
            return pd.DataFrame()
            
        if '規格' not in df.columns:
            df['規格'] = "標準" # 預設值

        # 處理數值欄位 (若你的報表目前沒有成本欄位，這裡會預設為 0)
        if '平均成本' not in df.columns:
            df['平均成本'] = 0.0
        
        return df

    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"❌ 找不到名稱為 '{SPREADSHEET_NAME}' 的試算表。請確認：\n1. Google 試算表左上角的檔名已改成 '{SPREADSHEET_NAME}'\n2. 已分享給機器人信箱。")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ 讀取資料失敗: {e}")
        return pd.DataFrame()

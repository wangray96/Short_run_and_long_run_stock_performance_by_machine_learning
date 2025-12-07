import pandas as pd
from google.colab import drive

def analyze_data(file_path):
    try:
        # 讀取數據檔案，設置 low_memory=False
        drive.mount('/drive', force_remount=True)
        data = pd.read_csv(file_path, low_memory=False, encoding='utf-8')

        # 統計資料行數
        count = len(data)

        # 列出所有的 columns
        columns = data.columns.tolist()

        print(f"資料行數: {count}")
        print(f"Columns: {columns}")

        return count, columns
    except Exception as e:
        print(f"讀取檔案時發生錯誤: {e}")
        return None, None


def load_csv_and_record_rows(file_path):
    """
    讀取CSV檔案，記錄原始資料的行數，並打印列名。新增計算指定欄位的唯一值數量以及日期範圍，並標準化列名。

    Parameters:
    file_path (str): CSV檔案的路徑。

    Returns:
    DataFrame, int: 返回讀取的DataFrame和原始資料的行數。
    """
    drive.mount('/drive', force_remount=True)
    print(f"正在處理檔案: {file_path}")  # 列印出載入的檔案路徑
    data = pd.read_csv(file_path, low_memory=False, encoding='utf-8')

    # 標準化列名
    data.columns = [col.lower().strip() for col in data.columns]  # 將所有列名轉成小寫並移除多餘空白

    # 列名替換規則，可根據需求增加新的規則
    rename_dict = {
        'public_date': 'date',
        'datadate': 'date',  # 將 public_date 重命名為 date
        # 可以添加更多的替換規則，如 'old_name': 'new_standard_name',
    }

    data.rename(columns={col: rename_dict[col] for col in data.columns if col in rename_dict}, inplace=True)
    # 標準化列名
    data.columns = [col.lower().strip() for col in data.columns]  # 將所有列名轉成小寫並移除多餘空白
    # 確保識別碼列名正確
    for col in data.columns:
        if 'gvkey' in col:
            data.rename(columns={col: 'gvkey'}, inplace=True)
        elif 'permno' in col:
            data.rename(columns={col: 'permno'}, inplace=True)

    original_rows = data.shape[0]
    print(f"原始資料行數: {original_rows}")
    print(f"資料列名: {list(data.columns)}")
    print(f"資料數量: {len(data.columns)}")

    # 檢查並計算唯一標識符的數量
    unique_identifier_count = None
    identifier = ""
    if 'gvkey' in data.columns and 'permno' in data.columns:
        unique_identifier_count = data['gvkey'].nunique()
        identifier = "gvkey"
    elif 'permno' in data.columns:
        unique_identifier_count = data['permno'].nunique()
        identifier = "permno"
    elif 'gvkey' in data.columns:
        unique_identifier_count = data['gvkey'].nunique()
        identifier = "gvkey"

    if unique_identifier_count is not None:
        print(f"唯一{identifier}數量: {unique_identifier_count}")
    else:
        print("未找到 'gvkey' 或 'permno'.")

    # 檢查並打印日期範圍
    if 'date' in data.columns:
        data['date'] = pd.to_datetime(data['date'], errors='coerce')  # 轉換日期格式，並處理非法日期
        min_date = data['date'].min()
        max_date = data['date'].max()
        if pd.notna(min_date) and pd.notna(max_date):
            print(f"最遠日期: {min_date.strftime('%Y-%m-%d')}")
            print(f"最近日期: {max_date.strftime('%Y-%m-%d')}")
        else:
            print("日期資料中包含無效值或全部為空。")
    else:
        print("未找到 'date' 列。")

    return data

def summarize_columns(data, columns):
    """
    統計指定列的資料總數和空白值數量。

    參數:
    data (pd.DataFrame): 需要統計的 DataFrame。
    columns (list): 需要統計的列名列表。

    返回:
    None: 此函數僅打印統計結果，不返回任何值。
    """
    # 檢查列是否存在於 DataFrame 中
    existing_columns = [col for col in columns if col in data.columns]
    if not existing_columns:
        print("None of the specified columns exist in the DataFrame.")
        return

    # 初始化統計數據
    summary_data = {
        "Total Count": [],
        "Missing Count": [],
        "Missing Percentage": []
    }

    # 計算每列的總數和空白值數量
    for column in existing_columns:
        total_count = data[column].count()
        missing_count = data[column].isna().sum()
        missing_percentage = (missing_count / len(data[column])) * 100  # 計算缺失值百分比
        summary_data["Total Count"].append(total_count)
        summary_data["Missing Count"].append(missing_count)
        summary_data["Missing Percentage"].append(missing_percentage)

        print(f"Column '{column}': Total = {total_count}, Missing = {missing_count}, "
              f"Missing Percentage = {missing_percentage:.2f}%")


def extract_multiple_columns(input_file_path, output_file_path, column_names):
    """
    從指定的 CSV 檔案中提取多個特定的列並輸出到另一個 CSV 檔案。

    參數:
    input_file_path (str): 輸入文件的路徑。
    output_file_path (str): 輸出文件的路徑。
    column_names (list of str): 需要提取的列名列表。

    返回:
    None
    """
    try:
        # 讀取 CSV 檔案
        data = pd.read_csv(input_file_path)

        # 檢查所有指定的列是否都存在於 DataFrame 中
        missing_columns = [col for col in column_names if col not in data.columns]
        if missing_columns:
            print(f"錯誤：以下列不存在於檔案中 {missing_columns}")
        else:
            # 提取特定的多個列
            column_data = data[column_names]

            # 將提取的列數據寫入到新的 CSV 檔案
            column_data.to_csv(output_file_path, index=False)
            print(f"檔案已成功輸出到 {output_file_path}")

    except Exception as e:
        print(f"處理檔案時發生錯誤: {e}")


# financial_ratio_file_path = '/drive/MyDrive/論文/data/financial_ratio_all.csv'
IBES_file_path = '/drive/MyDrive/論文/data/financial_ratio_all_IBES.csv'
crsp_price_path = '/drive/MyDrive/論文/data/CRSP_Stock_price_Monthly_final.csv'
# financial_ratio_count, financial_ratio_columns = analyze_data(financial_ratio_file_path)
crsp_count, crsp_columns = analyze_data(crsp_price_path)
IBES_count, IBES_columns = analyze_data(IBES_file_path)

print('---'*40)
# financial_ratio = load_csv_and_record_rows(financial_ratio_file_path)
crsp = load_csv_and_record_rows(crsp_price_path)
print('---'*40)
IBES = load_csv_and_record_rows(IBES_file_path)
print('---'*40)
crsp_columns = ['permno', 'date', 'nameendt', 'shrcd', 'exchcd', 'siccd', 'ncusip', 'ticker', 'comnam', 'shrcls', 'tsymbol', 'naics', 'primexch', 'trdstat', 'secstat', 'permco', 'issuno', 'hexcd', 'hsiccd', 'cusip', 'dclrdt', 'dlamt', 'dlpdt', 'dlstcd', 'nextdt', 'paydt', 'rcrddt', 'shrflg', 'hsicmg', 'hsicig', 'distcd', 'divamt', 'facpr', 'facshr', 'acperm', 'accomp', 'shrenddt', 'nwperm', 'dlretx', 'dlprc', 'dlret', 'trtscd', 'nmsind', 'mmcnt', 'nsdinx', 'bidlo', 'askhi', 'prc', 'vol', 'ret', 'bid', 'ask', 'shrout', 'cfacpr', 'cfacshr', 'altprc', 'spread', 'altprcdt', 'retx', 'vwretd', 'vwretx', 'ewretd', 'ewretx', 'sprtrn']
summarize_columns(crsp, crsp_columns)
# summarize_columns(financial_ratio, financial_ratio_columns)
print('---'*40)
IBES_columns = ['gvkey', 'permno', 'adate', 'qdate', 'date', 'capei', 'bm', 'evm', 'pe_op_basic', 'pe_op_dil', 'pe_exi', 'pe_inc', 'ps', 'pcf', 'dpr', 'npm', 'opmbd', 'opmad', 'gpm', 'ptpm', 'cfm', 'roa', 'roe', 'roce', 'efftax', 'aftret_eq', 'aftret_invcapx', 'aftret_equity', 'pretret_noa', 'pretret_earnat', 'gprof', 'equity_invcap', 'debt_invcap', 'totdebt_invcap', 'capital_ratio', 'int_debt', 'int_totdebt', 'cash_lt', 'invt_act', 'rect_act', 'debt_at', 'debt_ebitda', 'short_debt', 'curr_debt', 'lt_debt', 'profit_lct', 'ocf_lct', 'cash_debt', 'fcf_ocf', 'lt_ppent', 'dltt_be', 'debt_assets', 'debt_capital', 'de_ratio', 'intcov', 'intcov_ratio', 'cash_ratio', 'quick_ratio', 'curr_ratio', 'cash_conversion', 'inv_turn', 'at_turn', 'rect_turn', 'pay_turn', 'sale_invcap', 'sale_equity', 'sale_nwc', 'rd_sale', 'adv_sale', 'staff_sale', 'accrual', 'ptb', 'peg_trailing', 'divyield', 'peg_1yrforward', 'peg_ltgforward', 'ticker', 'cusip']
summarize_columns(IBES, IBES_columns)

print('---'*40)
output_file_path = '/drive/MyDrive/論文/data/output_IBES.csv'
column_names = ['gvkey', 'permno', 'cusip', 'TICKER', 'public_date', 'bm', 'evm', 'pe_exi', 'pe_inc',
                'ptb', 'GProf', 'gpm', 'npm', 'opmad', 'roa', 'roe', 'cfm'
                , 'cash_debt', 'short_debt', 'curr_debt', 'de_ratio', 'debt_at', 'quick_ratio',
                'curr_ratio', 'rect_turn', 'at_turn', 'rd_sale']
extract_multiple_columns(IBES_file_path, output_file_path, column_names)
print('---'*40)
crsp_output_file_path = '/drive/MyDrive/論文/data/output_crsp.csv'
column_names = ['PERMNO', 'date', 'NCUSIP', 'TICKER', 'COMNAM', 'CUSIP', 'PRC']
extract_multiple_columns(crsp_price_path, crsp_output_file_path, column_names)

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



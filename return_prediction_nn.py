import os
import random
import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error
from dateutil.relativedelta import relativedelta
import time
import psutil
from google.colab import drive

drive.mount('/drive', force_remount=True)

# 設定隨機種子，確保結果可重現
def set_random_seed(seed):
    os.environ['PYTHONHASHSEED'] = str(seed)
    random.seed(seed)
    np.random.seed(seed)
    tf.random.set_seed(seed)

# 定義訓練 + 預測函式（使用 rolling window）
def train_and_predict(data, start_date, end_date, prediction_date, growth_period, seed=10):
    set_random_seed(seed)

    # 擷取訓練視窗資料
    window_data = data.loc[start_date:end_date]
    if window_data.empty:
        print(f"📭 無可用資料區間：{start_date} ~ {end_date}")
        return None, None, None, None, None

    # 取 permno 與 ncusip 做唯一標識
    stock_ids = window_data[['permno', 'ncusip']].copy()

    # 分離特徵與目標變數
    X = window_data[['bm', 'pe_exi', 'pe_inc', 'ptb', 'gprof', 'gpm',
                     'npm', 'opmad', 'roa', 'roe', 'cfm', 'cash_debt',
                     'short_debt', 'curr_debt', 'de_ratio', 'debt_at',
                     'quick_ratio', 'curr_ratio', 'rect_turn', 'at_turn', 'rd_sale']]

    y = window_data[f'PRC GROWTH {growth_period}m']

    # 分割訓練集 / 驗證集
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.1, random_state=seed
    )

    # 特徵標準化
    sc = StandardScaler()
    X_train = sc.fit_transform(X_train)
    X_val = sc.transform(X_val)

    # 建立神經網路模型
    model = tf.keras.models.Sequential([
        tf.keras.layers.Dense(10, activation='relu', input_shape=(X_train.shape[1],)),
        tf.keras.layers.Dense(1)
    ])

    # 編譯模型
    model.compile(optimizer='adam', loss='mse')

    # 訓練模型
    start_time = time.time()
    model.fit(X_train, y_train, validation_data=(X_val, y_val),
              epochs=50, batch_size=512, verbose=2)
    end_time = time.time()

    training_time = end_time - start_time

    # 準備預測資料
    prediction_data = data[data.index == prediction_date]
    if prediction_data.empty:
        print(f"📭 無預測日期資料：{prediction_date}")
        return None, None, None, None

    X_pred = prediction_data[['bm', 'pe_exi', 'pe_inc', 'ptb', 'gprof', 'gpm',
                              'npm', 'opmad', 'roa', 'roe', 'cfm', 'cash_debt',
                              'short_debt', 'curr_debt', 'de_ratio', 'debt_at',
                              'quick_ratio', 'curr_ratio', 'rect_turn', 'at_turn', 'rd_sale']]

    X_pred = sc.transform(X_pred)

    # 進行預測
    y_pred = model.predict(X_pred).flatten()

    return y_pred, prediction_data[f'PRC GROWTH {growth_period}m'], \
           prediction_data[['permno', 'ncusip']], X_pred, training_time


# 依序處理不同視窗與不同預測年期
def process_all_windows_and_years(base_path):
    growth_periods = [6, 9]  # 預測 6 個月與 9 個月報酬
    prediction_years = [2, 3, 4, 5]  # 預測 horizon：2、3、4、5 年

    for growth_period in growth_periods:
        dataset_path = f'/drive/MyDrive/論文/data/final_result_{growth_period}m.csv'

        if os.path.exists(dataset_path):
            dataset = pd.read_csv(dataset_path)
            dataset['date'] = pd.to_datetime(dataset['date'])
            dataset = dataset.sort_values('date')
            dataset.set_index('date', inplace=True)

            # 移除不需要的欄位
            dataset = dataset.drop(columns=['evm'])

            for years in prediction_years:

                # 找出最早日期
                earliest_date = dataset.index.min()
                print(f"📅 資料最早日期：{earliest_date}")

                # 設定第一個預測日期
                prediction_date = earliest_date + relativedelta(years=years)

                all_results = []
                total_training_time = 0
                start_memory = psutil.virtual_memory().used

                # 每月滾動預測
                while prediction_date <= dataset.index.max():

                    start_date = prediction_date - relativedelta(years=years) + relativedelta(days=1)
                    end_date = prediction_date - relativedelta(days=1)

                    y_pred, y_true, stock_ids, X_pred, training_time = \
                        train_and_predict(dataset, start_date, end_date, prediction_date, growth_period)

                    total_training_time += training_time

                    if y_pred is not None and y_true is not None:
                        results_df = pd.DataFrame({
                            'permno': stock_ids['permno'].values,
                            'ncusip': stock_ids['ncusip'].values,
                            'Date': [prediction_date] * len(y_pred),
                            'True Values': y_true.values,
                            'Predicted Values': y_pred,
                            'Window Start': [start_date] * len(y_pred),
                            'Window End': [end_date] * len(y_pred)
                        })

                        all_results.append(results_df)
                        print(f"📌 已完成日期 {prediction_date} 的預測")

                    prediction_date += relativedelta(months=1)

                # 計算記憶體使用量
                end_memory = psutil.virtual_memory().used

                # 合併所有預測結果
                final_results_df = pd.concat(all_results)

                # 計算整體 RMSE
                overall_rmse = np.sqrt(mean_squared_error(
                    final_results_df['True Values'], final_results_df['Predicted Values']
                ))
                print(f"📉 整體 RMSE：{overall_rmse}")

                # 計算整體 MAE
                overall_mae = mean_absolute_error(
                    final_results_df['True Values'], final_results_df['Predicted Values']
                )
                print(f"📉 整體 MAE：{overall_mae}")

                print(f"⏱️ 總訓練時間：{total_training_time} 秒")
                print(f"🧠 記憶體使用量：{end_memory - start_memory} bytes")

                # 輸出結果
                final_results_filename = f'/drive/MyDrive/論文/data/NN1_{growth_period}M_{years}y_max_predictions_vs_true_val_adjust.csv'
                final_results_df.to_csv(final_results_filename, index=False)
                print(f"📁 結果已儲存：{final_results_filename}")


# 執行所有視窗與預測年份
process_all_windows_and_years('/drive/MyDrive/論文/data/')

"""
データベースを初期化するためのスクリプト
"""
import datetime
import sqlite3
import time
import urllib.request

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm


def fetch_stock_values(code):
    """
    株価のデータフレームを取得する
    """
    df = pd.DataFrame([], columns=['open', 'high', 'low', 'close', 'volume'])

    # 最初の１ページ目を処理
    url = f'https://kabutan.jp/stock/kabuka?code={code}&page=1'
    with urllib.request.urlopen(url) as res:
        soup = BeautifulSoup(res, 'html.parser')

    # 本日の日付を取得
    v_date_today = soup.find("table", class_="stock_kabuka0").find("time")
    v_date_today = v_date_today["datetime"]
    v_date_today_df = pd.DataFrame({'date': [v_date_today]})

    # 本日の株価を取得
    v_values_today = soup.find(
        "table", class_="stock_kabuka0").find_all("td")
    v_values_today_list = [
        v_values_today[0].text,
        v_values_today[1].text,
        v_values_today[2].text,
        v_values_today[3].text,
        v_values_today[6].text,
    ]
    v_values_today_df = pd.DataFrame(
        [v_values_today_list],
        columns=['open', 'high', 'low', 'close', 'volume']
    )

    # 日付と株価を結合
    df_today = pd.concat([v_date_today_df, v_values_today_df], axis=1)

    df = pd.DataFrame([], columns=['open', 'high', 'low', 'close', 'volume'])

    for i in range(1, 11):

        url = f'https://kabutan.jp/stock/kabuka?code={code}&page={i}'
        with urllib.request.urlopen(url) as res:
            soup = BeautifulSoup(res, 'html.parser')

        # 日付を取得
        s_time = soup.find_all("time")
        for j in range(9, len(s_time)):
            try:
                d_dt = datetime.datetime.strptime(s_time[j].text, "%y/%m/%d")
                d_str = d_dt.strftime('%Y-%m-%d')
                y_str = (datetime.datetime.today() -
                         datetime.timedelta(1)).strftime('%Y-%m-%d')
                if d_str == y_str:
                    idx = j
                    break
            except ValueError:
                pass
        v_date = [d['datetime'] for d in soup.find_all("time")][idx:]
        v_date_df = pd.DataFrame({'date': v_date}).iloc[::-1]

        # 株価を取得
        v_values = soup.find("table", class_="stock_kabuka_dwm").find_all("td")
        v_values_list = []
        for j in range(0, len(v_date)):
            v_open = v_values[7 * j].text
            v_high = v_values[7 * j + 1].text
            v_low = v_values[7 * j + 2].text
            v_close = v_values[7 * j + 3].text
            v_volume = v_values[7 * j + 6].text
            v_values_list.append([
                v_open, v_high, v_low, v_close, v_volume,
            ])
        v_values_df = pd.DataFrame(
            v_values_list,
            columns=['open', 'high', 'low', 'close', 'volume']
        )

        # 日付と株価を結合
        df = pd.concat(
            [pd.concat([v_date_df, v_values_df], axis=1), df], axis=0
        )

    # 本日の株価を末尾に結合する
    df = pd.concat([df, df_today], axis=0)

    return df


if __name__ == '__main__':

    # CSV を読み込む
    codes_df = pd.read_csv('codes.csv')

    # データベースに保存
    conn = sqlite3.connect('stocks.db')
    with conn:
        codes_df.to_sql('Symbols', conn, if_exists='replace', index=False)

    # プログレスバーを定義
    bar = tqdm(total=len(codes_df), dynamic_ncols=True,
               iterable=True, leave=False)
    bar.set_description('データを取得しています')

    for code in codes_df['code']:

        # 株価のデータフレームを取得
        values_df = fetch_stock_values(code)

        # データベースに保存
        conn = sqlite3.connect('stocks.db')
        with conn:
            values_df.to_sql(code, conn, if_exists='replace', index=False)

        bar.update(1)
        time.sleep(1)

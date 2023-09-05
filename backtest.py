import sqlite3

import pandas as pd


def load_codes_dataframe():
    """
    銘柄コードのデータフレームを取得する
    """
    query = 'select code from Symbols;'
    conn = sqlite3.connect('stocks.db')
    with conn:
        df = pd.read_sql_query(query, conn)

    return df


def load_value_dataframe(code, column):

    query = f'select {column} from \"{code}\" order by date;'
    conn = sqlite3.connect('stocks.db')
    with conn:
        df = pd.read_sql_query(query, conn)

    return df


def calc_moving_average(df):

    df = df['close'].str.replace(',', '').astype(int)
    df75 = df.rolling(window=75).mean().dropna()
    df25 = df.rolling(window=25).mean().dropna()
    df5 = df.rolling(window=5).mean().dropna()
    df = pd.concat([df75, df25, df5], axis='columns', join='inner')
    df.columns = ['m75', 'm25', 'm05']

    return df


if __name__ == '__main__':

    # 銘柄コードのデータフレームを取得する
    codes_df = load_codes_dataframe()

    for code in codes_df['code']:

        # 終値をロードする
        df_close = load_value_dataframe(code, 'close')

        # 移動平均を計算する
        df_close_ave = calc_moving_average(df_close)

        breakpoint()

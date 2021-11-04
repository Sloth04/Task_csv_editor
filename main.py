import os
import pandas as pd
from glob import glob
import numpy as np

look_for_code = {'62W4960182644872': 'Канівська ГЕС', '62W8027183829970': 'Кременчуцька ГЕС',
                 '62W578426156126U': 'Дніпро-1 ГЕС', '62W380785590623K': 'Дніпро-2 ГЕС',
                 '62W8116841028205': 'Каховська ГЕС', '62W3963431638476': 'Середньодніпровська ГЕС',
                 '62W1502413906970': 'Київська ГЕС', '62W601196275466X': 'Курахівська ТЕС',
                 '62W909789678127W': 'Запорізька ТЕС', '62W29095992560BC': 'Бурштинська ТЕС (Бурштин)',
                 '62W4203004736348': 'ХТЕЦ-5'}
look_for_type = 'A01'


def find_and_save(df, target_dic, target_type):
    step = 0
    df_re = pd.DataFrame()
    df_re.index = df.index
    for item in range(df.shape[1]):
        temp_list = df.columns[item].split(' ')
        if target_type in temp_list:
            df_re = pd.concat([df_re, df[df.columns[item]]], axis=1)
            df_re.rename(columns={df_re.columns[step]: temp_list[2] + ' ' + temp_list[3]}, inplace=True)
            step += 1
    df.fillna(0, inplace=True)
    df.replace(np.nan, 0, regex=True, inplace=True)
    print(df_re)
    # print(step)
    return df_re


def unite(list_df):
    united_df = pd.concat(list_df)
    # print(united_df)
    return united_df


def create_df(name):
    df = pd.read_csv(name, sep=';', index_col=0)
    # columns = df.columns
    # for col in columns:
    #     temp_list = df.columns[col].split(' ')
    #     col_list = col.split()
    #     if not col_list[-5] in look_for_code.keys():
    #         continue
    #     elif look_for_type in col_list:
    #         continue
    #     version = int(col_list[-1])
    #     if target_type in temp_list:
    #         df_re = pd.concat([df_re, df[df.columns[item]]], axis=1)
    #         df_re.rename(columns={df_re.columns[step]: temp_list[2] + ' ' + temp_list[3]}, inplace=True)
    df.index.name = 'Date'
    df.sort_index(axis=1)
    df.index = df.index.map(lambda x: str(x)[:-8])
    df.index = pd.to_datetime(df.index)
    df = df.groupby(pd.Grouper(freq='H')).mean()
    # print(df)
    return df


def main():
    list_csv = []

    cwd = os.path.dirname(os.path.abspath(__file__))
    target = os.path.join(cwd, "in", '*08.csv')
    dir_list = glob(target)
    for item in dir_list:
        name_df = create_df(item)
        list_csv.append(name_df)
    # print(list_csv)
    united_df = unite(list_csv)
    selected_df = find_and_save(united_df, look_for_code, look_for_type)
    # selected_df.to_csv('output_result.csv', sep=';')
    # selected_df.to_excel('output_result.xlsx')


if __name__ == '__main__':
    main()

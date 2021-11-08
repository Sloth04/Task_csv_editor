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


def create_df(name):
    df = pd.read_csv(name, sep=';', index_col=0)
    df.index.name = 'Date'
    df.sort_index(axis=1)
    df.index = df.index.map(lambda x: str(x)[:-8])
    df.index = pd.to_datetime(df.index)
    df = df.groupby(pd.Grouper(freq='H')).mean()
    columns = df.columns
    df_re = pd.DataFrame()
    ver = dict.fromkeys(look_for_code.keys())
    ver_i = dict.fromkeys(look_for_code.keys())
    for item in ver.keys():
        ver[item] = 0
        ver_i[item] = 0
    item = 0
    for col in columns:
        item += 1
        col_list = col.split()
        if look_for_type not in col_list:
            continue
        elif not col_list[col_list.index(look_for_type) - 1] in look_for_code.keys():
            continue
        wc = col_list[col_list.index(look_for_type) - 1]
        if float(col_list[-1]) > float(ver.get(wc)):
            ver.update({wc: col_list[-1]})
            ver_i.update({wc: item})
    added = 0
    for i in ver_i.keys():
        df_re = pd.concat([df_re, df[df.columns[ver_i.get(i)-1]]], axis=1)
        df_re.rename(columns={df_re.columns[added]: i + ' ' + look_for_code[i]}, inplace=True)
        added += 1
    df_re.fillna(0, inplace=True)
    df_re.replace(np.nan, 0, regex=True, inplace=True)
    return df_re


def main():
    list_csv = []

    cwd = os.path.dirname(os.path.abspath(__file__))
    target = os.path.join(cwd, "in", '*08.csv')
    dir_list = glob(target)
    for item in dir_list:
        name_df = create_df(item)
        list_csv.append(name_df)
    united_df = pd.concat(list_csv)
    united_df.to_csv('output_result.csv', sep=';')


if __name__ == '__main__':
    main()

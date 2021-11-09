import os
import pandas as pd
from glob import glob
import numpy as np
import re

look_for_code = {'62W1502413906970': 'KYIVHPP', '62W4960182644872': 'KANIVHPP', '62W8027183829970': 'KREMHPP',
                 '62W3963431638476': 'SEREDHPP', '62W578426156126U': 'DNIP1HPP', '62W380785590623K': 'DNIP2HPP',
                 '62W8116841028205': 'KAKHHPP', '62W29095992560BC': 'BURSHTPP-BEI', '62W601196275466X': 'KURTPP',
                 '62W909789678127W': 'ZAPTPP', '62W4203004736348': 'KHAR5CHPP'}
look_for_type = ['A01']


def create_df(name):
    df = pd.read_csv(name, sep=';', index_col=0)
    df.index.name = 'Date'
    df.sort_index(axis=1)
    df.index = df.index.map(lambda x: str(x)[:-8])
    df.index = pd.to_datetime(df.index)
    df = df.groupby(pd.Grouper(freq='H')).mean()
    columns = df.columns
    df_re = pd.DataFrame()
    ver_d = dict.fromkeys(look_for_code.keys())
    ver_i = dict.fromkeys(look_for_code.keys())
    code_reg = r"\w{2}[W]+\w{13}"
    type_reg = r"[A]+\d{2}"
    ver_reg = r"\d+.\d+$"
    for i in ver_d.keys():
        ver_d[i] = 0
        ver_i[i] = 0
    item = 0
    for col in columns:
        item += 1
        found_code = re.findall(code_reg, col)
        found_type = re.findall(type_reg, col)
        found_ver = re.findall(ver_reg, col)
        if not found_code or not found_type or not found_ver:
            continue
        if found_type[0] not in look_for_type:
            continue
        elif not found_code[0] in look_for_code.keys():
            continue
        if float(found_ver[0]) > float(ver_d.get(found_code[0])):
            ver_d.update({found_code[0]: found_ver[0]})
            ver_i.update({found_code[0]: item})
    added = 0
    for i in ver_i.keys():
        df_re = pd.concat([df_re, df[df.columns[ver_i.get(i)-1]]], axis=1)
        df_re.rename(columns={df_re.columns[added]: i + ' ' + look_for_code[i]}, inplace=True)
        added += 1
    df_re.fillna(0, inplace=True)
    df_re.replace(np.nan, 0, regex=True, inplace=True)
    # print(df_re)
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
    # print(united_df)
    united_df.to_csv('output_result.csv', sep=';')


if __name__ == '__main__':
    main()

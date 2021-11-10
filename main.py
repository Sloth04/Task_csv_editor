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
    df_re = pd.DataFrame()
    df = pd.read_csv(name, sep=';', index_col=0)
    df.sort_index(axis=1)
    df.index = df.index.map(lambda x: str(x)[:-8])
    df.index = pd.to_datetime(df.index)
    df = df.groupby(pd.Grouper(freq='H')).mean()
    ver = {k: {'index': 0, 'version': 0} for k in look_for_code}
    pattern = re.compile(r'(\w{2}[W]+\w{13}).*A01.*(\d+.\d+)')
    for i, col in enumerate(df.columns):
        if not pattern.findall(col):
            continue
        result = pattern.findall(col)[0]
        if result[0] not in look_for_code.keys():
            continue
        if float(result[1]) > float(ver.get(result[0]).get('version')):
            ver.update({result[0]: {'index': i, 'version': result[1]}})
    for i, w in enumerate(look_for_code.keys()):
        df_re = pd.concat([df_re, df[df.columns[ver.get(w).get('index')]]], axis=1)
        df_re.rename(columns={df_re.columns[i]: f'{w} {look_for_code[w]}'}, inplace=True)
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

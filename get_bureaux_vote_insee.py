import pandas as pd
import numpy as np
from ArangoDB import ArangoDB

def convert_to_integer(x):
    try:
        return int(x)
    except:
        return np.nan

def import_raw_data(filename:str, sep:str=",")->pd.DataFrame:
    return pd.read_csv(filename, sep=sep, dtype=str)

def reformat_dataframe(raw_df:pd.DataFrame)->pd.DataFrame:
    df = raw_df.copy()
    df['code_int'] = df['code'].map(convert_to_integer)
    df = df.sort_values(['commune_code', 'code_int', 'code'])
    df['code_id'] = df.groupby('commune_code').cumcount()+1
    df['id'] = df['commune_code'].map(str) + "-" + df['code_id'].map(str)
    df.drop(columns=["code_id", "code_int"], inplace=True)
    return df.set_index('id')

if __name__ == '__main__':
    raw_df = import_raw_data("data/raw/Bureaux_de_vote.csv", sep=";")
    df = reformat_dataframe(raw_df)
    df.to_csv("data/verified/Bureaux_de_vote.csv", sep=";", encoding="utf-8")

    db = ArangoDB()
    db.import_csv(filename="data/verified/Bureaux_de_vote.csv", index_col_name="id", collection_name="Bureau_Vote")

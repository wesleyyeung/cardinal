
import pandas as pd
import numpy as np
from cleaning.base_cleaner import BaseCleaner
import yaml
from utils import flatten
from hash import hash_sha1

class CISCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()


    def cis_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[df['Procedure Date'] == '00/00/00','Procedure Date'] = np.nan
        df['Procedure Date'] = pd.to_datetime(df['Procedure Date'], errors='coerce')

        if 'Study Num' in df.columns:
            df['Study Num'] = df['Study Num'].apply(lambda row: hash_sha1(row))

        for col in ['RF_IDRef', 'NEXUSIDRef', 'VIP']:
            df.drop(columns=col, inplace=True, errors='ignore')

        df.rename(columns={
            'HospitalID': 'Hospital',
            'Proc_HospitalID': 'Hospital',
            'Procedure Date.1': 'Procedure Date',
            'Procedure_Hospital': 'Hospital'
        }, inplace=True)

        header = ['IDRef', 'Study Num', 'Procedure Date']
        other_cols = [col for col in df.columns if col not in header]
        df = df[header + other_cols]
        return df
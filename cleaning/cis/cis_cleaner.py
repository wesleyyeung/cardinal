
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
        if 'Study Num' in df.columns:
            df['Study Num'] = df['Study Num'].apply(lambda row: hash_sha1(row))

        for col in ['RF_IDRef', 'NEXUSIDRef', 'VIP','Original Study Num','ResCode']:
            df.drop(columns=col, inplace=True, errors='ignore')

        df.rename(columns={
            'HospitalID': 'Hospital',
            'Proc_HospitalID': 'Hospital',
            'Procedure_Hospital': 'Hospital'
        }, inplace=True)

        #Convert datetime columns
        df.loc[:,'Procedure Date'] = pd.to_datetime(df['Procedure Date'], format='mixed', errors='coerce')
        df.loc[:,'Procedure Date'] = df.loc[:,'Procedure Date'].astype(str)

        #Rename columns
        header = ['IDRef', 'Study Num']
        if 'Procedure Date' in list(df):
            header = ['IDRef', 'Study Num', 'Procedure Date']
        if 'Hospital' in list(df):
            header = header + ['Hospital']
        
        other_cols = [col for col in df.columns if col not in header]
        df = df[header + other_cols]
        return df.rename(columns={
            'IDRef':'subject_id',
            'Study Num':'study_id',
            'Procedure Date':'procedure_date',
            'Procedure2':'procedure_name',
            'Hospital':'hospital'
        })

import pandas as pd
import numpy as np
from lib.cleaning.base_cleaner import BaseCleaner
from lib.utils import flatten, hash_sha1

class CISCleaner(BaseCleaner):
    def __init__(self, phi_dictionary):
        self.phi_dictionary = phi_dictionary
        self.key_id_cols = self._extract_key_columns()

    def _extract_key_columns(self):
        all_columns = flatten(self.phi_dictionary.values())
        key_id_cols = []
        for col in all_columns:
            for substr in ['idref', 'hospital', 'study num', 'procedure date']:
                if substr in col.lower():
                    key_id_cols.append(col)
        key_id_cols = list(set(key_id_cols))
        if 'Original Study Num' in key_id_cols:
            key_id_cols.remove('Original Study Num')
        return key_id_cols

    def clean(self, df: pd.DataFrame, filename: str) -> pd.DataFrame:
        df.loc[df['Procedure Date'] == '00/00/00','Procedure Date'] = np.nan
        df['Procedure Date'] = pd.to_datetime(df['Procedure Date'], errors='coerce')

        exclude = self.phi_dictionary.get(filename, [])
        exclude = [col for col in exclude if col not in self.key_id_cols and col in df.columns]
        df = df.drop(columns=exclude, errors='ignore')

        if 'Study Num' in df.columns:
            df['Study Num'] = df['Study Num'].apply(lambda row: hash_sha1(row))

        for col in ['RF_IDRef', 'NEXUSIDRef', 'IDRef_sha2', 'VIP']:
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

        df = self.sanitize_dataframe_columns(df)  
        return df.drop_duplicates(keep='first') 
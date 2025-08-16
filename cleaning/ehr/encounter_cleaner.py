import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHREncounterCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()

    @staticmethod
    def sanitize_numeric_codes(input):
        try:
            return str(int(float(input)))
        except Exception as e:
            if pd.isnull(input):
                return '-9999'
            elif ',' in str(input):
                return str(input).replace('.0','')
            else:
                print(f'Unable to parse input: {input}; original exception {e}')
                raise

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df['primary_diagnosis'] = df['primary_diagnosis'].apply(lambda row: self.sanitize_numeric_codes(row))

        #Check for non-converted exponential terms
        if any(df['primary_diagnosis'].str.contains('e')):
            if df['primary_diagnosis'][df['primary_diagnosis'].str.contains('e').fillna(False)].tolist():
                print(df['primary_diagnosis'][df['primary_diagnosis'].str.contains('e').fillna(False)].tolist())
                raise
        #Check for non-converted decimal points
        if any(df['primary_diagnosis'].str.contains(r'\.')):
            if df['primary_diagnosis'][df['primary_diagnosis'].str.contains(r'\.').fillna(False)].tolist():
                print(df['primary_diagnosis'][df['primary_diagnosis'].str.contains(r'\.').fillna(False)].tolist())
                raise
        return df.rename(columns={
            'mrn_sha1':'subject_id'
        })
import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class CCDOHCACleaner(BaseCleaner):
    def __init__(self):
        super().__init__()

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df['arrival_dt'] = df['Date of arrival at ED'] + ' ' + df['Time of arrival at ED']
        df['arrival_dt'] = pd.to_datetime(df['arrival_dt'],format="%d/%m/%Y %H:%M:%S",errors='coerce')
        df['Date of Discharge or Death'] = pd.to_datetime(df['Date of Discharge or Death'],format="%d/%m/%Y", errors='coerce')
        return df.rename(columns={
            'mrn_sha1':'subject_id'
        })
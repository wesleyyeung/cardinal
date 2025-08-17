import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class CCDOHCACleaner(BaseCleaner):
    def __init__(self):
        super().__init__()

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df['time_of_arrival_at_ed'] = df['time_of_arrival_at_ed'].apply(lambda row: row if sum([1 if char == ':' else 0 for char in row]) == 2 else row +':00')
        df['arrival_dt'] = pd.to_datetime(df['date_of_arrival_at_ed'] + ' ' + df['time_of_arrival_at_ed'],format='%d/%m/%Y %H:%M:%S',errors='raise')
        df['Date of Discharge or Death'] = pd.to_datetime(df['Date of Discharge or Death'],format="%d/%m/%Y", errors='coerce')
        return df.rename(columns={
            'mrn_sha1':'subject_id'
        })
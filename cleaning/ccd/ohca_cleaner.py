import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class CCDOHCACleaner(BaseCleaner):
    def __init__(self):
        super().__init__()

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df['Time of arrival at ED'] = df['Time of arrival at ED'].apply(lambda row: row if sum([1 if char == ':' else 0 for char in row]) == 2 else row +':00')
        df['arrival_dt'] = pd.to_datetime(df['Date of arrival at ED'] + ' ' + df['Time of arrival at ED'],format='mixed',dayfirst=True,errors='raise')
        df['Date of Discharge or Death'] = pd.to_datetime(df['Date of Discharge or Death'],format="mixed",dayfirst=True,errors='coerce')
        return df.rename(columns={
            'mrn_sha1':'subject_id',
            'Time of arrival at ED':'time_of_arrival_at_ed',
            'Date of arrival at ED':'date_of_arrival_at_ed',
            'Date of Discharge or Death':'date_of_discharge_or_death'
        })
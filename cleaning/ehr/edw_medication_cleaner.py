import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHREDWMedicationCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()
        self.dt_format_dict = {
            'CREATE_DATE': '%Y-%m-%d',
            'STARTTIME': '%Y-%m-%d',
            'ENDTIME': '%Y-%m-%d'
        }

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop(columns='ADMIN_TIME', errors='ignore')
        return df
import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHRPatientCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        self.dt_cols = [col for col in df.columns if ('dt' in col.lower() or 'onset' in col.lower())]
        return df
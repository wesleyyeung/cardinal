import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHRDTSDischargeMedicationCleaner(BaseCleaner):
    
    def __init__(self):
        super().__init__()
        self.dt_cols = ['medication_start_dt','medication_end_dt','created_dt']

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHRDTSMedicationNonFormularyCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()
        self.dt_cols = ['first_ordered_dt','first_start_dt','last_end_dc_dt']
        
    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in ['order_locations','order_types','order_statuses']:
            if col in df.columns:
                df[col] = df[col].apply(lambda row: '|'.join(set(row.split('|'))) if isinstance(row, str) else row)
        return df

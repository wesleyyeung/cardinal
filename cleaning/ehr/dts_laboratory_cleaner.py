import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHRDTSLaboratoryCleaner(BaseCleaner):
    
    def __init__(self):
        super().__init__()
        self.dt_cols = [
            'msg_dt','lr_created_dt','order_dt','specimen_collected_dt',
            'speciment_received_dt','examination_dt','report_dt',
            'lri_created_dt','created_dt'
        ]

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        return df
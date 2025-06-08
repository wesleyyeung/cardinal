import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHREDWOutpatientMedicationCleaner(BaseCleaner):

    def __init__(self):
        super().__init__()
        self.dt_cols = ['MODIFICATION_DATE_TIME']

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        return df
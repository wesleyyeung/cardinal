import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class ECGMuseCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()
        self.dt_cols = ['RestingECG/TestDemographics/AcquisitionDate']

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

import pandas as pd
from lib.cleaning.base_cleaner import BaseCleaner

class ECGCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()

    def clean(self, df: pd.DataFrame, df_type: str) -> pd.DataFrame:
        func_map = {
            'iecg.csv': self.clean_iecg,
            'muse.csv': self.clean_muse
        }

        if df_type not in func_map:
            raise ValueError(f"Unknown df_type: {df_type}")

        df = func_map[df_type](df)
        df = self.sanitize_dataframe_columns(df)
        return df.drop_duplicates(keep='first')

    def clean_iecg(self, df: pd.DataFrame) -> pd.DataFrame:
        dt_cols = ['restingecgdata/dataacquisition/@date']
        df = self.standardize_dates(df, dt_cols)
        return df
    
    def clean_muse(self, df: pd.DataFrame) -> pd.DataFrame:
        dt_cols = ['RestingECG/TestDemographics/AcquisitionDate']
        df = self.standardize_dates(df, dt_cols)
        return df
        
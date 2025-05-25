import pandas as pd
from lib.cleaning.base_cleaner import BaseCleaner

class CCDCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()

    def clean(self, df: pd.DataFrame, df_type: str) -> pd.DataFrame:
        func_map = {
            'ohca.csv': self.clean_ohca
        }

        if df_type not in func_map:
            raise ValueError(f"Unknown df_type: {df_type}")

        df = func_map[df_type](df)
        df = self.sanitize_dataframe_columns(df)
        print(df.columns)
        raise
        return df.drop_duplicates(keep='first')

    def clean_ohca(self, df: pd.DataFrame) -> pd.DataFrame:
        dt_cols = ['Date of arrival at ED', 'Date of Discharge or Death']
        df = self.standardize_dates(df, dt_cols)
        return df
import re
import numpy as np
import pandas as pd
from ..utils import ColumnNameSanitizer
from itertools import compress
import yaml

class BaseCleaner:
    
    def standardize_dates(self, df: pd.DataFrame, dt_cols: dict) -> pd.DataFrame:
        for col in dt_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        return df
    
    def standardize_dates_from_dict(self, df: pd.DataFrame, dt_format_dict: dict) -> pd.DataFrame:
        for col,fmt in dt_format_dict.items():
            df[col] = pd.to_datetime(df[col],format=fmt,errors='coerce')
        return df
    
    def get_filename_suffix(self, df: pd.DataFrame, dt_cols) -> str:
        #Get max datetime
        max_date = None
        for col in dt_cols:
            try:
                max_date = self.standardize_dates(df,[col]).max()
            except:
                continue

        if pd.isnull(max_date):
            return "undated"
        return max_date.strftime("%Y-%m-%d")

    def sanitize_dataframe_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Sanitize all column names
        max_col_length = max([len(col) for col in df.columns.to_list()])
        if max_col_length >= 59:
            new_column_dict = ColumnNameSanitizer().sanitise(df.columns,simple=False) # Handle long column names
        else: 
            new_column_dict = ColumnNameSanitizer().sanitise(df.columns,simple=True)

        check_column_lengths = [len(col)>=59 for col in new_column_dict.values()]
        if any(check_column_lengths):
            long_column_names = list(compress(new_column_dict.values(), check_column_lengths))
            raise ValueError(f'Columns: {long_column_names} exceed 59 characters and cannot be ingested into PSQL')

        df.rename(columns=new_column_dict,inplace=True)

        # Drop duplicate column names, keeping first occurrence
        _, unique_indices = np.unique(df.columns, return_index=True)
        df = df.iloc[:, sorted(unique_indices)]
        return df
    
    
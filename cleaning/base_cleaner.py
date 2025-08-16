import numpy as np
import pandas as pd
from utils import ColumnNameSanitizer
from itertools import compress

class BaseCleaner:
    
    def __init__(self, destination_schema = None, destination_tablename = None):
        self.ABBREVIATIONS = {}
        self.dt_cols = []
        self.dt_format_dict = {}
        self.destination_schema = destination_schema
        self.destination_tablename = destination_tablename

    def clean(self, df: pd.DataFrame) -> tuple:
        df = df.drop_duplicates(keep='first') 
        df = self.custom_clean(df) #implemented by subclasses
        if not self.dt_format_dict: # if empty
            df = self.standardize_dates(df=df,dt_cols=self.dt_cols)
        else:
            df = self.standardize_dates_from_dict(df=df,dt_format_dict=self.dt_format_dict)
        df = self.sanitize_dataframe_columns(df=df,abbreviations=self.ABBREVIATIONS)
        return df, self.destination_schema, self.destination_tablename

    def standardize_dates(self, df: pd.DataFrame, dt_cols: list = []) -> pd.DataFrame:
        if not dt_cols: # if empty
            dt_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
        for col in dt_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        return df
    
    def standardize_dates_from_dict(self, df: pd.DataFrame, dt_format_dict: dict) -> pd.DataFrame:
        for col,fmt in dt_format_dict.items():
            df[col] = pd.to_datetime(df[col],format=fmt,errors='coerce')
        return df

    def sanitize_dataframe_columns(self, df: pd.DataFrame,abbreviations: dict) -> pd.DataFrame:
        # Sanitize all column names
        max_col_length = max([len(col) for col in df.columns.to_list()])
        if max_col_length >= 59:
            new_column_dict = ColumnNameSanitizer(abbreviations=abbreviations).sanitise(df.columns.tolist(),simple=False) # Handle long column names
        else: 
            new_column_dict = ColumnNameSanitizer().sanitise(df.columns.tolist(),simple=True)
        check_column_lengths = [len(col)>=59 for col in new_column_dict.values()]
        if any(check_column_lengths):
            long_column_names = list(compress(new_column_dict.values(), check_column_lengths))
            raise ValueError(f'Columns: {long_column_names} exceed 59 characters and cannot be ingested into PSQL')

        df.rename(columns=new_column_dict,inplace=True)

        # Drop duplicate column names, keeping first occurrence
        _, unique_indices = np.unique(df.columns, return_index=True)
        df = df.iloc[:, sorted(unique_indices)]
        return df
    
    def custom_clean(self, df: pd.DataFrame):
        return df
    
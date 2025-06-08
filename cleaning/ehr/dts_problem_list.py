import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHRDTSProblemListCleaner(BaseCleaner):
    
    def __init__(self):
        super().__init__()
        
    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


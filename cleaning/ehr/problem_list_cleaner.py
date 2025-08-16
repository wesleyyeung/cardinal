import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHRProblemListCleaner(BaseCleaner):
    
    def __init__(self):
        super().__init__()

    def custom_clean(self, df: pd.DataFrame):
        return df.rename(columns={
            'mrn_sha1':'subject_id'
        })
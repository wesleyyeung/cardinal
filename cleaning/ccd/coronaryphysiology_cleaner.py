import pandas as pd
from cleaning.base_cleaner import BaseCleaner
from hash import hash_sha1

class CCDCoronaryPhysiologyCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(how='all')
        df['Name'] = df['Name'].apply(lambda row: hash_sha1(str(row)))
        return df.rename(columns={'name':'Name','NRIC':'subject_id'})
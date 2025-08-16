import numpy as np
import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHRPatientCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        self.dt_cols = [col for col in df.columns if ('dt' in col.lower() or 'onset' in col.lower())]
        df['death_indicator'] = pd.to_datetime(df['death_indicator'],errors='coerce')
        df['gender'] = df['gender'].replace({'1':np.nan,'2':np.nan,'F':'Female','M':'Male','U':'Unknown'})
        df['ethnicity_race'] = df['ethnicity_race'].replace({'C':'Chinese','CA':'Caucasian','CN':'Chinese','EU':'Eurasian','I':'Indian','IN':'Indian','M':'Malay','MY':'Malay','N':np.nan,'O':'Others','S':'Sikh','SK':'Sikh','XX':'Unknown'})
        return df.rename(columns={
            'mrn_sha1':'subject_id',
            'dob':'date_of_birth',
            'death_indicator':'date_of_death',
            'ethnicity_race':'ethnicity'
        })
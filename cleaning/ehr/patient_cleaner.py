import numpy as np
import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHRPatientCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[[
            'mrn_sha1',
            'dob',
            'gender',
            'death_indicator',
            'ethnicity_race',
            'first_followup_dt',
            'last_followup_dt',
            'db_source',
            'last_encounter_location',
            'hypertension',
            'hypertension_onset',
            'hyperlipidemia',
            'hyperlipidemia_onset',
            'diabetes_mellitus',
            'diabetes_mellitus_onset',
            'ihd',
            'ihd_onset',
            'heart_failure',
            'heart_failure_onset',
            'stroke',
            'stroke_onset',
            'afib',
            'afib_onset',
            'chronic_kidney_disease',
            'chronic_kidney_disease_onset',
            'renal_replacement_therapy',
            'renal_replacement_therapy_onset',
            'peripheral_vascular_disease',
            'peripheral_vascular_disease_onset',
            'thyroid_disease',
            'thyroid_disease_onset',
            'copd',
            'copd_onset'
        ]]
        df['death_indicator'] = pd.to_datetime(df['death_indicator'],errors='coerce')
        df['gender'] = df['gender'].replace({'1':np.nan,'2':np.nan,'F':'Female','M':'Male','U':'Unknown'})
        df['ethnicity_race'] = df['ethnicity_race'].replace({'C':'Chinese','CA':'Caucasian','CN':'Chinese','EU':'Eurasian','I':'Indian','IN':'Indian','M':'Malay','MY':'Malay','N':np.nan,'O':'Others','S':'Sikh','SK':'Sikh','XX':'Unknown'})
        
        self.dt_cols = [col for col in df.columns if ('dt' in col.lower() or 'onset' in col.lower())] + ['date_of_birth']
        
        return df.rename(columns={
            'mrn_sha1':'subject_id',
            'dob':'date_of_birth',
            'death_indicator':'date_of_death',
            'ethnicity_race':'ethnicity'
        })
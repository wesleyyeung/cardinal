import numpy as np
import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHRComorbiditiesCleaner(BaseCleaner):
    def __init__(self, destination_schema=None, destination_tablename=None):
        super().__init__(destination_schema=destination_schema,destination_tablename=destination_tablename)

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        self.dt_cols = [col for col in df.columns if ('dt' in col.lower() or 'onset' in col.lower())]
        df = df[[
            'mrn_sha1',
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
        return df.rename({
            'mrn_sha1':'subject_id'
        })
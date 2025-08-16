import pandas as pd
from cleaning.base_cleaner import BaseCleaner
import numpy as np
from utils import medication_type_cleaner
import sqlite3

class EHREDWOutpatientMedicationCleaner(BaseCleaner):
    
    def __init__(self, destination_schema=None, destination_tablename=None, join_df=None):
        super().__init__(destination_schema=destination_schema,destination_tablename=destination_tablename)
        self.enc = join_df
        self.enc = self.enc[self.enc['visit_id'].str.contains('edw')]
        self.enc.loc[:,'visit_id'] = self.enc['visit_id'].str.replace('edw_','').astype(int)

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        final_cols = [
            'subject_id',
            'medication_start_dt',
            'medication_end_dt',
            'medication_route',
            'medication_name',
            'medication_type',
            'instruction',
            'dose',
            'dose_units',
            'frequency',
            'duration',
            'duration_units'
        ]

        df = df.merge(self.enc,left_on='ENC_KEY',right_on='visit_id')

        rename_dict = {
            'mrn_sha1':'subject_id',
            'CREATION_DATE_KEY':'medication_start_dt',
            'DRUG_NAME':'medication_name',
            'encounter_type':'medication_type',
            'ORDER_INSTRUCTION_DESC':'instruction',
            'DRUG_STRENGTH_AMT':'dose',
            'ORDER_FREQUENCY_DESC':'frequency',
            'ORDER_DURATION_DESC':'duration',
        }
        df = df.rename(columns=rename_dict)
        df['medication_route'] = np.nan 
        df['medication_start_dt'] = pd.to_datetime(df['medication_start_dt'],format='mixed')
        df['medication_type'] = df['medication_type'].rename({'IP':'Inpatient','OP':'Oupatient','AE':'Emergency'})

        df['duration'] = df['duration'].str.lower()
        df[['duration','duration_units']] = df['duration'].str.split(' ',n=1,expand=True)
        df[['dose','dose_units']] = df['dose'].str.split(r'(\d+)(?!.*\d)',regex=True,expand=True).iloc[:,1:]
        td_days = df['duration'].replace({'d2-d6':1,'d1-d12':1,'d15-d25':1,'d1-d15':1,'d1-d14':1}).astype(float) * df['duration_units'].replace({'months':30,'month':30,'menstrual cycle':30,'calendar month':30,'mens cycle':30,'days per cycle':30,'weeks':7,'week':7,'days':1,'day':1}).astype(float)
        df['medication_end_dt'] = df['medication_start_dt'] + td_days.fillna(0).apply(lambda row: pd.Timedelta(days=row))
        df = df[final_cols]

        df.loc[df['medication_name'].isna(),'medication_name'] = df['instruction'][df['medication_name'].isna()] 
        df['medication_type'] = df['medication_type'].apply(lambda row: medication_type_cleaner(row))
        df['medication_start_dt'] = pd.to_datetime(df['medication_start_dt'],format='mixed',errors='coerce')
        df['medication_end_dt'] = pd.to_datetime(df['medication_end_dt'],format='mixed',errors='coerce')
        df['medication_route'] = df['medication_route'].replace({'PO':'Oral','IV':'Intravenous','SC':'Subcutaneous','NG':'Nasogastric','IM':'Intramuscular'})

        return df
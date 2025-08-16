import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
from cleaning.base_cleaner import BaseCleaner
import numpy as np
from utils import parse_medication_instruction, medication_type_cleaner, elementwise_nonmissing

class EHREDWMedicationCleaner(BaseCleaner):
    
    def __init__(self, destination_schema=None, destination_tablename=None,join_df=None):
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
            'STARTTIME':'medication_start_dt',
            'ENDTIME':'medication_end_dt',
            'ROUTECODE':'medication_route',
            'ORDERTEXT':'medication_name',
            'MEDICATION':'instruction',
            'encounter_type':'medication_type',
            'FREQUENCY1CODE':'frequency',

        }

        df = df.rename(columns=rename_dict)
        df['medication_type'] = df['medication_type'].rename({'IP':'Inpatient','OP':'Oupatient','AE':'Emergency'})
        df['dose'] = np.nan
        df['duration'] = np.nan
        edw_instruction = df['instruction'].apply(lambda row: parse_medication_instruction(row)).apply(pd.Series)[['dose','frequency','duration']]
        edw_dosage_regimen = df['DOSAGEREGIMEN'].apply(lambda row: parse_medication_instruction(str(row))).apply(pd.Series)[['dose','frequency','duration']]

        df[['dose','frequency','duration']] = elementwise_nonmissing([df[['dose','frequency','duration']],edw_instruction,edw_dosage_regimen])

        df['medication_route'] = df['medication_route'].rename({'PO':'Oral','IV':'Intravenous','SC':'Subcutaneous','NG':'Nasogastric'})
        df[['dose','dose_units']] = df['dose'].str.split(r'(\d+)(?!.*\d)',regex=True,expand=True).iloc[:,1:]
        df['duration'] = (pd.to_datetime(df['medication_end_dt'],format='mixed',errors='coerce') - pd.to_datetime(df['medication_start_dt'],format='mixed',errors='coerce')).dt.days
        df['duration_units'] = 'days' 
        df = df[final_cols]

        df.loc[df['medication_name'].isna(),'medication_name'] = df['instruction'][df['medication_name'].isna()] 
        df['medication_type'] = df['medication_type'].apply(lambda row: medication_type_cleaner(row))
        df['medication_start_dt'] = pd.to_datetime(df['medication_start_dt'],format='mixed',errors='coerce')
        df['medication_end_dt'] = pd.to_datetime(df['medication_end_dt'],format='mixed',errors='coerce')
        df['medication_route'] = df['medication_route'].replace({'PO':'Oral','IV':'Intravenous','SC':'Subcutaneous','NG':'Nasogastric','IM':'Intramuscular'})
        
        return df
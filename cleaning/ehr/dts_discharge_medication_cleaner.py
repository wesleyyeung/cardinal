import pandas as pd
from cleaning.base_cleaner import BaseCleaner
from utils import parse_medication_instruction, medication_type_cleaner

class EHRDTSDischargeMedicationCleaner(BaseCleaner):
    
    def __init__(self, destination_schema=None, destination_tablename=None,join_df=None):
        super().__init__(destination_schema=destination_schema,destination_tablename=destination_tablename)
        self.enc = join_df
        self.enc = self.enc[self.enc['visit_id'].str.contains('hosp')]
        self.enc.loc[:,'visit_id'] = self.enc['visit_id'].str.replace('hosp_','').astype(int)

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
        
        df = df.merge(self.enc,left_on='discharge_summary_id',right_on='visit_id')
        
        df['medication_type'] = 'discharge'
        rename_dict = {
            'mrn_sha1':'subject_id',
            'discharge_route':'medication_route',
            'medication_dosage_regimen':'medication_dose'
        }
        df = df.rename(columns=rename_dict)
        columns = ['subject_id','medication_start_dt','medication_end_dt','medication_route','medication_name','medication_dose','medication_type']
        df = df[columns]
        new_df = df['medication_dose'].apply(lambda row: parse_medication_instruction(row)).apply(pd.Series)
        df = df.merge(new_df,left_index=True,right_index=True) 
        df = df.drop(columns='medication_dose')
        df[['dose','dose_units']] = df['dose'].str.split(' ',n=1, expand=True)
        df[['duration','duration_units']] = df['duration'].str.split(' ',n=1, expand=True)
        df = df[final_cols]

        df['medication_name'][df['medication_name'].isna()] = df['instruction'][df['medication_name'].isna()] 
        df['medication_type'] = df['medication_type'].apply(lambda row: medication_type_cleaner(row))
        df['medication_start_dt'] = pd.to_datetime(df['medication_start_dt'],format='mixed',errors='coerce')
        df['medication_end_dt'] = pd.to_datetime(df['medication_end_dt'],format='mixed',errors='coerce')
        df['medication_route'] = df['medication_route'].replace({'PO':'Oral','IV':'Intravenous','SC':'Subcutaneous','NG':'Nasogastric','IM':'Intramuscular'})
        
        return df
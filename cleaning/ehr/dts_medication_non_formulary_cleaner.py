import pandas as pd
from cleaning.base_cleaner import BaseCleaner
import numpy as np
from utils import get_unique, medication_type_cleaner

class EHRDTSMedicationNonFormularyCleaner(BaseCleaner):
    
    def __init__(self, destination_schema=None, destination_tablename=None):
        super().__init__(destination_schema=destination_schema,destination_tablename=destination_tablename)
        
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
        df['order_types'] = df['order_types'].apply(lambda row: get_unique(row))
        rename_dict = {
            'mrn_sha1':'subject_id',
            'first_start_dt':'medication_start_dt',
            'last_end_dc_dt':'medication_end_dt',
            'route_of_administration':'medication_route',
            'med':'medication_name',
            'max_dose':'dose',
            'dosage_quantity_unit':'dose_units',
            'order_types':'medication_type',
            'frequency':'frequency',
            'total_med_days':'duration',
        }
        df['instruction'] = np.nan
        df = df.rename(columns=rename_dict)
        df['duration_units'] = 'days'
        df = df[final_cols]

        df.loc[df['medication_name'].isna(),'medication_name'] = df['instruction'][df['medication_name'].isna()] 
        df['medication_type'] = df['medication_type'].apply(lambda row: medication_type_cleaner(row))
        df['medication_start_dt'] = pd.to_datetime(df['medication_start_dt'],format='mixed',errors='coerce')
        df['medication_end_dt'] = pd.to_datetime(df['medication_end_dt'],format='mixed',errors='coerce')
        df['medication_route'] = df['medication_route'].replace({'PO':'Oral','IV':'Intravenous','SC':'Subcutaneous','NG':'Nasogastric','IM':'Intramuscular'})

        return df
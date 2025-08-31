import numpy as np
import pandas as pd
from cleaning.base_cleaner import BaseCleaner
import sqlite3
import json

class EHREDWLaboratoryCleaner(BaseCleaner):
    
    def __init__(self, destination_schema=None, destination_tablename=None,join_df=None):
        super().__init__(destination_schema=destination_schema,destination_tablename=destination_tablename)
        with open('config/config.json','r') as file:
            conf = json.load(file)
        preprocess_con = sqlite3.connect(f"{conf['data_path']}/preprocess.db")
        self.enc = join_df
        self.enc = self.enc[self.enc['visit_id'].str.contains('edw')]
        self.enc.loc[:,'visit_id'] = self.enc['visit_id'].str.replace('edw_','').astype(int)
        
    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:

        df = df.merge(self.enc,left_on='ENC_KEY',right_on='visit_id',how='left')

        mapping_dict = {
            'mrn_sha1':'subject_id',
            'REQ_LOCATION':'location',
            'COLLECTION_DATE_SID':'specimen_collected_dt',
            'RESULT_TEST_DATE':'report_dt',
            'SHORTTEXT':'investigation_item',
            'TEST_RESULTS':'numeric_value',
            'UNITS':'numeric_value_uom',
            '':'text_value'
        }
        df = df.rename(columns=mapping_dict)
        df = df[['subject_id','location','specimen_collected_dt','report_dt','investigation_item','numeric_value','numeric_value_uom']]
        df['text_value'] = np.nan
        df['specimen_collected_dt'] = pd.to_datetime(df['specimen_collected_dt'],format='%Y%m%d',errors='coerce')
        df['report_dt'] = pd.to_datetime(df['report_dt'],format='%Y-%m-%d',errors='coerce')

        return df[mapping_dict.values()]
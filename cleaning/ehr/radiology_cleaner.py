import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHRRadiologyCleaner(BaseCleaner):
    
    def __init__(self, destination_schema=None, destination_tablename=None):
        super().__init__(destination_schema=destination_schema, destination_tablename=destination_tablename)

    def custom_clean(self, df: pd.DataFrame):
        df = df[['mrn_sha1','id','examination_dt','investigation_type','investigation_name','reason_for_investign','item_code','item_comment','abnormal_flag_code','accession_num',]]
        return df.rename(columns={
            'mrn_sha1':'subject_id',
            'id':'radiology_id',
            'examination_dt':'examination_dt',
            'investigation_type':'examination_type',
            'investigation_name':'examination_name',
            'reason_for_investign':'indication',
            'item_code':'report_type',
            'item_comment':'report_text',
            'abnormal_flag_code':'result_flag',
            'accession_num':'accession_num'
        })
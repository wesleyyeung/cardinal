import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHRSurgeryCleaner(BaseCleaner):
    
    def __init__(self, destination_schema=None, destination_tablename=None):
        super().__init__(destination_schema=destination_schema, destination_tablename=destination_tablename)

    def custom_clean(self, df: pd.DataFrame):
        df = df[['mrn_sha1','id','src_institution','primary_operator_id','ot_procedure_name_code','start_dt','reported_dt','doc_status',]]
        return df.rename(columns={
            'mrn_sha1':'subject_id',
            'id':'surgery_id',
            'src_institution':'institution',
            'ot_procedure_name_code':'surgery_code',
            'start_dt':'start_dt',
            'reported_dt':'reported_dt',
            'primary_operator_id':'surgeon_id',
            'doc_status':'report_status'
        })
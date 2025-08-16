import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHRDTSLaboratoryCleaner(BaseCleaner):
    
    def __init__(self, destination_schema=None, destination_tablename=None):
        super().__init__(destination_schema=destination_schema,destination_tablename=destination_tablename)
        
    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        mapping_dict = {
            'mrn_sha1':'subject_id',
            'src_institution':'location',
            'specimen_collected_dt':'specimen_collected_dt',
            'report_dt':'report_dt',
            'investigation_item':'investigation_item',
            'numeric_value':'numeric_value',
            'numeric_value_uom':'numeric_value_uom',
            'text_value':'text_value'
        }
        df = df.rename(columns=mapping_dict)
        return df[mapping_dict.values()]
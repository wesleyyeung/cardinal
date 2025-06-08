import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHREDWLabsCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()
        self.dt_format_dict = {
            'COLLECTION_DATE_SID': '%Y%m%d',
            'REQUEST_DATE_SID': '%Y%m%d',
            'URGENCY_STATUS': '%Y-%m-%d',
            'RECEIVED_DT_SID': '%Y%m%d',
            'RESULT_TEST_DATE': '%Y-%m-%d'
        }

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        return df
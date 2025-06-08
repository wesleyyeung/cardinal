import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHREDWEncounterCleaner(BaseCleaner):

    def __init__(self):
        super().__init__()
        self.dt_format_dict = {
            'ENC_END_DT_KEY': '%Y%m%d',
            'ENC_START_DT_KEY': '%Y%m%d',
            'IPDSC_OPVISIT_DT_KEY': '%Y%m%d',
            'ADM_DATE': '%Y-%m-%d',
            'DSC_DATE': '%Y-%m-%d',
            'PLAN_DSC_DATE': '%Y-%m-%d',
            'PLAN_VISIT_CREATE_DATE': '%Y-%m-%d',
            'PLAN_VISIT_DATE': '%Y-%m-%d',
            'VISIT_DATE': '%Y-%m-%d'
        }

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:      
        return df
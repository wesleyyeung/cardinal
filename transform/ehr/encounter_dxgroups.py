import pandas as pd
pd.options.mode.chained_assignment = None
import yaml
from transform.base_transform import BaseTransform
from utils import has_any_code
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

class EHREncounterDxGroups(BaseTransform):
    
    def __init__(self):
        super().__init__()

    def custom_transform(self, df: pd.DataFrame) -> tuple:
        db_keys = ['dts','edw']
        dx_keys = []
        combined_codes = {}

        for db in db_keys:
            with open(f'transform/ehr/{db}.yml','r') as file:
                codes = yaml.safe_load(file)
            combined_codes[db] = {}
            for dx in codes.keys():
                combined_codes[db][dx] = codes.get(dx)
            dx_keys += list(codes.keys())
        
        df['combined_diagnosis'] = df['primary_diagnosis'].astype(str).str.replace('.0','') + ',' + df['secondary_diagnosis'].astype(str).str.replace('.0','')

        dx_keys = list(set(dx_keys))
        df[dx_keys] = 0
        
        for db in db_keys:
            for dx in combined_codes[db].keys():
                code_list = combined_codes[db][dx] 
                input = df.loc[df['db_source']==db,'combined_diagnosis']
                if len(input) == 0:
                    continue
                df.loc[df['db_source']==db,dx] = input.apply(lambda row: has_any_code(row,code_list))    
        df = df.drop(columns=['primary_diagnosis','secondary_diagnosis','combined_diagnosis'])

        df['death'] = df['visit_outcome'].isin(['Death Coroners','Death Non-Coroners','Deceased']).astype(int)
        cols = ['visit_id','mrn_sha1','visit_date'] + list(combined_codes[db].keys()) + ['death']
        return df[cols], "dxgroups"
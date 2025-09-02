import pandas as pd
pd.options.mode.chained_assignment = None
import json
from transform.base_transform import BaseTransform
from utils import has_any_code
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

class EHREncounterDiagnosis(BaseTransform):
    
    def __init__(self):
        super().__init__()

    def custom_transform(self, df: pd.DataFrame) -> tuple:
        db_keys = ['dts','edw']
        dx_keys = []
        combined_codes = {}

        with open(f'transform/ehr/code_categories.json','r') as file:
            code_categories = json.load(file)

        for db in db_keys:
            codes = code_categories[db] #get numeric codes per database
            combined_codes[db] = {}
            for dx in codes.keys():
                combined_codes[db][dx] = codes.get(dx)
            dx_keys += list(codes.keys())
        
        
        df.loc[df['db_source']=='edw','combined_diagnosis'] = df.loc[df['db_source']=='edw','primary_diagnosis'].astype(str).str.replace('.0','') + ',' + df.loc[df['db_source']=='edw','secondary_diagnosis'].astype(str).str.replace('.0','')
        df.loc[df['db_source']=='dts','combined_diagnosis'] = df.loc[df['db_source']=='dts','primary_diagnosis'].astype(str) + ',' + df.loc[df['db_source']=='dts','secondary_diagnosis'].astype(str)
        df.loc[:,'combined_diagnosis'] = df['combined_diagnosis'].astype(str).str.replace(',nan','').str.replace('nan,','').str.replace('nan','')

        if any(df['combined_diagnosis'].str.contains(r'\.')):
            print(df['combined_diagnosis'][df['combined_diagnosis'].str.contains(r'\.')].tolist())
            raise

        dx_keys = list(set(dx_keys))
        df[dx_keys] = 0

        for db in db_keys:
            for dx in combined_codes[db].keys():
                code_list = combined_codes[db][dx] 
                input = df.loc[df['db_source']==db,'combined_diagnosis']
                if len(input) == 0:
                    continue
                output = input.apply(lambda row: has_any_code(row,code_list)) 
                df.loc[df['db_source']==db,dx] = output
   
        df['death'] = df['visit_outcome'].isin(['Death Coroners','Death Non-Coroners','Deceased']).astype(int)
        cols = ['visit_id','subject_id','visit_date','location','sub_location','combined_diagnosis'] + dx_keys + ['death']
        return "derived", df[cols], "diagnosis"
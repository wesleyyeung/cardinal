from pandas import DataFrame
import yaml
from transform.base_transform import BaseTransform
from utils import build_reverse_mapper, remap_value_fast

class CISCagLesionPivot(BaseTransform):
    
    def __init__(self):
        super().__init__()

    def custom_transform(self, df: DataFrame) -> tuple:
        with open('transform/cis/caglesion.yml','r') as file:
            caglesion_dict = yaml.safe_load(file)
        df = df[['idref','procedure_date','study_num','lesion','stenosis']]
        reverse_mapper = build_reverse_mapper(caglesion_dict)
        df['lesion'] = df['lesion'].apply(lambda row: remap_value_fast(row,reverse_mapper))
        df_exploded = df.explode('lesion')
        df_exploded = df_exploded.drop_duplicates(keep='first')
        df_exploded = df_exploded[df_exploded['lesion'].isna()==False]
        pivot_df = df_exploded.pivot(columns='lesion', values='stenosis')
        # Optional: Reset index if you want a clean DataFrame
        pivot_df = pivot_df.reset_index(drop=True)
        df = df_exploded[['idref','procedure_date','study_num']].merge(pivot_df,left_index=True,right_index=True)
        df = df.groupby('study_num').max().reset_index()
        df = df[['idref','procedure_date','study_num']+list(caglesion_dict.keys())].fillna(0)
        return df, "pivoted"
        

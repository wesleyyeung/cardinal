import pandas as pd
from transform.base_transform import BaseTransform

class SCDBCathTransform(BaseTransform):

    def __init__(self):
        super().__init__()
    
    def custom_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df
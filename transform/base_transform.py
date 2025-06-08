import pandas as pd

class BaseTransform:

    def __init__(self):
        pass

    def transform(self, df: pd.DataFrame) -> tuple:
        df, new_name = self.custom_transform(df) # type: ignore 
        return df, new_name
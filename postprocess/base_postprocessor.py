import pandas as pd

class BasePostProcessor:

    def __init__(self):
        pass

    def postprocessor(self, df: pd.DataFrame) -> tuple:
        target_schema, df, new_name = self.custom_transform(df) # type: ignore 
        return target_schema, df, new_name
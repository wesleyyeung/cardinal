import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class CDiagnosisCodeCleaner(BaseCleaner):
    
    def __init__(self, destination_schema=None, destination_tablename=None):
        super().__init__(destination_schema=destination_schema,destination_tablename=destination_tablename)
        
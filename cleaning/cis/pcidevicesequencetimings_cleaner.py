import pandas as pd
from cleaning.cis.cis_cleaner import CISCleaner

class CISPCIDeviceSequenceTimingsCleaner(CISCleaner):

    def __init__(self):
        super().__init__()

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.cis_clean(df)
        return df
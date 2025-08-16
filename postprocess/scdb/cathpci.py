import pandas as pd
from postprocess.base_postprocessor import BasePostProcessor

class SCDBCathPCIPostProcessor(BasePostProcessor):

    def __init__(self):
        super().__init__()

    def postprocess(self):
        
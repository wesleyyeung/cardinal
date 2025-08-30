import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class EHRFlowsheetVitalsCleaner(BaseCleaner):
    
    def __init__(self, destination_schema=None, destination_tablename=None):
        super().__init__(destination_schema=destination_schema, destination_tablename=destination_tablename)

    def custom_clean(self, df: pd.DataFrame):
        df = df[[
            'mrn_sha1',
            'recorded_dt',
            'temperature',
            'temperature_uom',
            'heart_rate',
            'heart_rate_uom',
            'blood_pressure',
            'blood_pressure_uom',
            'map',
            'map_uom',
            'resp_rate',
            'resp_rate_uom',
            'o2_sat',
            'o2_sat_uom',
            'fiO2',
            'fiO2_uom',
            'painScore',
            'bp_position',
            'blender_fiO2_percent',
            'blender_fio2_percent_uom',
            'o2_flow_rate',
            'o2_flow_rate_uom',
            'o2_device',
            'rad_o2_device',
            'ed_o2_device',
            'ed_r_o2_device',
            'o2_device_humidified',
            'flow_rate',
            'flow_rate_uom'
        ]]
        return df.rename(columns={
            'mrn_sha1':'subject_id'
        })
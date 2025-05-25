import pandas as pd
from lib.cleaning.base_cleaner import BaseCleaner

class EHRCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()

    def clean(self, df: pd.DataFrame, df_type: str) -> pd.DataFrame:
        func_map = {
            'patient.csv': self.clean_patient,
            'edw_labs.csv': self.clean_edw_labs,
            'edw_encounter.csv': self.clean_edw_encounter,
            'edw_medication.csv': self.clean_edw_medication,
            'edw_outpatient_medication.csv': self.clean_edw_outpatient_medication,
            'dts_labs.csv': self.clean_dts_labs,
            'dts_medication_non_formulary.csv': self.clean_dts_medication_non_formulary,
            'dts_medication.csv': self.clean_dts_medication
        }

        if df_type not in func_map:
            raise ValueError(f"Unknown df_type: {df_type}")

        df = func_map[df_type](df)
        df = self.sanitize_dataframe_columns(df)
        return df.drop_duplicates(keep='first')

    def clean_patient(self, df: pd.DataFrame) -> pd.DataFrame:
        dt_cols = [col for col in df.columns if ('dt' in col.lower() or 'onset' in col.lower())]
        df = self.standardize_dates(df, dt_cols)
        return df

    def clean_edw_labs(self, df: pd.DataFrame) -> pd.DataFrame:
        dt_format_dict = {
            'COLLECTION_DATE_SID': '%Y%m%d',
            'REQUEST_DATE_SID': '%Y%m%d',
            'URGENCY_STATUS': '%Y-%m-%d',
            'RECEIVED_DT_SID': '%Y%m%d',
            'RESULT_TEST_DATE': '%Y-%m-%d'
        }
        return self.standardize_dates_from_dict(df, dt_format_dict)

    def clean_edw_encounter(self, df: pd.DataFrame) -> pd.DataFrame:
        dt_format_dict = {
            'ENC_END_DT_KEY': '%Y%m%d',
            'ENC_START_DT_KEY': '%Y%m%d',
            'IPDSC_OPVISIT_DT_KEY': '%Y%m%d',
            'ADM_DATE': '%Y-%m-%d',
            'DSC_DATE': '%Y-%m-%d',
            'PLAN_DSC_DATE': '%Y-%m-%d',
            'PLAN_VISIT_CREATE_DATE': '%Y-%m-%d',
            'PLAN_VISIT_DATE': '%Y-%m-%d',
            'VISIT_DATE': '%Y-%m-%d'
        }
        return self.standardize_dates_from_dict(df, dt_format_dict)

    def clean_edw_medication(self, df: pd.DataFrame) -> pd.DataFrame:
        df.drop(columns='ADMIN_TIME', inplace=True, errors='ignore')
        dt_format_dict = {
            'CREATE_DATE': '%Y-%m-%d',
            'STARTTIME': '%Y-%m-%d',
            'ENDTIME': '%Y-%m-%d'
        }
        return self.standardize_dates_from_dict(df, dt_format_dict)

    def clean_edw_outpatient_medication(self, df: pd.DataFrame) -> pd.DataFrame:
        dt_cols = ['MODIFICATION_DATE_TIME']
        return self.standardize_dates(df, dt_cols)

    def clean_dts_labs(self, df: pd.DataFrame) -> pd.DataFrame:
        dt_cols = [
            'msg_dt','lr_created_dt','order_dt','specimen_collected_dt',
            'speciment_received_dt','examination_dt','report_dt',
            'lri_created_dt','created_dt'
        ]
        return self.standardize_dates(df, dt_cols)

    def clean_dts_medication(self, df: pd.DataFrame) -> pd.DataFrame:
        dt_cols = ['medication_start_dt','medication_end_dt','created_dt']
        return self.standardize_dates(df, dt_cols)

    def clean_dts_medication_non_formulary(self, df: pd.DataFrame) -> pd.DataFrame:
        dt_cols = ['first_ordered_dt','first_start_dt','last_end_dc_dt']
        df = self.standardize_dates(df, dt_cols)
        for col in ['order_locations','order_types','order_statuses']:
            if col in df.columns:
                df[col] = df[col].apply(lambda row: '|'.join(set(row.split('|'))) if isinstance(row, str) else row)
        return df

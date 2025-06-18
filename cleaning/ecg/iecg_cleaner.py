import re
import numpy as np
import pandas as pd
from cleaning.base_cleaner import BaseCleaner
from hash import hash_sha1

class ECGiECGCleaner(BaseCleaner):
    
    def __init__(self):
        super().__init__()    
        self.dt_cols = ['ecg_time']

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        #IDs
        df['study_id'] = df['filename'].apply(lambda row: hash_sha1(row.split('\\')[-1].split('.xml')[0],output_length=30))
        df['ecg_time'] = pd.to_datetime(df['restingecgdata/dataacquisition/@date'] + ' ' + df['restingecgdata/dataacquisition/@time'],format='%Y-%m-%d %H:%M:%S')
        def normalize_statements(row):
            combined = []
            statement = row['restingecgdata/interpretations/interpretation/statement']
            try:
                combined += [dct['leftstatement'].lower()+'; '+dct['rightstatement'].lower() for dct in eval(statement)]
            except:
                pass
            right_statement = row['restingecgdata/interpretations/interpretation/statement/rightstatement'].lower()
            left_statement = row['restingecgdata/interpretations/interpretation/statement/leftstatement'].lower()
            combined += [right_statement] + [left_statement]
            combined = [item for item in set(combined) if item != '']
            combined = '; '.join(combined)
            return combined

        #Coded Reports
        df['report'] = df[[
            'restingecgdata/interpretations/interpretation/statement/leftstatement',
            'restingecgdata/interpretations/interpretation/statement/rightstatement',
            'restingecgdata/interpretations/interpretation/statement'
        ]].fillna('').apply(lambda row: normalize_statements(row),axis=1)

        #Bandwidth
        df['highpassfilter'] = df['restingecgdata/reportinfo/reportbandwidth/highpassfiltersetting']
        df['lowpassfilter'] = df['restingecgdata/reportinfo/reportbandwidth/lowpassfiltersetting']
        df['bandwidth'] = str(df['highpassfilter'])+'-'+str(df['lowpassfilter'])+'Hz'

        #Filters
        df['notchfilterfreqs'] = df['restingecgdata/dataacquisition/signalcharacteristics/notchfilterfreqs']
        df['artifactfilter'] = df['restingecgdata/reportinfo/reportbandwidth/artifactfilterflag']
        df['hysteresisfilter'] = df['restingecgdata/reportinfo/reportbandwidth/hysteresisfilterflag']

        def generate_filter_summary(row):
            notch_filter, artifact_filter, hysteresis_filter = '', '', ''
            if row['notchfilterfreqs'].isna().values[0] == False:
                notch_filter = str(row['notchfilterfreqs'])+' Hz notch'
            if row['artifactfilter'].values[0]:
                artifact_filter = 'Artifact filter'
            if row['hysteresisfilter'].values[0]:
                hysteresis_filter = 'Hysteresis filter'
            combined = notch_filter + artifact_filter + hysteresis_filter
            return combined

        df['filtering'] = generate_filter_summary(df[[
            'notchfilterfreqs',
            'artifactfilter',
            'hysteresisfilter'
        ]])

        #Intervals
        df['heartrate'] = pd.to_numeric(df['restingecgdata/interpretations/interpretation/globalmeasurements/heartrate/#text'],errors='coerce')
        df['qtinterval'] = pd.to_numeric(df['restingecgdata/internalmeasurements/crossleadmeasurements/meanqtint'],errors='coerce')
        df['qtc'] = pd.to_numeric(df['restingecgdata/internalmeasurements/crossleadmeasurements/meanqtc'],errors='coerce')

        #Other Important Metadata:
        stem = 'restingecgdata/dataacquisition/signalcharacteristics/'
        rename_dict = {}
        for col in df.columns:
            if stem in col:
                rename_dict[col] = re.sub(stem,'',col)
        df = df.rename(columns=rename_dict)

        remap_dict = {
            'restingecgdata/patient/generalpatientdata/patientid':'subject_id',
            'restingecgdata/patient/generalpatientdata/age/dateofbirth':'subject_dob',
            'restingecgdata/patient/generalpatientdata/age/years':'subject_age',
            #'study_id', #derived
            'restingecgdata/dataacquisition/machine/@detaildescription':'cart_id',
            #'ecg_time', #derived
            #'report_#', #derived
            #'bandwidth', #dervied
            #'filtering', #dervied
            'restingecgdata/internalmeasurements/groupmeasurements/groupmeasurement/meanrrint':'rr_interval',
            #'p_onset', #unavailable
            #'p_end', #unavailable
            #'qrs_onset',
            #'qrs_end', #unavailable
            #'t_end', #unavailable
            'restingecgdata/internalmeasurements/crossleadmeasurements/pfrontaxis':'p_axis',
            'restingecgdata/internalmeasurements/crossleadmeasurements/qrsfrontaxis':'qrs_axis',
            'restingecgdata/internalmeasurements/crossleadmeasurements/stfrontaxis':'t_axis'  
        }

        df['qrs_onset'] = df[[
            'restingecgdata/interpretations/interpretation/globalmeasurements/qonset/#text',
            'restingecgdata/interpretations/interpretation/globalmeasurements/qonset'
        ]].apply(lambda row: np.nanmin(row),axis=1)

        df = df.rename(columns=remap_dict)

        selected_cols = [
            'filename',
            'subject_id',
            'subject_dob',
            'subject_age',
            'study_id',
            'cart_id',
            'ecg_time',
            'report',
            'bandwidth',
            'highpassfilter',
            'lowpassfilter',
            'filtering',
            'notchfilterfreqs',
            'artifactfilter',
            'hysteresisfilter',
            'heartrate',
            'qtinterval',
            'qtc'
        ] + list(rename_dict.values()) + [
            'rr_interval',
            'qrs_onset',
            'p_axis',
            'qrs_axis',
            't_axis'
        ]
        df = df[selected_cols]
        return df
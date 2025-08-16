import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class ECGMuseCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()
        self.dt_cols = ['ecg_time']

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        remap_dict = {
            'RestingECG/PatientDemographics/PatientID':'subject_id',
            #'subject_dob', #derived
            'RestingECG/PatientDemographics/PatientAge':'subject_age',
            #'study_id', unavailable
            'RestingECG/TestDemographics/LocationName':'cart_id', #surrogate, to replace
            #'ecg_time', #derived
            'RestingECG/OriginalDiagnosis/DiagnosisStatement':'report', 
            #'bandwidth', #unavailable
            #'filtering', #unavailable
            'RestingECG/RestingECGMeasurements/VentricularRate':'heartrate',
            'RestingECG/RestingECGMeasurements/QTInterval':'qtinterval',
            'RestingECG/RestingECGMeasurements/QTCorrected':'qtc',
            #'rr_interval', #unavailable
            #'p_onset', #unavailable
            #'p_end', #unavailable
            'RestingECG/RestingECGMeasurements/QOnset':'qrs_onset',
            #'qrs_end', #unavailable
            #'t_end', #unavailable
            #'p_axis', #unavailable
            'RestingECG/RestingECGMeasurements/RAxis':'qrs_axis',
            'RestingECG/RestingECGMeasurements/TAxis':'t_axis'  
        }

        df = df.rename(columns=remap_dict) 
        df['filename'] = df['Folder'] + '/' + df['Filename']
        df['ecg_time'] = pd.to_datetime(df['RestingECG/TestDemographics/AcquisitionDate'] + ' ' + df['RestingECG/TestDemographics/AcquisitionTime'])
        
        selected_cols = [
            'filename',
            'subject_id',
            #'subject_dob',
            'subject_age',
            #'study_id',
            'cart_id',
            'ecg_time',
            'report',
            #'bandwidth',
            #'highpassfilter',
            #'lowpassfilter',
            #'filtering',
            #'notchfilterfreqs',
            #'artifactfilter',
            #'hysteresisfilter',
            'heartrate',
            'qtinterval',
            'qtc',
            #'rr_interval',
            'qrs_onset',
            #'p_axis',
            'qrs_axis',
            't_axis'
        ]
        return df[selected_cols]

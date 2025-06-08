import pandas as pd
from cleaning.base_cleaner import BaseCleaner

class CCDCardiacSurgeryCleaner(BaseCleaner):

    def __init__(self):
        super().__init__()
        self.ABBREVIATIONS = {
            'renal_disease_at_the_time_of_surgery_choicefunctioning_transplant':'renal_disease_functioning_transplant',
            'renal_disease_at_the_time_of_surgery_choicecreatinine_200_umol_per_l':'renal_disease_cr_200_umol_per_l', 
            'renal_disease_at_the_time_of_surgery_choicedialysis_acute_renal_failure':'renal_disease_aki',
            'renal_disease_at_the_time_of_surgery_choicedialysis_chronic_renal_failure':'renal_disease_crf',
            'other_cardiac_procedures_choicearrythmia_correction_surgery':'other_cardiacsurg_arrhythmia',
            'other_cardiac_procedures_choicepulmonary_thromboembolectomy':'other_cardiacsurg_pte',
            'other_cardiac_procedures_choicesubaortic_stenosis_resection':'other_cardiacsurg_sasr',
            'other_cardiac_procedures_choicesurgical_ventricular_restoration':'other_cardiacsurg_svr',
            'other_cardiac_procedures_choiceasd_repair__secundum_or_sinus_venosus':'other_cardiacsurg_asdrepair',
            'other_cardiac_procedures_choiceother_procedure_not_listed_above':'other_cardiacsurg_other',
            'other_thoracic_and_vascular_procedures_choiceperipheral_vascular':'other_surg_pvd',
            'other_thoracic_and_vascular_procedures_choicecarotid_endarterectomy':'other_surg_cea',
            'other_thoracic_and_vascular_procedures_choiceother_thoracic':'other_surg_thoracic',
            'atrial_fibrillation_correction_surgery_choicepulmonary_vein_isolation_pvi':'af_surgery_pvi',
            'atrial_fibrillation_correction_surgery_choiceleft_atrial_maze':'af_surgery_la_maze',
            'atrial_fibrillation_correction_surgery_choicebiatrial_maze_la__ra':'af_surgery_ba_maze'
        }

    def custom_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        return df
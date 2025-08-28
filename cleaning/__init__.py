import sqlite3
import pandas as pd
import json

from cleaning.ccd.ohca_cleaner import CCDOHCACleaner
from cleaning.ccd.cardiacsurgery_cleaner import CCDCardiacSurgeryCleaner
from cleaning.ccd.coronaryphysiology_cleaner import CCDCoronaryPhysiologyCleaner

from cleaning.cis.cag_cleaner import CISCAGCleaner
from cleaning.cis.caglesion_cleaner import CISCAGLesionCleaner
from cleaning.cis.echo_cleaner import CISEchoCleaner
from cleaning.cis.nuclear_cleaner import CISNuclearCleaner
from cleaning.cis.pacemaker_cleaner import CISPacemakerCleaner
from cleaning.cis.pci_cleaner import CISPCICleaner
from cleaning.cis.pcidevicesequence_cleaner import CISPCIDeviceSequenceCleaner
from cleaning.cis.pcidevicesequencetimings_cleaner import CISPCIDeviceSequenceTimingsCleaner
from cleaning.cis.pcilesionsequence_cleaner import CISPCILesionSequenceCleaner
from cleaning.cis.procedure_cleaner import CISProcedureCleaner

from cleaning.ecg.iecg_cleaner import ECGiECGCleaner
from cleaning.ecg.muse_cleaner import ECGMuseCleaner

from cleaning.concept.diagnosis_code_cleaner import CDiagnosisCodeCleaner

from cleaning.ehr.dts_discharge_medication_cleaner import EHRDTSDischargeMedicationCleaner
from cleaning.ehr.dts_medication_cleaner import EHRDTSMedicationCleaner
from cleaning.ehr.edw_medication_cleaner import EHREDWMedicationCleaner
from cleaning.ehr.edw_outpatient_medication_cleaner import EHREDWOutpatientMedicationCleaner

from cleaning.ehr.dts_laboratory_cleaner import EHRDTSLaboratoryCleaner
from cleaning.ehr.edw_laboratory_cleaner import EHREDWLaboratoryCleaner

from cleaning.ehr.problem_list_cleaner import EHRProblemListCleaner
from cleaning.ehr.patient_cleaner import EHRPatientCleaner
from cleaning.ehr.encounter_cleaner import EHREncounterCleaner

with open('config/config.json','r') as file:
    conf = json.load(file)

print('Reading preprocess db')
preprocess_con = sqlite3.connect(f"{conf['data_path']}/preprocess.db")
enc_df = pd.read_sql_query(f"SELECT visit_id, mrn_sha1, encounter_type FROM 'ehr.encounter'",preprocess_con)
preprocess_con.close()
print('Complete')

CLEANER_REGISTRY = {
    "ccd": {
        "ohca": CCDOHCACleaner(),
        "cardiacsurgery": CCDCardiacSurgeryCleaner(),
        "coronaryphysiology": CCDCoronaryPhysiologyCleaner()
    },
    "cis": {
        "cag": CISCAGCleaner(),
        "caglesion": CISCAGLesionCleaner(),
        "echo": CISEchoCleaner(),
        "nuclear": CISNuclearCleaner(),
        "pacemaker": CISPacemakerCleaner(),
        "pci": CISPCICleaner(),
        "pcidevicesequence": CISPCIDeviceSequenceCleaner(),
        "pcidevicesequencetimings": CISPCIDeviceSequenceTimingsCleaner(),
        "pcilesionsequence": CISPCILesionSequenceCleaner(),
        "procedure": CISProcedureCleaner()
    },
    "concept":{
        "diagnosis_codes":CDiagnosisCodeCleaner(destination_schema="concept", destination_tablename="diagnosis_code") 
    },
    "ehr": {
        "dts_discharge_medication": EHRDTSDischargeMedicationCleaner(destination_schema="ehr", destination_tablename="medication",join_df=enc_df),
        "dts_medication": EHRDTSMedicationCleaner(destination_schema="ehr", destination_tablename="medication"),
        "edw_medication": EHREDWMedicationCleaner(destination_schema="ehr", destination_tablename="medication",join_df=enc_df),
        "edw_outpatient_medication": EHREDWOutpatientMedicationCleaner(destination_schema="ehr", destination_tablename="medication",join_df=enc_df),
        "dts_laboratory": EHRDTSLaboratoryCleaner(destination_schema="ehr", destination_tablename="lab"),
        "edw_laboratory": EHREDWLaboratoryCleaner(destination_schema="ehr", destination_tablename="lab",join_df=enc_df),
        "problem_list": EHRProblemListCleaner(),
        "patient": EHRPatientCleaner(),
        "encounter": EHREncounterCleaner()
    },
    "ecg": {
        "iecg": ECGiECGCleaner(),
        "muse": ECGMuseCleaner()
    }
}
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

from cleaning.ehr.dts_discharge_medication_cleaner import EHRDTSDischargeMedicationCleaner
from cleaning.ehr.dts_laboratory_cleaner import EHRDTSLaboratoryCleaner
from cleaning.ehr.dts_medication_non_formulary_cleaner import EHRDTSMedicationNonFormularyCleaner
from cleaning.ehr.dts_problem_list import EHRDTSProblemListCleaner
from cleaning.ehr.edw_encounter_cleaner import EHREDWEncounterCleaner
from cleaning.ehr.edw_labs_cleaner import EHREDWLabsCleaner
from cleaning.ehr.edw_medication_cleaner import EHREDWMedicationCleaner
from cleaning.ehr.edw_outpatient_medication_cleaner import EHREDWOutpatientMedicationCleaner
from cleaning.ehr.patient_cleaner import EHRPatientCleaner
from cleaning.ehr.encounter_cleaner import EHREncounterCleaner

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
        "procedure": CISProcedureCleaner(),
    },
    "ehr": {
        "dts_discharge_medication" : EHRDTSDischargeMedicationCleaner(),
        "dts_laboratory": EHRDTSLaboratoryCleaner(),
        "dts_medication_non_formulary": EHRDTSMedicationNonFormularyCleaner(),
        "dts_problem_list": EHRDTSProblemListCleaner(),
        "edw_encounter": EHREDWEncounterCleaner(),
        "edw_labs" : EHREDWLabsCleaner(),
        "edw_medication": EHREDWMedicationCleaner(),
        "edw_outpatient_medication": EHREDWOutpatientMedicationCleaner(),
        "patient": EHRPatientCleaner(),
        "encounter": EHREncounterCleaner()
    },
    "ecg": {
        "iecg": ECGiECGCleaner(),
        "muse": ECGMuseCleaner(),
    }
}
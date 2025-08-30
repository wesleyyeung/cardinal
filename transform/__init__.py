from transform.cis.caglesion_pivot import CISCagLesionPivot
from transform.cis.echo_valvelesion import CISEchoValveLesion
from transform.ehr.encounter_diagnosis import EHREncounterDiagnosis

TRANSFORM_REGISTRY = {
    'cis': {
        'caglesion': CISCagLesionPivot(),
        'echo': CISEchoValveLesion()
    },
    'ehr': {
        'encounter': EHREncounterDiagnosis()
    }
}
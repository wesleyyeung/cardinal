from transform.cis.caglesion_pivot import CISCagLesionPivot
from transform.cis.echo_valvelesion import CISEchoValveLesion
from transform.ehr.encounter_dxgroups import EHREncounterDxGroups

TRANSFORM_REGISTRY = {
    'cis': {
        'caglesion': CISCagLesionPivot(),
        'echo': CISEchoValveLesion()
    },
    'ehr': {
        'encounter': EHREncounterDxGroups()
    }
}
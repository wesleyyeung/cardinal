from transform.cis.caglesion_pivot import CISCagLesionPivot
from transform.ehr.encounter_dxgroups import EHREncounterDxGroups

TRANSFORM_REGISTRY = {
    'cis': {
        'caglesion': CISCagLesionPivot()
    },
    'ehr': {
        'encounter': EHREncounterDxGroups()
    }
}
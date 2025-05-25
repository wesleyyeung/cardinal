
import yaml
from pathlib import Path
from lib.cleaning.clean_cis import CISCleaner
from lib.cleaning.clean_ccd import CCDCleaner
from lib.cleaning.clean_ecg import ECGCleaner
from lib.cleaning.clean_ehr import EHRCleaner

# Load PHI dictionary safely using relative path
phi_config_path = Path(__file__).parent.parent / "config" / "cis_private_health_information.yml"
with open(phi_config_path, 'r') as file:
    phi_dictionary = yaml.safe_load(file)

def clean_table(df, table_name, filename=None):
    table_name = table_name.lower()

    if table_name == "ecg":
        cleaner = ECGCleaner()
        if not filename:
            raise ValueError("Filename must be provided for ECG cleaner")
        return cleaner.clean(df, filename)

    elif table_name == "cis":
        cleaner = CISCleaner(phi_dictionary)
        if not filename:
            raise ValueError("Filename must be provided for CIS cleaner")
        return cleaner.clean(df, filename)

    elif table_name == "ccd":
        cleaner = CCDCleaner()
        if not filename:
            raise ValueError("Filename must be provided for CCD cleaner")
        return cleaner.clean(df, filename)
    
    elif table_name == "ehr":
        cleaner = EHRCleaner()
        if not filename:
            raise ValueError("Filename must be provided for EHR cleaner")
        return cleaner.clean(df, filename)

    else:
        raise ValueError(f"No cleaner registered for table '{table_name}'")

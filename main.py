import subprocess
import argparse
import sys
from pathlib import Path
import yaml

class Cardinal:

    def __init__(self, config_path: str = 'config/config.yml'):
        with open(config_path,'r') as file:
            config = yaml.safe_load(file)
        self.datasets = config['datasets']
        self.raw_path = config['raw_path']
        self.preprocessed_path = config['preprocessed_path']
        self.clean_path = config['clean_path']

    @staticmethod
    def run_step(description, command):
            print(f"\n{description}")
            try:
                subprocess.run(command, check=True)
            except subprocess.CalledProcessError:
                print(f"Failed: {description}")
                sys.exit(1)
            print(f"Completed: {description}")

    def main(self):
        base_path = Path(__file__).parent
        raw_path = self.raw_path
        preprocessed_path = self.preprocessed_path
        clean_path = self.clean_path
        self.run_step("Preprocessing raw CSV files",
                ["python", str(base_path / "preprocess.py"), "--raw_path", raw_path, "--preprocessed_path",preprocessed_path])

        self.run_step("Cleaning preprocessed CSV files",
                ["python", str(base_path / "clean.py"), "--preprocessed_path", preprocessed_path, "--clean_path", clean_path])

        for dataset in self.datasets:
            self.run_step("Creating PostgreSQL schema for {dataset}",
                    ["python", str(base_path / "create_schema.py"), "--dataset", dataset])

        self.run_step("Transforming preprocessed CSV files",
                ["python", str(base_path / "transforms.py"), "--clean_path", clean_path])

        self.run_step("Loading cleaned data into staging tables",
                ["python", str(base_path / "load.py"), "--clean_path", clean_path])
        for dataset in self.datasets:
            self.run_step("Merging staging data into final tables",
                ["python", str(base_path / "merge.py"), "--dataset", dataset])

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=False, help="Path to config file")
    args = parser.parse_args()
    if hasattr(args,'config'):
        card = Cardinal(args.config)
    else:
        card = Cardinal()
    card.main()

    
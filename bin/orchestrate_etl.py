
import subprocess
import argparse
import sys
from pathlib import Path

def run_step(description, command):
    print(f"\n{description}")
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError:
        print(f"Failed: {description}")
        sys.exit(1)
    print(f"Completed: {description}")

def main(dataset):
    base_path = Path(__file__).parent

    run_step("Cleaning raw CSV files",
             ["python", str(base_path / "clean.py"), "--dataset", dataset])

    run_step("Creating PostgreSQL schemas",
             ["python", str(base_path / "create_schema.py"), "--dataset", dataset])

    run_step("Loading cleaned data into staging tables",
             ["python", str(base_path / "load.py"), "--dataset", dataset])

    run_step("Merging staging data into final tables",
             ["python", str(base_path / "merge.py"), "--dataset", dataset])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Dataset name (e.g., ccd or cis)")
    args = parser.parse_args()
    main(args.dataset)

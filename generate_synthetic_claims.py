"""
Synthetic Medical Claims Generator using Synthea

This script downloads Synthea (if needed), generates synthetic patient data,
and extracts medical claims information.

Requirements:
- Java JDK 11 or newer installed
- Python packages: pandas, requests

Usage:
    python generate_synthetic_claims.py
"""

import subprocess
import os
import sys
import zipfile
import shutil
import pandas as pd
from pathlib import Path

# Configuration
SYNTHEA_VERSION = "3.3.0"
SYNTHEA_JAR_URL = f"https://github.com/synthetichealth/synthea/releases/download/v{SYNTHEA_VERSION}/synthea-with-dependencies.jar"
SYNTHEA_JAR_NAME = "synthea-with-dependencies.jar"
OUTPUT_DIR = "synthea_output"
NUM_PATIENTS = 100  # Number of synthetic patients to generate


def check_java_installed():
    """Check if Java is installed and available."""
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True
        )
        print("✓ Java is installed")
        return True
    except FileNotFoundError:
        print("✗ Java is not installed or not in PATH")
        print("  Please install Java JDK 11 or newer from: https://adoptium.net/")
        return False


def download_synthea(jar_path: Path):
    """Download Synthea JAR file if not present."""
    if jar_path.exists():
        print(f"✓ Synthea JAR already exists: {jar_path}")
        return True

    print(f"Downloading Synthea v{SYNTHEA_VERSION}...")

    try:
        import requests
    except ImportError:
        print("Installing requests package...")
        subprocess.run([sys.executable, "-m", "pip", "install", "requests"], check=True)
        import requests

    try:
        response = requests.get(SYNTHEA_JAR_URL, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0

        with open(jar_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size:
                    percent = (downloaded / total_size) * 100
                    print(f"\r  Downloaded: {percent:.1f}%", end="", flush=True)

        print(f"\n✓ Downloaded Synthea to: {jar_path}")
        return True

    except Exception as e:
        print(f"✗ Failed to download Synthea: {e}")
        return False


def run_synthea(jar_path: Path, output_dir: Path, num_patients: int, state: str = "Massachusetts"):
    """Run Synthea to generate synthetic patient data."""
    print(f"\nGenerating {num_patients} synthetic patients in {state}...")

    # Clean output directory
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    # Run Synthea
    cmd = [
        "java", "-jar", str(jar_path),
        "-p", str(num_patients),  # Number of patients
        "-s", "12345",            # Seed for reproducibility
        "--exporter.csv.export", "true",
        "--exporter.fhir.export", "false",
        "--exporter.ccda.export", "false",
        "--exporter.baseDirectory", str(output_dir),
        state
    ]

    print(f"Running command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )

        if result.returncode == 0:
            print("✓ Synthea generation completed successfully")
            return True
        else:
            print(f"✗ Synthea failed with return code: {result.returncode}")
            print(f"  STDERR: {result.stderr[:500] if result.stderr else 'No error output'}")
            return False

    except subprocess.TimeoutExpired:
        print("✗ Synthea generation timed out")
        return False
    except Exception as e:
        print(f"✗ Error running Synthea: {e}")
        return False


def load_claims_data(output_dir: Path) -> dict:
    """Load generated claims data from CSV files."""
    csv_dir = output_dir / "csv"

    if not csv_dir.exists():
        print(f"✗ CSV output directory not found: {csv_dir}")
        return {}

    # List of relevant claims-related files
    claims_files = {
        "claims": "claims.csv",
        "claims_transactions": "claims_transactions.csv",
        "patients": "patients.csv",
        "encounters": "encounters.csv",
        "conditions": "conditions.csv",
        "procedures": "procedures.csv",
        "medications": "medications.csv",
        "payers": "payers.csv",
        "providers": "providers.csv",
    }

    data = {}

    print("\nLoading generated data files:")
    for name, filename in claims_files.items():
        filepath = csv_dir / filename
        if filepath.exists():
            df = pd.read_csv(filepath)
            data[name] = df
            print(f"  ✓ {filename}: {len(df)} records")
        else:
            print(f"  - {filename}: not found")

    return data


def create_claims_summary(data: dict) -> pd.DataFrame:
    """Create a summary dataframe of medical claims."""
    if "claims" not in data:
        print("No claims data available")
        return pd.DataFrame()

    claims = data["claims"].copy()

    # Add patient info if available
    if "patients" in data:
        patients = data["patients"][["Id", "FIRST", "LAST", "BIRTHDATE", "GENDER"]]
        patients = patients.rename(columns={"Id": "PATIENTID"})
        claims = claims.merge(patients, on="PATIENTID", how="left")

    # Add provider info if available
    if "providers" in data and "PROVIDERID" in claims.columns:
        providers = data["providers"][["Id", "NAME", "SPECIALITY"]].rename(
            columns={"Id": "PROVIDERID", "NAME": "PROVIDER_NAME", "SPECIALITY": "PROVIDER_SPECIALTY"}
        )
        claims = claims.merge(providers, on="PROVIDERID", how="left")

    return claims


def display_sample_claims(claims_df: pd.DataFrame, num_samples: int = 10):
    """Display sample claims data."""
    if claims_df.empty:
        print("No claims to display")
        return

    print(f"\n{'='*80}")
    print(f"SAMPLE SYNTHETIC MEDICAL CLAIMS (showing {min(num_samples, len(claims_df))} of {len(claims_df)})")
    print(f"{'='*80}")

    # Select key columns to display
    display_cols = [
        "Id", "PATIENTID", "FIRST", "LAST",
        "PRIMARYPATIENTINSURANCEID", "TOTALPAYMENTS"
    ]

    # Filter to columns that exist
    available_cols = [col for col in display_cols if col in claims_df.columns]

    if available_cols:
        sample = claims_df[available_cols].head(num_samples)
        print(sample.to_string(index=False))
    else:
        print(claims_df.head(num_samples).to_string(index=False))


def save_claims_to_csv(claims_df: pd.DataFrame, output_path: str):
    """Save the claims summary to a CSV file."""
    claims_df.to_csv(output_path, index=False)
    print(f"\n✓ Claims saved to: {output_path}")


def main():
    """Main function to generate synthetic medical claims."""
    print("=" * 60)
    print("SYNTHETIC MEDICAL CLAIMS GENERATOR")
    print("Using Synthea - Synthetic Patient Generator")
    print("=" * 60)

    # Setup paths
    script_dir = Path(__file__).parent.resolve()
    jar_path = script_dir / SYNTHEA_JAR_NAME
    output_dir = script_dir / OUTPUT_DIR

    # Step 1: Check Java
    print("\n[1/5] Checking Java installation...")
    if not check_java_installed():
        sys.exit(1)

    # Step 2: Download Synthea
    print("\n[2/5] Checking Synthea...")
    if not download_synthea(jar_path):
        sys.exit(1)

    # Step 3: Run Synthea
    print("\n[3/5] Running Synthea...")
    if not run_synthea(jar_path, output_dir, NUM_PATIENTS):
        sys.exit(1)

    # Step 4: Load data
    print("\n[4/5] Loading generated data...")
    data = load_claims_data(output_dir)

    if not data:
        print("✗ No data was generated")
        sys.exit(1)

    # Step 5: Process and display claims
    print("\n[5/5] Processing claims data...")
    claims_df = create_claims_summary(data)

    if not claims_df.empty:
        display_sample_claims(claims_df)

        # Save to CSV
        output_csv = script_dir / "synthetic_claims_100.csv"
        save_claims_to_csv(claims_df, str(output_csv))

        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        print(f"Total patients generated: {len(data.get('patients', []))}")
        print(f"Total claims generated: {len(claims_df)}")
        print(f"Total encounters: {len(data.get('encounters', []))}")
        print(f"Output directory: {output_dir}")
        print(f"Claims CSV: {output_csv}")
    else:
        print("No claims data was generated")

    return data, claims_df


if __name__ == "__main__":
    data, claims = main()

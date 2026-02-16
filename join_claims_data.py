"""
Join Synthea Medical Claims Data

This script joins all medical claims-related CSV files from Synthea output
into a comprehensive unified dataset.
"""

import pandas as pd
import os
from pathlib import Path


def load_csv(data_dir: Path, filename: str) -> pd.DataFrame:
    """Load a CSV file and return a DataFrame."""
    filepath = data_dir / filename
    print(f"Loading {filename}...")
    df = pd.read_csv(filepath)
    print(f"  -> {len(df):,} rows, {len(df.columns)} columns")
    return df


def aggregate_clinical_data(df: pd.DataFrame, group_cols: list,
                            code_col: str = 'CODE',
                            desc_col: str = 'DESCRIPTION') -> pd.DataFrame:
    """
    Aggregate clinical data (procedures, conditions, etc.) by grouping columns.
    Returns concatenated codes and descriptions.
    """
    if df.empty:
        return pd.DataFrame(columns=group_cols + [f'{code_col}_LIST', f'{desc_col}_LIST'])

    agg_df = df.groupby(group_cols, as_index=False).agg({
        code_col: lambda x: '|'.join(x.dropna().astype(str)),
        desc_col: lambda x: '|'.join(x.dropna().astype(str))
    })
    agg_df = agg_df.rename(columns={
        code_col: f'{code_col}_LIST',
        desc_col: f'{desc_col}_LIST'
    })
    return agg_df


def main():
    # Configuration
    data_dir = Path("synthea_output/csv")
    output_file = "joined_claims_data.csv"

    print("=" * 60)
    print("Synthea Medical Claims Data Joiner")
    print("=" * 60)

    # =========================================================================
    # Step 1: Load all CSV files
    # =========================================================================
    print("\n[Step 1] Loading CSV files...\n")

    # Core tables
    claims = load_csv(data_dir, "claims.csv")
    claims_transactions = load_csv(data_dir, "claims_transactions.csv")
    encounters = load_csv(data_dir, "encounters.csv")
    patients = load_csv(data_dir, "patients.csv")

    # Reference tables
    providers = load_csv(data_dir, "providers.csv")
    organizations = load_csv(data_dir, "organizations.csv")
    payers = load_csv(data_dir, "payers.csv")

    # Clinical tables
    procedures = load_csv(data_dir, "procedures.csv")
    conditions = load_csv(data_dir, "conditions.csv")
    medications = load_csv(data_dir, "medications.csv")

    # =========================================================================
    # Step 2: Prepare reference tables with prefixed column names
    # =========================================================================
    print("\n[Step 2] Preparing reference tables...\n")

    # Rename columns in reference tables to avoid conflicts
    patients_renamed = patients.rename(columns={
        col: f'PATIENT_{col}' if col != 'Id' else col
        for col in patients.columns
    })

    providers_renamed = providers.rename(columns={
        col: f'PROVIDER_{col}' if col != 'Id' else col
        for col in providers.columns
    })

    organizations_renamed = organizations.rename(columns={
        col: f'ORG_{col}' if col != 'Id' else col
        for col in organizations.columns
    })

    payers_renamed = payers.rename(columns={
        col: f'PAYER_{col}' if col != 'Id' else col
        for col in payers.columns
    })

    # =========================================================================
    # Step 3: Aggregate clinical data by encounter
    # =========================================================================
    print("[Step 3] Aggregating clinical data by encounter...\n")

    # Aggregate procedures per encounter
    if not procedures.empty:
        procedures_agg = aggregate_clinical_data(
            procedures,
            ['PATIENT', 'ENCOUNTER'],
            'CODE',
            'DESCRIPTION'
        )
        procedures_agg = procedures_agg.rename(columns={
            'CODE_LIST': 'PROCEDURE_CODES',
            'DESCRIPTION_LIST': 'PROCEDURE_DESCRIPTIONS'
        })
        print(f"  Procedures aggregated: {len(procedures_agg):,} encounter groups")
    else:
        procedures_agg = pd.DataFrame(columns=['PATIENT', 'ENCOUNTER', 'PROCEDURE_CODES', 'PROCEDURE_DESCRIPTIONS'])

    # Aggregate conditions/diagnoses per encounter
    if not conditions.empty:
        conditions_agg = aggregate_clinical_data(
            conditions,
            ['PATIENT', 'ENCOUNTER'],
            'CODE',
            'DESCRIPTION'
        )
        conditions_agg = conditions_agg.rename(columns={
            'CODE_LIST': 'CONDITION_CODES',
            'DESCRIPTION_LIST': 'CONDITION_DESCRIPTIONS'
        })
        print(f"  Conditions aggregated: {len(conditions_agg):,} encounter groups")
    else:
        conditions_agg = pd.DataFrame(columns=['PATIENT', 'ENCOUNTER', 'CONDITION_CODES', 'CONDITION_DESCRIPTIONS'])

    # Aggregate medications per encounter
    if not medications.empty:
        medications_agg = aggregate_clinical_data(
            medications,
            ['PATIENT', 'ENCOUNTER'],
            'CODE',
            'DESCRIPTION'
        )
        medications_agg = medications_agg.rename(columns={
            'CODE_LIST': 'MEDICATION_CODES',
            'DESCRIPTION_LIST': 'MEDICATION_DESCRIPTIONS'
        })
        print(f"  Medications aggregated: {len(medications_agg):,} encounter groups")
    else:
        medications_agg = pd.DataFrame(columns=['PATIENT', 'ENCOUNTER', 'MEDICATION_CODES', 'MEDICATION_DESCRIPTIONS'])

    # =========================================================================
    # Step 4: Join claims with claims_transactions
    # =========================================================================
    print("\n[Step 4] Joining claims with transactions...\n")

    # Rename claims columns to avoid conflicts (many columns overlap)
    claims_renamed = claims.rename(columns={
        'Id': 'CLAIM_ID',
        'PATIENTID': 'CLAIM_PATIENTID',
        'PROVIDERID': 'CLAIM_PROVIDERID',
        'DEPARTMENTID': 'CLAIM_DEPARTMENTID',
        'APPOINTMENTID': 'CLAIM_APPOINTMENTID',
        'SUPERVISINGPROVIDERID': 'CLAIM_SUPERVISINGPROVIDERID'
    })

    # Join claims with transactions
    claims_with_trans = pd.merge(
        claims_transactions,
        claims_renamed,
        left_on='CLAIMID',
        right_on='CLAIM_ID',
        how='left'
    )
    print(f"  Claims + Transactions: {len(claims_with_trans):,} rows")

    # =========================================================================
    # Step 5: Join with encounters
    # =========================================================================
    print("\n[Step 5] Joining with encounters...\n")

    # Rename encounter columns
    encounters_renamed = encounters.rename(columns={
        'Id': 'ENCOUNTER_ID',
        'START': 'ENCOUNTER_START',
        'STOP': 'ENCOUNTER_STOP',
        'CODE': 'ENCOUNTER_CODE',
        'DESCRIPTION': 'ENCOUNTER_DESCRIPTION',
        'REASONCODE': 'ENCOUNTER_REASONCODE',
        'REASONDESCRIPTION': 'ENCOUNTER_REASONDESCRIPTION'
    })

    # Join with encounters using CLAIM_APPOINTMENTID (the encounter reference from claims)
    joined = pd.merge(
        claims_with_trans,
        encounters_renamed,
        left_on='CLAIM_APPOINTMENTID',
        right_on='ENCOUNTER_ID',
        how='left',
        suffixes=('', '_ENC')
    )
    print(f"  After encounter join: {len(joined):,} rows")

    # =========================================================================
    # Step 6: Join with patients
    # =========================================================================
    print("\n[Step 6] Joining with patients...\n")

    joined = pd.merge(
        joined,
        patients_renamed,
        left_on='CLAIM_PATIENTID',
        right_on='Id',
        how='left',
        suffixes=('', '_PAT')
    )
    joined = joined.drop(columns=['Id'], errors='ignore')
    print(f"  After patient join: {len(joined):,} rows")

    # =========================================================================
    # Step 7: Join with providers
    # =========================================================================
    print("\n[Step 7] Joining with providers...\n")

    # Use PROVIDERID from claims_transactions for the primary provider join
    joined = pd.merge(
        joined,
        providers_renamed,
        left_on='PROVIDERID',
        right_on='Id',
        how='left',
        suffixes=('', '_PROV')
    )
    joined = joined.drop(columns=['Id'], errors='ignore')
    print(f"  After provider join: {len(joined):,} rows")

    # =========================================================================
    # Step 8: Join with organizations
    # =========================================================================
    print("\n[Step 8] Joining with organizations...\n")

    joined = pd.merge(
        joined,
        organizations_renamed,
        left_on='ORGANIZATION',
        right_on='Id',
        how='left',
        suffixes=('', '_ORG')
    )
    joined = joined.drop(columns=['Id'], errors='ignore')
    print(f"  After organization join: {len(joined):,} rows")

    # =========================================================================
    # Step 9: Join with payers
    # =========================================================================
    print("\n[Step 9] Joining with payers...\n")

    joined = pd.merge(
        joined,
        payers_renamed,
        left_on='PAYER',
        right_on='Id',
        how='left',
        suffixes=('', '_PAYER')
    )
    joined = joined.drop(columns=['Id'], errors='ignore')
    print(f"  After payer join: {len(joined):,} rows")

    # =========================================================================
    # Step 10: Join with aggregated clinical data
    # =========================================================================
    print("\n[Step 10] Joining with clinical data...\n")

    # Join procedures
    joined = pd.merge(
        joined,
        procedures_agg,
        left_on=['CLAIM_PATIENTID', 'ENCOUNTER_ID'],
        right_on=['PATIENT', 'ENCOUNTER'],
        how='left',
        suffixes=('', '_PROC')
    )
    joined = joined.drop(columns=['PATIENT', 'ENCOUNTER'], errors='ignore')
    print(f"  After procedures join: {len(joined):,} rows")

    # Join conditions
    joined = pd.merge(
        joined,
        conditions_agg,
        left_on=['CLAIM_PATIENTID', 'ENCOUNTER_ID'],
        right_on=['PATIENT', 'ENCOUNTER'],
        how='left',
        suffixes=('', '_COND')
    )
    joined = joined.drop(columns=['PATIENT', 'ENCOUNTER'], errors='ignore')
    print(f"  After conditions join: {len(joined):,} rows")

    # Join medications
    joined = pd.merge(
        joined,
        medications_agg,
        left_on=['CLAIM_PATIENTID', 'ENCOUNTER_ID'],
        right_on=['PATIENT', 'ENCOUNTER'],
        how='left',
        suffixes=('', '_MED')
    )
    joined = joined.drop(columns=['PATIENT', 'ENCOUNTER'], errors='ignore')
    print(f"  After medications join: {len(joined):,} rows")

    # =========================================================================
    # Step 11: Clean up and organize columns
    # =========================================================================
    print("\n[Step 11] Cleaning up columns...\n")

    # Remove duplicate columns that may have been created
    duplicate_cols = [col for col in joined.columns if col.endswith('_x') or col.endswith('_y')]
    if duplicate_cols:
        print(f"  Removing {len(duplicate_cols)} duplicate columns")
        joined = joined.drop(columns=duplicate_cols, errors='ignore')

    # =========================================================================
    # Step 12: Save the joined data
    # =========================================================================
    print("\n[Step 12] Saving joined data...\n")

    output_path = Path(output_file)
    joined.to_csv(output_path, index=False)

    file_size_mb = output_path.stat().st_size / (1024 * 1024)

    print("=" * 60)
    print("COMPLETE!")
    print("=" * 60)
    print(f"\nOutput file: {output_file}")
    print(f"Total rows: {len(joined):,}")
    print(f"Total columns: {len(joined.columns)}")
    print(f"File size: {file_size_mb:.2f} MB")

    # Print column summary
    print("\n" + "-" * 60)
    print("Column Categories:")
    print("-" * 60)

    col_categories = {
        'Transaction': [c for c in joined.columns if 'TRANS' in c or c in ['ID', 'CLAIMID', 'TYPE', 'AMOUNT', 'PAYMENTS', 'ADJUSTMENTS']],
        'Claim': [c for c in joined.columns if 'CLAIM' in c or 'DIAGNOSIS' in c or 'STATUS' in c],
        'Encounter': [c for c in joined.columns if 'ENCOUNTER' in c or 'APPOINTMENT' in c],
        'Patient': [c for c in joined.columns if 'PATIENT' in c],
        'Provider': [c for c in joined.columns if 'PROVIDER' in c],
        'Organization': [c for c in joined.columns if 'ORG_' in c],
        'Payer': [c for c in joined.columns if 'PAYER' in c],
        'Clinical': [c for c in joined.columns if any(x in c for x in ['PROCEDURE', 'CONDITION', 'MEDICATION'])]
    }

    for category, cols in col_categories.items():
        if cols:
            print(f"  {category}: {len(cols)} columns")

    return joined


if __name__ == "__main__":
    df = main()

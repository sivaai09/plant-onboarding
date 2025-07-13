import argparse
import yaml
from collections import Counter

from config import get_gcp_project_id
from core.bigquery_client import BigQueryClient

CONFIG_FILE_PATH = "plant_onboarding_config.yaml"

def analyze_dataset_and_generate_config(source_project_id: str, source_dataset_id: str):
    """
    Scans a BigQuery dataset to find a common discriminator column,
    finds its unique values, and generates a plant_onboarding_config.yaml file.
    """
    print(f"Starting analysis of dataset: {source_project_id}.{source_dataset_id}")
    
    try:
        auth_project_id = get_gcp_project_id()
        client = BigQueryClient(project_id=auth_project_id)
    except ValueError as e:
        print(f"[ERROR] Configuration Error: {e}")
        return

    # 1. Get all tables from the dataset
    print("\nStep 1: Fetching table schemas...")
    try:
        tables = client.get_tables(source_dataset_id, project_id=source_project_id)
        if not tables:
            print(f"[ERROR] No tables found in dataset '{source_dataset_id}'. Please check the dataset name and permissions.")
            return
        print(f"Found {len(tables)} tables: {[t.name for t in tables]}")
    except Exception as e:
        print(f"[ERROR] Could not fetch tables: {e}")
        return

    # 2. Find common columns
    print("\nStep 2: Analyzing columns to find common discriminators...")
    if not tables:
        print("[ERROR] No tables found to analyze.")
        return

    columns_by_table = {table.name: {col.name for col in table.columns} for table in tables}
    common_columns = set.intersection(*columns_by_table.values())
    
    if not common_columns:
        print("[ERROR] No columns were found to be common across all tables. Cannot determine a discriminator.")
        return
    print(f"Found {len(common_columns)} common columns.")

    # 3. Analyze cardinality
    print("\nStep 3: Analyzing cardinality of common columns (this may take a moment)...")
    column_cardinality = {}
    for column_name in common_columns:
        first_table = tables[0]
        query = f"SELECT COUNT(DISTINCT {column_name}) as unique_count FROM `{first_table.project}.{first_table.dataset}.{first_table.name}`"
        try:
            print(f"  - Analyzing column: '{column_name}'...")
            results = client.client.query(query).result()
            for row in results:
                column_cardinality[column_name] = row.unique_count
        except Exception as e:
            print(f"[WARNING] Could not analyze cardinality for column '{column_name}': {e}")
            column_cardinality[column_name] = 999999 

    sorted_candidates = sorted(column_cardinality.items(), key=lambda item: item[1])

    # 4. Present candidates and get user choice
    print("\nStep 4: Candidate Discriminator Columns (ranked by lowest unique values)")
    for i, (col, count) in enumerate(sorted_candidates):
        print(f"  {i+1}. Column: '{col}' (Unique Values: {count})")

    chosen_discriminator, _ = sorted_candidates[0]
    print(f"\nStep 5: Automatically selecting best candidate: '{chosen_discriminator}'")

    # 5. Get all unique values for the chosen discriminator
    print(f"\nStep 5: Fetching all unique values for '{chosen_discriminator}'...")
    all_values = set()
    for table in tables:
        query = f"SELECT DISTINCT {chosen_discriminator} FROM `{table.project}.{table.dataset}.{table.name}`"
        try:
            print(f"  - Querying table: {table.name}")
            query_job = client.client.query(query)
            for row in query_job:
                all_values.add(row[0])
        except Exception as e:
            print(f"[WARNING] Could not fetch unique values from table '{table.name}': {e}")

    print(f"Found {len(all_values)} unique values.")

    # 6. Generate the YAML config file
    print(f"\nStep 6: Generating `{CONFIG_FILE_PATH}`...")
    plant_map = {}
    for i, value in enumerate(sorted(list(all_values))):
        plant_key = f"plant_{i+1}"
        plant_map[plant_key] = {
            'discriminator_value': value,
            'description': f"Auto-generated description for: {value}"
        }

    config_data = {
        'source_dataset': f"{source_project_id}.{source_dataset_id}",
        'discriminator_column': chosen_discriminator,
        'plants': plant_map
    }

    try:
        with open(CONFIG_FILE_PATH, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)
        print(f"\nSuccessfully generated `{CONFIG_FILE_PATH}`!")
        print("Configuration generation complete.")
    except Exception as e:
        print(f"[ERROR] Failed to write config file: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a plant onboarding config by scanning a BigQuery dataset.")
    parser.add_argument("--project_id", required=True, help="The BigQuery Project ID to scan (e.g., 'bigquery-public-data').")
    parser.add_argument("--dataset_id", required=True, help="The BigQuery Dataset ID to scan (e.g., 'austin_incidents').")
    args = parser.parse_args()
    
    analyze_dataset_and_generate_config(args.project_id, args.dataset_id)
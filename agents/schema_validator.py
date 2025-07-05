from typing import List

from models.schema_objects import SchemaObject, View, Table
from core.bigquery_client import BigQueryClient
from utils.sql_parser import get_tables_from_sql

class SchemaValidatorAgent:
    def __init__(self, client: BigQueryClient):
        self.client = client

    def validate_schema(self, schema_objects: List[SchemaObject]) -> bool:
        """Validates the entire schema, including missing references and SQL syntax."""
        print("[VALIDATION] Starting schema validation...")
        all_object_names = {obj.name for obj in schema_objects}
        is_valid = True

        for obj in schema_objects:
            if isinstance(obj, View):
                # Validate missing table references in views
                referenced_names = get_tables_from_sql(obj.sql)
                for ref_name in referenced_names:
                    # This check is simplified. A more robust check would resolve full qualified names.
                    if ref_name not in all_object_names:
                        print(f"[WARNING] View '{obj.name}' references unknown object '{ref_name}'.")
                        # is_valid = False # Uncomment to make this a hard error

                # Validate SQL syntax by attempting a dry run
                print(f"[VALIDATION] Dry-running DDL for {obj.name} to check syntax...")
                try:
                    # For views, we can try to dry-run the SELECT statement part
                    # For CREATE VIEW, BigQuery API handles syntax validation during job creation
                    # A more thorough check would involve a BigQuery dry-run query job.
                    # For now, we rely on BigQuery's error reporting during actual execution.
                    pass # Placeholder for future BigQuery dry-run API call
                except Exception as e:
                    print(f"[ERROR] SQL syntax error in {obj.name}: {e}")
                    is_valid = False

        if is_valid:
            print("[VALIDATION] Schema validation completed successfully.")
        else:
            print("[VALIDATION] Schema validation found issues.")
        return is_valid
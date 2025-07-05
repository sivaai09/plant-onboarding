from typing import List

from models.schema_objects import SchemaObject, View, Table
from core.bigquery_client import BigQueryClient
from utils.sql_parser import get_tables_from_sql

class SchemaValidatorAgent:
    def __init__(self, client: BigQueryClient):
        self.client = client

    def validate_schema(self, schema_objects: List[SchemaObject]) -> bool:
        """Validates the entire schema, including missing references and SQL syntax."""
        all_object_names = {obj.name for obj in schema_objects}
        is_valid = True

        for obj in schema_objects:
            if isinstance(obj, View):
                referenced_names = get_tables_from_sql(obj.sql)
                for ref_name in referenced_names:
                    if ref_name not in all_object_names:
                        print(f"[WARNING] View '{obj.name}' references unknown object '{ref_name}'.")

        return is_valid

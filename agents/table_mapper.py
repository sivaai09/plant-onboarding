from typing import List, Dict
from models.schema_objects import Table
from utils.naming_utils import generate_new_name

class TableMapperAgent:
    def map_tables(self, tables: List[Table], source_plant: str, target_plant: str) -> Dict[str, str]:
        """Maps table names from the source plant to the target plant, including fully qualified names."""
        mapping = {}
        for table in tables:
            # Map the simple table name
            mapping[table.name] = generate_new_name(table.name, source_plant, target_plant)

            # Map the dataset.table name
            old_dataset_table = f"{table.dataset}.{table.name}"
            new_dataset_table = f"{target_plant}.{generate_new_name(table.name, source_plant, target_plant)}"
            mapping[old_dataset_table] = new_dataset_table

            # Map the project.dataset.table name
            old_full_table = f"{table.project}.{table.dataset}.{table.name}"
            new_full_table = f"{table.project}.{target_plant}.{generate_new_name(table.name, source_plant, target_plant)}"
            mapping[old_full_table] = new_full_table

        return mapping
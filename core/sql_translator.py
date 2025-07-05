from typing import Dict
from utils.sql_parser import replace_table_names

def translate_view_sql(view_sql: str, table_mapping: Dict[str, str], view_mapping: Dict[str, str]) -> str:
    """Translate view SQL from one plant to another."""
    # First, replace table names using the more robust parser
    translated_sql = replace_table_names(view_sql, table_mapping)

    # Then, replace view names (if any) - this might need similar robustness
    translated_sql = replace_table_names(translated_sql, view_mapping) # Re-using for view names for simplicity

    return translated_sql
from typing import Dict
from utils.sql_parser import replace_table_names

def translate_view_sql(view_sql: str, table_mapping: Dict[str, str], view_mapping: Dict[str, str]) -> str:
    """Translate view SQL from one plant to another."""
    translated_sql = replace_table_names(view_sql, table_mapping)
    translated_sql = replace_table_names(translated_sql, view_mapping)
    return translated_sql

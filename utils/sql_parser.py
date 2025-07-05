import sqlparse
import re
from typing import List, Dict

def get_tables_from_sql(sql: str) -> List[str]:
    """Extracts table names (including fully qualified) from a SQL query."""
    tables = set()
    parsed = sqlparse.parse(sql)
    for statement in parsed:
        for token in statement.tokens:
            if isinstance(token, sqlparse.sql.Identifier):
                tables.add(token.get_real_name())
            elif token.is_group and token.get_type() == 'FROM':
                for sub_token in token.tokens:
                    if isinstance(sub_token, sqlparse.sql.IdentifierList):
                        for identifier in sub_token.get_identifiers():
                            tables.add(identifier.get_real_name())
                    elif isinstance(sub_token, sqlparse.sql.Identifier):
                        tables.add(sub_token.get_real_name())
    return list(tables)

def replace_table_names(sql: str, mapping: Dict[str, str]) -> str:
    """Replaces table names in a SQL query based on a mapping, handling fully qualified names.
    Prioritizes longer matches to avoid partial replacements.
    """
    # Sort mapping by length of old_name descending to ensure longer, more specific matches are replaced first
    sorted_mapping = sorted(mapping.items(), key=lambda item: len(item[0]), reverse=True)

    modified_sql = sql
    for old_name, new_name in sorted_mapping:
        # Escape special characters in old_name for regex matching
        escaped_old_name = re.escape(old_name)

        # Create a regex pattern to match the old_name as a whole word, considering various delimiters
        # This pattern tries to be flexible with backticks and dots, and ensures whole word matching
        # It looks for the old_name surrounded by non-word characters or start/end of string
        # This is still a heuristic and might need further refinement for all edge cases in SQL.
        pattern = r'\b' + escaped_old_name.replace(r'\.', r'(?:\.|`\.`)') + r'\b'
        
        # Replace all occurrences of the old_name with the new_name
        modified_sql = re.sub(pattern, new_name, modified_sql, flags=re.IGNORECASE)

    return modified_sql

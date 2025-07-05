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
    sorted_mapping = sorted(mapping.items(), key=lambda item: len(item[0]), reverse=True)

    modified_sql = sql
    for old_name, new_name in sorted_mapping:
        escaped_old_name = re.escape(old_name)
        
        pattern = r'\b' + escaped_old_name.replace(r'\.', r'(?:\.|`\.`)') + r'\b'
        
        modified_sql = re.sub(pattern, new_name, modified_sql, flags=re.IGNORECASE)

    return modified_sql
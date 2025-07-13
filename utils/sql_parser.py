import sqlparse
from typing import List

def get_tables_from_sql(sql: str) -> List[str]:
    """Extracts table and view names from a SQL query."""
    tables = set()
    parsed = sqlparse.parse(sql)
    for statement in parsed:
        _extract_from_tokens(statement.tokens, tables)
    return list(tables)

def _extract_from_tokens(tokens, tables):
    for token in tokens:
        if isinstance(token, sqlparse.sql.Identifier):
            tables.add(token.get_real_name())
        elif isinstance(token, sqlparse.sql.IdentifierList):
            for identifier in token.get_identifiers():
                if isinstance(identifier, sqlparse.sql.Identifier):
                    tables.add(identifier.get_real_name())
        elif token.is_group:
            _extract_from_tokens(token.tokens, tables)

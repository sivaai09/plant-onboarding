from dataclasses import dataclass, field
from typing import List, Dict, Any

@dataclass
class Column:
    name: str
    data_type: str
    mode: str = "NULLABLE"

@dataclass
class SchemaObject:
    name: str
    project: str
    dataset: str

@dataclass
class Table(SchemaObject):
    columns: List[Column] = field(default_factory=list)
    schema_type: str = "TABLE"

@dataclass
class View(SchemaObject):
    sql: str
    dependencies: List[str] = field(default_factory=list)
    schema_type: str = "VIEW"

@dataclass
class MaterializedView(View):
    partition_column: str = None
    cluster_columns: List[str] = field(default_factory=list)
    refresh_schedule: str = None
    auto_refresh: bool = True
    schema_type: str = "MATERIALIZED_VIEW"

@dataclass
class PlantSchema:
    tables: List[Table]
    views: List[View]
    materialized_views: List[MaterializedView]
    dependencies: Dict[str, List[str]] = field(default_factory=dict)
from typing import List
import networkx as nx
from models.schema_objects import SchemaObject

def resolve_creation_order(schema_objects: List[SchemaObject], dependencies: dict) -> List[SchemaObject]:
    """Determine correct order to create tables and views"""
    graph = nx.DiGraph()

    for obj in schema_objects:
        graph.add_node(obj.name)

    for obj_name, deps in dependencies.items():
        for dep_name in deps:
            graph.add_edge(dep_name, obj_name)

    try:
        sorted_nodes = list(nx.topological_sort(graph))
        name_to_obj = {obj.name: obj for obj in schema_objects}
        return [name_to_obj[name] for name in sorted_nodes]
    except nx.NetworkXUnfeasible:
        raise ValueError("Circular dependency detected in schema.")

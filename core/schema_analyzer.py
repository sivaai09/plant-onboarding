from typing import List, Dict

from core.bigquery_client import BigQueryClient
from models.schema_objects import PlantSchema, Table, View, MaterializedView
from utils.sql_parser import get_tables_from_sql
import streamlit as st # Import streamlit for st.write

def analyze_plant_schema(client: BigQueryClient, plant_name: str) -> PlantSchema:
    """Extract tables, views, and their relationships"""

    st.write(f"Attempting to retrieve tables for dataset: {plant_name}")
    tables = client.get_tables(plant_name)
    st.write(f"Retrieved {len(tables)} tables for {plant_name}")

    st.write(f"Attempting to retrieve views for dataset: {plant_name}")
    views = client.get_views(plant_name)
    st.write(f"Retrieved {len(views)} views for {plant_name}")

    st.write(f"Attempting to retrieve materialized views for dataset: {plant_name}")
    materialized_views = client.get_materialized_views(plant_name)
    st.write(f"Retrieved {len(materialized_views)} materialized views for {plant_name}")

    all_schema_objects = {obj.name: obj for obj in tables + views + materialized_views}
    dependencies: Dict[str, List[str]] = {}

    # Initialize dependencies for all objects
    for obj_name in all_schema_objects.keys():
        dependencies[obj_name] = []

    # Build dependency graph for views and materialized views
    for view_obj in views + materialized_views:
        referenced_tables_and_views = get_tables_from_sql(view_obj.sql)
        for ref_name in referenced_tables_and_views:
            if ref_name in all_schema_objects:
                dependencies[view_obj.name].append(ref_name)

    return PlantSchema(
        tables=tables,
        views=views,
        materialized_views=materialized_views,
        dependencies=dependencies
    )

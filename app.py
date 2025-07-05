import streamlit as st
import graphviz

from config import get_gcp_project_id
from core.bigquery_client import BigQueryClient
from core.schema_analyzer import analyze_plant_schema
from core.dependency_resolver import resolve_creation_order
from core.sql_translator import translate_view_sql
from agents.table_mapper import TableMapperAgent
from agents.view_mapper import ViewMapperAgent
from agents.ddl_generator import generate_ddl
from agents.schema_validator import SchemaValidatorAgent
from models.schema_objects import Table, View, MaterializedView
from utils.naming_utils import generate_new_name

st.set_page_config(layout="wide")
st.title("BigQuery Plant Onboarding System")

# --- Sidebar Configuration ---
st.sidebar.header("Onboarding Configuration")

try:
    project_id = get_gcp_project_id()
    st.sidebar.success(f"GCP Project ID: {project_id}")
except ValueError as e:
    st.sidebar.error(f"Configuration Error: {e}")
    st.stop()

reference_plant = st.sidebar.text_input("Reference Plant Dataset ID", "plant1")
new_plant = st.sidebar.text_input("New Plant Dataset ID", "plant2")
include_views = st.sidebar.checkbox("Include Views (and Materialized Views)", True)
dry_run = st.sidebar.checkbox("Dry Run (Preview DDL only)", True)

# --- Main Application Logic ---
if st.sidebar.button("Start Onboarding"):
    st.subheader("1. Analyzing Reference Plant Schema")
    client = BigQueryClient(project_id=project_id)
    schema = analyze_plant_schema(client, reference_plant)

    st.write(f"Found {len(schema.tables)} tables, {len(schema.views)} views, and {len(schema.materialized_views)} materialized views in `{reference_plant}`.")

    # --- Schema Tree View ---
    st.subheader("2. Reference Schema Tree View")
    st.write("This shows the structure of the reference plant's schema.")
    schema_tree_data = []
    for table in schema.tables:
        schema_tree_data.append({"Object": table.name, "Type": "Table", "SQL": "N/A"})
    for view in schema.views:
        schema_tree_data.append({"Object": view.name, "Type": "View", "SQL": view.sql})
    for mv in schema.materialized_views:
        schema_tree_data.append({"Object": mv.name, "Type": "Materialized View", "SQL": mv.sql})
    st.dataframe(schema_tree_data, use_container_width=True)

    # --- Mapping Phase ---
    st.subheader("3. Mapping and Translation")
    table_mapper = TableMapperAgent()
    table_mapping = table_mapper.map_tables(schema.tables, reference_plant, new_plant)

    view_mapper = ViewMapperAgent(project_id=project_id) # Pass project_id to ViewMapperAgent
    new_schema_objects = []
    translated_view_sqls = {}

    for table in schema.tables:
        new_table = Table(
            name=table_mapping.get(table.name, table.name),
            project=project_id,
            dataset=new_plant,
            columns=table.columns
        )
        new_schema_objects.append(new_table)

    if include_views:
        for view in schema.views:
            with st.spinner(f"Translating view {view.name} with AI..."):
                translated_view = view_mapper.map_view(view, table_mapping, new_plant)
                new_schema_objects.append(translated_view)
                translated_view_sqls[view.name] = translated_view.sql
        for mv in schema.materialized_views:
            # For MVs, we'll just do name mapping for now, AI translation can be added later
            new_mv = MaterializedView(
                name=generate_new_name(mv.name, reference_plant, new_plant),
                project=project_id,
                dataset=new_plant,
                sql=translate_view_sql(mv.sql, table_mapping, {}), # Translate SQL for MVs
                partition_column=mv.partition_column,
                cluster_columns=mv.cluster_columns,
                refresh_schedule=mv.refresh_schedule,
                auto_refresh=mv.auto_refresh
            )
            new_schema_objects.append(new_mv)

    st.success("Schema objects mapped and views translated.")

    # --- Selective Migration (Placeholder for now, all objects are processed) ---
    st.subheader("4. Selective Migration (All objects selected)")
    st.info("In this version, all detected and translated objects will be processed.")

    # --- Dependency Resolution ---
    st.subheader("5. Resolving Creation Order")
    translated_dependencies = {}
    all_mappings = {**table_mapping}
    if include_views:
        for view in schema.views:
            all_mappings[view.name] = generate_new_name(view.name, reference_plant, new_plant)
        for mv in schema.materialized_views:
            all_mappings[mv.name] = generate_new_name(mv.name, reference_plant, new_plant)

    for obj_name, deps in schema.dependencies.items():
        new_obj_name = all_mappings.get(obj_name)
        if new_obj_name:
            translated_deps = [all_mappings.get(dep) for dep in deps if all_mappings.get(dep)]
            translated_dependencies[new_obj_name] = translated_deps

    ordered_objects = resolve_creation_order(new_schema_objects, translated_dependencies)
    st.write(f"Determined creation order for {len(ordered_objects)} objects.")

    # --- Dependency Visualization ---
    st.subheader("6. Dependency Visualization")
    graph = graphviz.Digraph(comment='BigQuery Schema Dependencies')
    for obj in ordered_objects:
        graph.node(obj.name, label=f"{obj.name}\n({obj.schema_type})")
    for obj_name, deps in translated_dependencies.items():
        for dep in deps:
            if dep in all_mappings.values(): # Only draw edges for objects we are creating
                graph.edge(dep, obj_name)
    st.graphviz_chart(graph)

    # --- SQL Preview ---
    st.subheader("7. SQL DDL Preview")
    st.write("Review the generated DDL statements.")
    ddl_statements = []
    for obj in ordered_objects:
        ddl = generate_ddl(obj)
        ddl_statements.append((obj.name, obj.schema_type, ddl))

    for name, obj_type, ddl in ddl_statements:
        with st.expander(f"{obj_type}: {name}"):
            st.code(ddl, language="sql")

    # --- Validation Phase ---
    st.subheader("8. Schema Validation")
    validator = SchemaValidatorAgent(client=client)
    if validator.validate_schema(new_schema_objects):
        st.success("Schema validation passed!")
    else:
        st.error("Schema validation failed. Please review warnings/errors above.")
        st.stop()

    # --- Execution Phase ---
    st.subheader("9. Execution")
    if not dry_run:
        st.write(f"Creating dataset `{new_plant}` if it doesn't exist...")
        client.create_dataset_if_not_exists(new_plant)

        progress_bar = st.progress(0)
        status_text = st.empty()

        for i, obj in enumerate(ordered_objects):
            ddl = generate_ddl(obj)
            status_text.text(f"Processing {obj.schema_type}: {obj.name} ({i+1}/{len(ordered_objects)})")
            client.execute_ddl(ddl, dry_run=False) # Force execution
            progress_bar.progress((i + 1) / len(ordered_objects))
        st.success("Onboarding process completed successfully!")
    else:
        st.info("Dry Run mode: No DDL was executed. Review the DDL Preview above.")

    st.balloons()
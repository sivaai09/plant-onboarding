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

# Initialize session state for schema and objects if not already present
if 'schema' not in st.session_state:
    st.session_state.schema = None
if 'new_schema_objects' not in st.session_state:
    st.session_state.new_schema_objects = []
if 'selected_objects' not in st.session_state:
    st.session_state.selected_objects = []
if 'table_mapping' not in st.session_state:
    st.session_state.table_mapping = {}
if 'view_mapping' not in st.session_state:
    st.session_state.view_mapping = {}
if 'translated_view_sqls' not in st.session_state:
    st.session_state.translated_view_sqls = {}

# --- Main Application Logic ---
if st.sidebar.button("Analyze Schema"):
    st.session_state.schema = None # Reset schema on new analysis
    st.session_state.new_schema_objects = []
    st.session_state.selected_objects = []
    st.session_state.table_mapping = {}
    st.session_state.view_mapping = {}
    st.session_state.translated_view_sqls = {}

    st.subheader("1. Analyzing Reference Plant Schema")
    client = BigQueryClient(project_id=project_id)
    schema = analyze_plant_schema(client, reference_plant)
    st.session_state.schema = schema

    st.write(f"Found {len(schema.tables)} tables, {len(schema.views)} views, and {len(schema.materialized_views)} materialized views in `{reference_plant}`.")

    # --- Schema Tree View ---
    st.subheader("2. Reference Schema Tree View")
    st.write("This shows the structure of the reference plant's schema.")
    schema_tree_data = []
    for table in schema.tables:
        schema_tree_data.append({"Object Name": table.name, "Type": "Table", "Source SQL": "N/A"})
    for view in schema.views:
        schema_tree_data.append({"Object Name": view.name, "Type": "View", "Source SQL": view.sql})
    for mv in schema.materialized_views:
        schema_tree_data.append({"Object Name": mv.name, "Type": "Materialized View", "Source SQL": mv.sql})
    st.dataframe(schema_tree_data, use_container_width=True)

    # --- Mapping and Translation ---
    st.subheader("3. Mapping and Translation")
    table_mapper = TableMapperAgent()
    st.session_state.table_mapping = table_mapper.map_tables(schema.tables, reference_plant, new_plant)

    view_mapper = ViewMapperAgent(project_id=project_id) # Pass project_id to ViewMapperAgent
    new_schema_objects_temp = []

    for table in schema.tables:
        new_table = Table(
            name=st.session_state.table_mapping.get(table.name, table.name),
            project=project_id,
            dataset=new_plant,
            columns=table.columns
        )
        new_schema_objects_temp.append(new_table)

    if include_views:
        for view in schema.views:
            with st.spinner(f"Translating view {view.name} with AI..."):
                translated_view = view_mapper.map_view(view, st.session_state.table_mapping, new_plant)
                if translated_view:
                    new_schema_objects_temp.append(translated_view)
                    st.session_state.translated_view_sqls[view.name] = translated_view.sql
                else:
                    st.error(f"Failed to translate view {view.name}. Skipping this view.")
        for mv in schema.materialized_views:
            new_mv = MaterializedView(
                name=generate_new_name(mv.name, reference_plant, new_plant),
                project=project_id,
                dataset=new_plant,
                sql=translate_view_sql(mv.sql, st.session_state.table_mapping, {}), # Translate SQL for MVs
                partition_column=mv.partition_column,
                cluster_columns=mv.cluster_columns,
                refresh_schedule=mv.refresh_schedule,
                auto_refresh=mv.auto_refresh
            )
            new_schema_objects_temp.append(new_mv)

    st.session_state.new_schema_objects = new_schema_objects_temp
    st.success("Schema objects mapped and views translated.")

    # --- Selective Migration (Placeholder for now, all objects are processed) ---
    st.subheader("4. Selective Migration")
    st.write("Select the objects you wish to onboard to the new plant.")
    object_names = [obj.name for obj in st.session_state.new_schema_objects]
    st.session_state.selected_objects = st.multiselect(
        "Choose objects to migrate:",
        options=object_names,
        default=object_names # Select all by default
    )

    st.info(f"{len(st.session_state.selected_objects)} objects selected for migration.")

# Only proceed if schema has been analyzed and objects are available
if st.session_state.schema and st.session_state.new_schema_objects:
    st.subheader("5. Resolving Creation Order")
    # Filter new_schema_objects based on selection
    filtered_new_schema_objects = [obj for obj in st.session_state.new_schema_objects if obj.name in st.session_state.selected_objects]

    translated_dependencies = {}
    all_mappings = {**st.session_state.table_mapping}
    if include_views:
        for view in st.session_state.schema.views:
            all_mappings[view.name] = generate_new_name(view.name, reference_plant, new_plant)
        for mv in st.session_state.schema.materialized_views:
            all_mappings[mv.name] = generate_new_name(mv.name, reference_plant, new_plant)

    for obj_name, deps in st.session_state.schema.dependencies.items():
        new_obj_name = all_mappings.get(obj_name)
        if new_obj_name:
            translated_deps = [all_mappings.get(dep) for dep in deps if all_mappings.get(dep)]
            translated_dependencies[new_obj_name] = translated_deps

    ordered_objects = resolve_creation_order(filtered_new_schema_objects, translated_dependencies)
    st.write(f"Determined creation order for {len(ordered_objects)} objects.")

    # --- Dependency Visualization ---
    st.subheader("6. Dependency Visualization")
    graph = graphviz.Digraph(comment='BigQuery Schema Dependencies')
    for obj in ordered_objects:
        graph.node(obj.name, label=f"{obj.name}\n({obj.schema_type})")
    for obj_name, deps in translated_dependencies.items():
        for dep in deps:
            if dep in st.session_state.selected_objects: # Only draw edges for selected objects
                graph.edge(dep, obj_name)
    st.graphviz_chart(graph)

    # --- SQL Preview ---
    st.subheader("7. SQL DDL Preview")
    st.write("Review the generated DDL statements.")
    ddl_statements = []
    for obj in ordered_objects:
        ddl = generate_ddl(obj)
        original_sql = "N/A"
        if isinstance(obj, View) and obj.name in st.session_state.translated_view_sqls:
            for original_view_obj in st.session_state.schema.views:
                if original_view_obj.name == obj.name:
                    original_sql = original_view_obj.sql
                    break
        elif isinstance(obj, MaterializedView):
            for original_mv_obj in st.session_state.schema.materialized_views:
                if original_mv_obj.name == obj.name:
                    original_sql = original_mv_obj.sql
                    break

        ddl_statements.append((obj.name, obj.schema_type, ddl, original_sql))

    for name, obj_type, ddl, original_sql in ddl_statements:
        with st.expander(f"{obj_type}: {name}"):
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Original SQL (if applicable):**")
                st.code(original_sql, language="sql")
            with col2:
                st.write("**Translated DDL:**")
                st.code(ddl, language="sql")

    # --- Validation Phase ---
    st.subheader("8. Schema Validation")
    client = BigQueryClient(project_id=project_id) # Re-initialize client for validation
    validator = SchemaValidatorAgent(client=client)
    if validator.validate_schema(filtered_new_schema_objects):
        st.success("Schema validation passed!")
    else:
        st.error("Schema validation failed. Please review warnings/errors above.")
        st.stop()

    # --- Execution Phase ---
    st.subheader("9. Execution")
    if st.button("Execute Onboarding"):
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

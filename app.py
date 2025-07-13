import streamlit as st
import graphviz
import yaml
import os

from config import get_gcp_project_id
from core.bigquery_client import BigQueryClient
from core.schema_analyzer import analyze_plant_schema
from core.dependency_resolver import resolve_creation_order
from agents.table_mapper import TableMapperAgent
from agents.view_mapper import ViewMapperAgent
from agents.ddl_generator import generate_ddl, generate_data_load_sql
from agents.schema_validator import SchemaValidatorAgent
from models.schema_objects import Table, View, MaterializedView

# --- Utility Functions ---
def load_config():
    """Loads the plant onboarding configuration from YAML."""
    config_path = 'plant_onboarding_config.yaml'
    if not os.path.exists(config_path):
        st.error("CRITICAL: `plant_onboarding_config.yaml` not found in the root directory.")
        return None
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        st.error(f"Error loading or parsing `plant_onboarding_config.yaml`: {e}")
        return None

# --- Page and Sidebar Setup ---
st.set_page_config(layout="wide")
st.title("BigQuery Plant Onboarding System")

st.sidebar.header("Onboarding Configuration")

config = load_config()
if not config:
    st.stop()

source_dataset = config.get('source_dataset')
if not source_dataset:
    st.sidebar.error("CRITICAL: `source_dataset` not defined in `plant_onboarding_config.yaml`.")
    st.stop()

st.sidebar.info(f"Source Dataset: `{source_dataset}`")

try:
    project_id = get_gcp_project_id()
    st.sidebar.success(f"GCP Project ID: {project_id}")
except ValueError as e:
    st.sidebar.error(f"Configuration Error: {e}")
    st.stop()

reference_plant = st.sidebar.text_input("Reference Plant Dataset (Blueprint)", "plant1")
new_plant = st.sidebar.text_input("New Plant Dataset (Target)", "plant2")
include_views = st.sidebar.checkbox("Include Views (and Materialized Views)", True)
load_data = st.sidebar.checkbox(f"Load Table Data from `{source_dataset}`", True)
dry_run = st.sidebar.checkbox("Dry Run (Preview DDL only)", True)


# --- Session State Initialization ---
if 'schema' not in st.session_state:
    st.session_state.schema = None
if 'new_schema_objects' not in st.session_state:
    st.session_state.new_schema_objects = {}
if 'selected_objects' not in st.session_state:
    st.session_state.selected_objects = []
if 'table_mapping' not in st.session_state:
    st.session_state.table_mapping = {}
if 'view_instructions' not in st.session_state:
    st.session_state.view_instructions = {}

# --- Main Application Logic ---
if st.sidebar.button("Analyze Blueprint & Map Tables"):
    st.session_state.schema = None
    st.session_state.new_schema_objects = {}
    st.session_state.selected_objects = []
    st.session_state.table_mapping = {}
    st.session_state.view_instructions = {}

    st.subheader("1. Analyzing Blueprint Schema & Mapping Tables")
    client = BigQueryClient(project_id=project_id)
    schema = analyze_plant_schema(client, reference_plant)
    st.session_state.schema = schema

    st.write(f"Found {len(schema.tables)} tables, {len(schema.views)} views, and {len(schema.materialized_views)} materialized views in blueprint `{reference_plant}`.")

    table_mapper = TableMapperAgent()
    st.session_state.table_mapping = table_mapper.map_tables(schema.tables, reference_plant, new_plant)
    print(f"[DEBUG] Table Mapping: {st.session_state.table_mapping}")

    new_schema_objects_temp = {}
    for table in schema.tables:
        new_name = st.session_state.table_mapping.get(table.name)
        print(f"[DEBUG] Original Table Name: {table.name}, New Name: {new_name}")
        if new_name:
            new_schema_objects_temp[new_name] = Table(
                name=new_name,
                project=project_id,
                dataset=new_plant,
                columns=table.columns
            )
    st.session_state.new_schema_objects = new_schema_objects_temp
    st.success("Blueprint analysis and table mapping complete. Now, you can generate the views.")

# --- View Generation Section ---
if st.session_state.schema and st.session_state.new_schema_objects and (st.session_state.schema.views or st.session_state.schema.materialized_views):
    st.subheader("2. View Generation from Source")
    st.write(f"The agent will use the views from `{reference_plant}` as a blueprint. It will then query the central source dataset (`{source_dataset}`) to build new views for `{new_plant}`. Provide any custom instructions below.")

    original_views_to_translate = st.session_state.schema.views + st.session_state.schema.materialized_views
    
    current_view_instructions = st.session_state.view_instructions

    for view_obj in original_views_to_translate:
        unique_key = f"instructions_{view_obj.name}"
        current_view_instructions[view_obj.name] = st.text_area(
            f"Custom Instructions for {view_obj.schema_type}: {view_obj.name}",
            value=current_view_instructions.get(view_obj.name, ""),
            key=unique_key
        )
    st.session_state.view_instructions = current_view_instructions

    if st.button("Generate Views from Source"):
        view_mapper = ViewMapperAgent(project_id=project_id)
        updated_new_schema_objects = st.session_state.new_schema_objects.copy()

        for view_obj in original_views_to_translate:
            custom_instr = st.session_state.view_instructions.get(view_obj.name, "")
            with st.spinner(f"Generating {view_obj.schema_type.lower()} {view_obj.name} from source..."):
                generated_view = view_mapper.map_.view(view_obj, st.session_state.table_mapping, new_plant, custom_instr)
                if generated_view:
                    updated_new_schema_objects[generated_view.name] = generated_view
                else:
                    st.warning(f"Skipping {view_obj.schema_type.lower()} {view_obj.name} due to generation failure.")
        
        st.session_state.new_schema_objects = updated_new_schema_objects
        st.success("View generation complete!")

# --- UI Rendering (Conditional on new_schema_objects being populated) ---
if st.session_state.new_schema_objects:
    st.subheader("3. Selective Migration")
    object_names = list(st.session_state.new_schema_objects.keys())
    st.session_state.selected_objects = st.multiselect(
        "Choose objects to migrate:",
        options=object_names,
        default=object_names
    )

    filtered_objects = {name: obj for name, obj in st.session_state.new_schema_objects.items() if name in st.session_state.selected_objects}
    ordered_objects = resolve_creation_order(list(filtered_objects.values()), st.session_state.schema.dependencies)

    st.subheader("4. Execution Plan & DDL Preview")
    st.write("The following objects will be created in the specified order. Expand each section to see the DDL and AI feedback.")
    
    for i, obj in enumerate(ordered_objects):
        with st.expander(f"{i+1}. {obj.schema_type}: {obj.name}"):
            st.code(generate_ddl(obj), language="sql")
            if isinstance(obj, (View, MaterializedView)):
                if obj.changes_made:
                    st.subheader("Changes Made by AI")
                    for change in obj.changes_made:
                        st.write(f"- {change}")
                if obj.warnings:
                    st.subheader("Warnings from AI")
                    for warning in obj.warnings:
                        st.warning(warning)

    if st.button("Execute Onboarding"):
        if not dry_run:
            client = BigQueryClient(project_id=project_id)
            st.write(f"Creating dataset `{new_plant}` if it doesn't exist...")
            client.create_dataset_if_not_exists(new_plant)

            progress_bar = st.progress(0)
            for i, obj in enumerate(ordered_objects):
                ddl_statement = generate_ddl(obj)
                st.write(f"Creating {obj.schema_type}: {obj.name}")
                print(f"[DEBUG] Executing DDL for {obj.name}:\n{ddl_statement}")
                client.execute_ddl(ddl_statement)
                progress_bar.progress((i + 1) / len(ordered_objects))

            if load_data:
                st.subheader(f"Loading Data from `{source_dataset}`")
                tables_to_load = [obj for obj in ordered_objects if isinstance(obj, Table)]
                for i, table in enumerate(tables_to_load):
                    # Find the original source table name from the mapping
                    source_table_name = None
                    for k, v in st.session_state.table_mapping.items():
                        if v == table.name:
                            source_table_name = k
                            break
                    
                    if source_table_name:
                        print(f"[DEBUG] Target Table Name for Data Load: {table.name}, Source Table Name: {source_table_name}")
                        st.write(f"Loading data into `{table.name}` from `{source_dataset}.{source_table_name}`")
                        # THIS IS THE CRITICAL FIX: Use the central source_dataset from the config
                        client.execute_ddl(generate_data_load_sql(table, source_table_name, source_dataset))
                    else:
                        st.warning(f"Could not find source table name for {table.name}. Skipping data load for this table.")

            st.success("Onboarding complete!")
            st.balloons()
        else:
            st.info("Dry Run mode. No changes were made.")
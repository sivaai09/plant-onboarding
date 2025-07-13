import json
import os
import yaml
import streamlit as st
from typing import Dict, Union
from models.schema_objects import View, MaterializedView

import vertexai
from vertexai.preview.generative_models import GenerativeModel, GenerationConfig

class ViewMapperAgent:
    def __init__(self, project_id: str, location: str = "us-central1"):
        vertexai.init(project=project_id, location=location)
        self.model = GenerativeModel("gemini-1.5-pro-preview-0409")
        self.generation_config = GenerationConfig(
            temperature=0.1,
            top_p=0.95,
            top_k=32,
            max_output_tokens=8192,
        )
        self.config = self._load_config()

    def _load_config(self) -> Dict:
        """Loads the plant onboarding configuration from YAML."""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'plant_onboarding_config.yaml')
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            st.error("CRITICAL: `plant_onboarding_config.yaml` not found.")
            return {}
        except Exception as e:
            st.error(f"Error loading config: {e}")
            return {}

    def map_view(
        self, view: Union[View, MaterializedView], table_mapping: dict, target_plant: str, custom_instructions: str = ""
    ) -> Union[View, MaterializedView, None]:
        """Map a view or materialized view to a new plant with AI assistance."""

        prompt_text = self._build_prompt(view, table_mapping, target_plant, custom_instructions)
        if not prompt_text:
            return None

        try:
            response = self.model.generate_content(
                prompt_text,
                generation_config=self.generation_config,
            )
            
            cleaned_response = response.text.strip().replace("```json", "").replace("```", "")
            response_data = json.loads(cleaned_response)

            common_args = {
                "name": response_data.get("new_view_name"),
                "project": view.project,
                "dataset": target_plant,
                "sql": response_data.get("translated_sql"),
                "changes_made": response_data.get("changes_made", []),
                "warnings": response_data.get("warnings", []),
            }

            if isinstance(view, MaterializedView):
                return MaterializedView(
                    **common_args,
                    partition_column=view.partition_column,
                    cluster_columns=view.cluster_columns,
                    refresh_schedule=view.refresh_schedule,
                    auto_refresh=view.auto_refresh,
                )
            else:
                return View(**common_args)

        except Exception as e:
            st.error(f"Vertex AI call failed for view {view.name}: {e}")
            st.text_area("Failed Prompt", prompt_text, height=300)
            return None

    def _build_prompt(self, view: Union[View, MaterializedView], table_mapping: dict, target_plant: str, custom_instructions: str) -> str:
        view_type = "Materialized View" if isinstance(view, MaterializedView) else "View"
        
        reference_plant = view.dataset
        target_plant_config = self.config.get('plants', {}).get(target_plant)
        reference_plant_config = self.config.get('plants', {}).get(reference_plant)

        if not target_plant_config or not reference_plant_config:
            st.error(f"Config for reference plant '{reference_plant}' or target plant '{target_plant}' not in config file.")
            return ""

        discriminator_column = self.config.get('discriminator_column', 'unknown_discriminator')
        source_dataset = self.config.get('source_dataset', 'unknown_source_dataset')
        ref_discriminator_val = reference_plant_config.get('discriminator_value', 'unknown_ref_value')
        target_discriminator_val = target_plant_config.get('discriminator_value', 'unknown_target_value')

        return f'''
        You are an expert BigQuery data architect. Your task is to create the SQL for a new plant-specific view by modeling it after an existing one, following a strict three-tiered data architecture.

        **1. The Goal:**
        Create a new `{view_type}` in the `{target_plant}` dataset.

        **2. The Blueprint (Reference View):**
        - **Name:** `{view.name}`
        - **From Dataset:** `{reference_plant}`
        - **Logic:** This view provides the business logic. It was filtered for its plant using a clause like `WHERE {discriminator_column} = '{ref_discriminator_val}'`.
        - **Original SQL:**
          ```sql
          {view.sql}
          ```

        **3. The Raw Materials (Source Dataset):**
        - **Dataset Name:** `{source_dataset}`
        - You MUST rewrite the query to use tables from this central dataset. For example, `FROM orders` becomes `FROM `{source_dataset}.orders``.

        **4. The Target (Your Output):**
        - **Target Dataset:** `{target_plant}`
        - The new view must be filtered for the target plant. The new filtering condition is `WHERE {discriminator_column} = '{target_discriminator_val}'`.

        **Your Instructions:**
        1.  **Analyze:** Understand the business logic of the reference SQL.
        2.  **Rewrite Table References:** Change all `FROM` and `JOIN` clauses to point to tables within the `{source_dataset}` dataset.
        3.  **Replace Plant Filter:** Find the `WHERE` clause that filters for the reference plant (`{ref_discriminator_val}`) and replace it with the filter for the target plant (`{target_discriminator_val}`).
        4.  **Generate New Name:** Create a suitable new name for the view in the target dataset.
        5.  **Apply Custom Instructions:** {custom_instructions if custom_instructions else 'None'}

        **Provide the response as a single, valid JSON object with these keys:**
        - `new_view_name`: (string) A new name for the view in the `{target_plant}` dataset.
        - `translated_sql`: (string) The complete, new, and valid SQL query.
        - `changes_made`: (list of strings) Brief summary of the key changes.
        - `warnings`: (list of strings) Potential issues or warnings.
        '''

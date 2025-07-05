import json
from typing import Dict
from models.schema_objects import View
from utils.naming_utils import generate_new_name

import vertexai
from vertexai.preview.generative_models import GenerativeModel, GenerationConfig

class ViewMapperAgent:
    def __init__(self, project_id: str, location: str = "us-central1"):
        vertexai.init(project=project_id, location=location)
        self.model = GenerativeModel("gemini-pro") 
        self.generation_config = GenerationConfig(
            temperature=0.1,
            top_p=0.95,
            top_k=32,
            max_output_tokens=8192,
        )

    def map_view(self, view: View, table_mapping: dict, target_plant: str) -> View:
        """Map view to new plant with AI assistance"""

        prompt_text = f"""
        You are an expert BigQuery SQL translator. Your task is to translate a BigQuery view definition
        from a source manufacturing plant's schema to a new target manufacturing plant's schema.

        Original View Details:
        - View Name: {view.name}
        - View SQL:
        ```sql
        {view.sql}
        ```
        - Source Plant Dataset: {view.dataset}

        Target Plant Details:
        - Target Plant Dataset: {target_plant}

        Table Mappings (Old Name -> New Name):
        ```json
        {json.dumps(table_mapping, indent=2)}
        ```

        Your translation must adhere to the following strict requirements:
        1.  **Update View Name**: Generate a new, appropriate view name for the target plant. The new name should follow the convention of replacing the source plant identifier with the target plant identifier (e.g., 'plant1_daily_summary' -> 'plant2_daily_summary'). If the original name doesn't contain a plant identifier, infer a suitable new name.
        2.  **Translate Table References**: Replace ALL occurrences of old table names (including fully qualified names like `project.dataset.table` or `dataset.table`) with their corresponding new table names based on the provided `Table Mappings`. Ensure the new table names are correctly referenced within the SQL.
        3.  **Adapt Plant-Specific Logic**: Analyze the SQL for any hardcoded plant-specific values (e.g., `WHERE location = 'MAIN_WAREHOUSE'`, `SELECT 'plant1' as plant_code`). Adapt these values to be appropriate for the `target_plant`. For example, if `plant1` is hardcoded, replace it with `target_plant`.
        4.  **Preserve Business Logic**: The core business logic, aggregations, joins, CTEs, and subqueries must remain functionally identical, only adapted to the new plant's naming conventions and specific values.
        5.  **Add Plant Identifier Column (if appropriate)**: If the original view implies a single plant context (e.g., by having a `plant_code` column or by its name), consider adding a new column `'{target_plant}' as plant_code` to the outermost SELECT statement, if it makes sense for the view's purpose and doesn't break existing logic.
        6.  **Output Format**: Provide the response in a JSON object with the following keys:
            -   `new_view_name`: The newly generated view name (string).
            -   `translated_sql`: The complete, translated BigQuery SQL for the new view (string).
            -   `changes_made`: A list of significant changes applied during translation (list of strings).
            -   `warnings`: Any concerns or assumptions made during translation (list of strings).

        Example of `translated_sql` format:
        ```sql
        CREATE VIEW `your_project.target_plant.new_view_name` AS
        SELECT
            -- ... translated columns and logic ...
        FROM
            `your_project.target_plant.new_table_name`
        WHERE
            -- ... adapted plant-specific conditions ...
        ```
        Ensure the `translated_sql` is a complete `CREATE VIEW` statement, including the `CREATE VIEW` clause and the fully qualified new view name.
        """

        try:
            response = self.model.generate_content(
                prompt_text,
                generation_config=self.generation_config,
            )
            return json.loads(response.text)
        except Exception as e:
            print(f"[ERROR] Vertex AI call failed for view {view.name}: {e}")
            print(f"[ERROR] Returning original view with name mapped: {view.name} -> {generate_new_name(view.name, view.dataset, target_plant)}")
            return View(
                name=generate_new_name(view.name, view.dataset, target_plant),
                project=view.project,
                dataset=target_plant,
                sql=view.sql
            )
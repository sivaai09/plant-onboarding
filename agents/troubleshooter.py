import json
from typing import Dict, Any

import vertexai
from vertexai.preview.generative_models import GenerativeModel, GenerationConfig

class TroubleshootingAgent:
    def __init__(self, project_id: str, location: str = "us-central1"):
        vertexai.init(project=project_id, location=location)
        self.model = GenerativeModel("gemini-pro") # Using gemini-pro for general availability
        self.generation_config = GenerationConfig(
            temperature=0.1,
            top_p=0.95,
            top_k=32,
            max_output_tokens=1024,
        )

    def diagnose(self, error_message: str, context: Dict = None) -> Dict[str, Any]:
        """Diagnoses an error message and provides troubleshooting steps and a proposed fix."""
        if context is None:
            context = {}

        prompt_text = f"""
        You are an AI assistant specialized in troubleshooting BigQuery and Google Cloud issues.
        A user encountered an error during a BigQuery schema onboarding process.

        Error Message:
        {error_message}

        Additional Context:
        {json.dumps(context, indent=2)}

        Please analyze the error message and provide a concise diagnosis, possible causes, and actionable next steps for the user.
        If you can identify a specific code change that would fix the issue, propose it.

        Provide your response in a JSON object with the following keys:
        - `problem`: (string) Concise summary of the problem.
        - `causes`: (list of strings) Possible causes.
        - `next_steps`: (list of strings) Actionable steps for the user.
        - `proposed_fix`: (object, optional) A dictionary containing:
            - `file_path`: (string) The path to the file that needs modification.
            - `conceptual_change`: (string) A natural language description of the change.
            - `example_old_code`: (string, optional) An example snippet of the code to be replaced.
            - `example_new_code`: (string, optional) An example snippet of the replacement code.

        Example JSON output:
        ```json
        {{
            "problem": "BigQuery Dataset Not Found",
            "causes": [
                "The specified BigQuery dataset does not exist.",
                "Incorrect dataset ID in configuration."
            ],
            "next_steps": [
                "Verify the dataset ID in your .env file or command line arguments.",
                "Create the dataset in BigQuery if it does not exist."
            ],
            "proposed_fix": {{
                "file_path": "plant_onboarding/core/bigquery_client.py",
                "conceptual_change": "Ensure the target dataset is created before attempting to create tables/views.",
                "example_old_code": "# client.create_dataset_if_not_exists(new_plant)",
                "example_new_code": "client.create_dataset_if_not_exists(new_plant)"
            }}
        }}
        ```
        """

        try:
            response = self.model.generate_content(
                prompt_text,
                generation_config=self.generation_config,
            )
            # Assuming the model returns a JSON string in its text attribute
            return json.loads(response.text)
        except Exception as e:
            return {
                "problem": "AI Diagnosis Failed",
                "causes": [f"Failed to get diagnosis from AI: {e}"],
                "next_steps": ["Check your Vertex AI setup and network connection."],
                "proposed_fix": None
            }
import os
from dotenv import load_dotenv

def get_gcp_project_id() -> str:
    """Gets the GCP project ID from the .env file or environment variables."""
    load_dotenv()
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id or project_id == "YOUR_GCP_PROJECT_ID":
        raise ValueError(
            "GCP project ID not found or is a placeholder. "
            "Please set it in the plant_onboarding/.env file."
        )
    return project_id
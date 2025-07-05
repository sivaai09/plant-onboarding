# BigQuery Plant Schema & Views Onboarding System

This project automates the process of onboarding new manufacturing plants by intelligently replicating and adapting existing BigQuery schemas, including both tables and views. It uses an agent-based system to analyze, translate, and generate the necessary DDL for creating a new plant's data infrastructure.

## Key Features

*   **Comprehensive Schema Analysis**: Extracts tables, views, and materialized views from an existing plant's BigQuery dataset. Now includes more robust SQL parsing for accurate dependency mapping.
*   **Intelligent SQL Translation (Powered by Vertex AI Gemini 1.5 Pro)**: Utilizes Google's Gemini 1.5 Pro model to intelligently translate view SQL. This includes adapting table names, view names, and plant-specific logic, ensuring business logic is preserved while adapting to the new plant's context.
*   **Dependency Management**: Automatically resolves the correct creation order for tables and views by building and analyzing a dependency graph.
*   **DDL Generation**: Creates `CREATE TABLE`, `CREATE VIEW`, and `CREATE MATERIALIZED VIEW` statements for the new plant, including support for materialized view specific properties like partitioning, clustering, and refresh schedules.
*   **Schema Validation**: Includes checks for missing table references and can be extended for SQL syntax validation, ensuring the generated schema is valid before deployment.
*   **Enhanced Interactive UI (Streamlit)**: Provides a comprehensive web interface with:
    *   **Schema Tree View**: Hierarchical display of tables, views, and materialized views.
    *   **Interactive Dependency Visualization**: A graph showing relationships between schema objects.
    *   **Side-by-side SQL Preview**: Review original and translated SQL DDL statements.
    *   **Progress Tracking**: Real-time updates during the onboarding process.
*   **Command-Line Interface**: A CLI is available for scripting and automation.
*   **Troubleshooting Agent with Proposed Fixes**: A dedicated agent that can diagnose error messages, provide actionable advice, and **propose specific code changes** for common issues. The user can then review and approve these proposed fixes before they are applied.
*   **Validation & Dry-Run**: Includes capabilities for validating the generated schema and previewing all changes before execution.

## Project Structure

```
plant_onboarding/
├── main.py                 # CLI interface (using Click)
├── app.py                  # Streamlit web interface
├── config.py               # GCP project and plant settings
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables (e.g., GCP_PROJECT_ID)
├── core/                   # Core logic for schema analysis, SQL translation, etc.
│   ├── schema_analyzer.py  # Improved SQL parsing for dependencies
│   ├── dependency_resolver.py
│   ├── sql_translator.py
│   └── bigquery_client.py  # Now fetches real Materialized Views
├── agents/                 # AI-powered agents for mapping and generation
│   ├── table_mapper.py
│   ├── view_mapper.py      # Now uses Vertex AI Gemini 1.5 Pro
│   ├── ddl_generator.py    # Generates DDL for Materialized Views
│   ├── schema_validator.py # Includes schema validation logic
│   └── troubleshooter.py   # New troubleshooting agent with fix proposals
├── models/                 # Data models for schema objects and configurations
│   ├── schema_objects.py
│   └── plant_config.py
└── utils/                  # Utility functions for SQL parsing and naming
    ├── sql_parser.py
    └── naming_utils.py
```

## Setup and Installation

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **GCP Authentication & Project ID**:
    This project requires access to Google Cloud Platform and Vertex AI. Ensure your environment is authenticated and your project ID is configured:

    *   **Authentication**: Run `gcloud auth application-default login` in your terminal.
    *   **Project ID**: Create a `.env` file in the `plant_onboarding/` directory (if it doesn't exist) and add your GCP Project ID:
        ```
        # plant_onboarding/.env
        GCP_PROJECT_ID="your-gcp-project-id"
        ```
        Replace `your-gcp-project-id` with your actual GCP Project ID.

    *   **Enable APIs**: Ensure the **BigQuery API** and **Vertex AI API** are enabled in your GCP project.

## Usage

### Streamlit Web Interface

The most user-friendly way to run the system is through the Streamlit app.

```bash
streamlit run plant_onboarding/app.py
```

This will launch a web application where you can:
- Specify the source and target plant names.
- Choose whether to include views.
- Perform a dry run to see the generated DDL and execution plan.
- Visualize dependencies between tables and views.

### Command-Line Interface (CLI)

The CLI provides a powerful way to script and automate the onboarding process.

**Full Migration (Tables + Views):**
```bash
python plant_onboarding/main.py onboard --new-plant plant2 --reference-plant plant1 --include-views
```

**Dry Run (Preview Changes):**
```bash
python plant_onboarding/main.py onboard --new-plant plant2 --reference-plant plant1 --include-views --dry-run
```

**Selective Migration (Specific Tables/Views):**
```bash
python plant_onboarding/main.py onboard --new-plant plant2 --reference-plant plant1 --only "orders,inventory,daily_summary"
```

**Troubleshooting an Error:**
```bash
python plant_onboarding/main.py troubleshoot --error-message "Your error message here"
```

**Get Help:**
```bash
python plant_onboarding/main.py --help
python plant_onboarding/main.py onboard --help
python plant_onboarding/main.py troubleshoot --help
```
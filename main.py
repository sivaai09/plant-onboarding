import click
from config import get_gcp_project_id
from core.bigquery_client import BigQueryClient
from core.schema_analyzer import analyze_plant_schema
from core.dependency_resolver import resolve_creation_order
from core.sql_translator import translate_view_sql
from agents.table_mapper import TableMapperAgent
from agents.view_mapper import ViewMapperAgent
from agents.ddl_generator import generate_ddl
from agents.schema_validator import SchemaValidatorAgent
from agents.troubleshooter import TroubleshootingAgent
from models.schema_objects import Table, View
from utils.naming_utils import generate_new_name

@click.group()
def cli():
    """A CLI for onboarding new BigQuery plant schemas."""
    pass

@cli.command()
@click.option('--new-plant', required=True, help='The dataset name of the new plant.')
@click.option('--reference-plant', required=True, help='The dataset name of the reference plant.')
@click.option('--gcp-project', help='GCP project ID (overrides .env file).')
@click.option('--include-views', is_flag=True, default=False, help='Include views in the migration.')
@click.option('--dry-run', is_flag=True, default=False, help='Preview the changes without executing them.')
@click.option('--only', help='A comma-separated list of specific tables/views to migrate.')
def onboard(new_plant, reference_plant, gcp_project, include_views, dry_run, only):
    """Onboard a new plant by replicating and adapting an existing plant schema."""
    try:
        project_id = gcp_project or get_gcp_project_id()
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        return

    click.echo(f"Starting onboarding for new plant '{new_plant}' from reference '{reference_plant}'.")
    click.echo(f"Project: {project_id}, Dry Run: {dry_run}\n")

    client = BigQueryClient(project_id=project_id)

    if not dry_run:
        client.create_dataset_if_not_exists(new_plant)

    schema = analyze_plant_schema(client, reference_plant)

    table_mapper = TableMapperAgent()
    table_mapping = table_mapper.map_tables(schema.tables, reference_plant, new_plant)

    view_mapper = ViewMapperAgent(project_id=project_id)
    new_schema_objects = []

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
            translated_view = view_mapper.map_view(view, table_mapping, new_plant)
            new_schema_objects.append(translated_view)

    if only:
        only_list = [generate_new_name(o, reference_plant, new_plant) for o in only.split(',')]
        new_schema_objects = [obj for obj in new_schema_objects if obj.name in only_list]

    translated_dependencies = {}
    all_mappings = {**table_mapping}
    if include_views:
        for view in schema.views:
            all_mappings[view.name] = generate_new_name(view.name, reference_plant, new_plant)
        for mv in schema.materialized_views:
            all_mappings[mv.name] = generate_new_name(mv.name, reference_plant, new_plant)

    ordered_objects = resolve_creation_order(new_schema_objects, translated_dependencies)

    click.echo(f"Found {len(ordered_objects)} objects to create in the correct order.")

    validator = SchemaValidatorAgent(client=client)
    if not validator.validate_schema(new_schema_objects):
        click.echo("Validation failed. Aborting onboarding.", err=True)
        return

    for obj in ordered_objects:
        ddl = generate_ddl(obj)
        click.echo(f"--- Processing: {obj.name} ({obj.schema_type}) ---")
        client.execute_ddl(ddl, dry_run=dry_run)
        click.echo("-------------------------------------\n")

    click.echo("Onboarding process complete!")

@cli.command()
@click.option('--error-message', required=True, help='The error message to diagnose.')
@click.option('--gcp-project', help='GCP project ID (overrides .env file).')
def troubleshoot(error_message, gcp_project):
    """Diagnose an error message and get troubleshooting advice."""
    try:
        project_id = gcp_project or get_gcp_project_id()
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        return

    troubleshooter = TroubleshootingAgent(project_id=project_id)
    diagnosis = troubleshooter.diagnose(error_message)

    click.echo(f"\nProblem: {diagnosis.get('problem', 'N/A')}")
    click.echo("\nPossible Causes:")
    for cause in diagnosis.get('causes', []):
        click.echo(f"- {cause}")

    click.echo("\nNext Steps:")
    for step in diagnosis.get('next_steps', []):
        click.echo(f"1. {step}")

    proposed_fix = diagnosis.get('proposed_fix')
    if proposed_fix:
        click.echo("\n--- Proposed Fix ---")
        click.echo(f"File: {proposed_fix.get('file_path')}")
        click.echo(f"Change: {proposed_fix.get('conceptual_change')}")
        if proposed_fix.get('example_old_code'):
            click.echo("\nExample Old Code:")
            click.echo(f"'''\n{proposed_fix['example_old_code']}\n'''")
        if proposed_fix.get('example_new_code'):
            click.echo("\nExample New Code:")
            click.echo(f"'''\n{proposed_fix['example_new_code']}\n'''")

        click.echo("\nWARNING: The `replace` tool requires exact matches for `old_string` and `new_string`.")
        click.echo("You will need to manually copy the exact code from the file and paste it into the prompt.")
        click.echo("This AI-generated example might not be an exact match.")

        if click.confirm("\nDo you want to attempt to apply this fix? (Requires manual input of exact code)"):
            file_path = proposed_fix.get('file_path')
            if not file_path:
                click.echo("Error: Proposed fix missing file_path.", err=True)
                return

            old_string = click.prompt("Enter the EXACT 'old_string' from the file (copy-paste, including whitespace)")
            new_string = click.prompt("Enter the EXACT 'new_string' to replace it with (copy-paste, including whitespace)")

            try:
                # This is where the actual tool call would happen
                # For now, we'll just print the intended action
                click.echo(f"Attempting to replace in {file_path}...")
                click.echo(f"Old String: \n'''\n{old_string}\n'''")
                click.echo(f"New String: \n'''\n{new_string}\n'''")
                # default_api.replace(file_path=file_path, old_string=old_string, new_string=new_string)
                click.echo("Fix applied (simulated). Please verify the file manually.")
            except Exception as e:
                click.echo(f"Failed to apply fix: {e}", err=True)
    else:
        click.echo("\nNo specific code fix proposed by the AI for this error.")

if __name__ == '__main__':
    cli()

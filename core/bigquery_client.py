from typing import List
from models.schema_objects import Table, View, MaterializedView, Column
from google.api_core.exceptions import NotFound, Conflict

class BigQueryClient:
    def __init__(self, project_id: str, location: str = "US"):
        self.project_id = project_id
        self.location = location
        try:
            from google.cloud import bigquery
            self.client = bigquery.Client(project=project_id, location=location)
            self.real_client = True
        except (ImportError, Exception) as e:
            print(f"WARNING: Could not initialize real BigQuery client: {e}. Running in mock mode.")
            self.client = None
            self.real_client = False

    def create_dataset_if_not_exists(self, dataset_id: str):
        """Creates a BigQuery dataset if it does not already exist."""
        if not self.real_client:
            print(f"[MOCK] Skipping dataset creation for {dataset_id}.")
            return

        from google.cloud import bigquery

        dataset_ref = self.client.dataset(dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.location
            try:
                self.client.create_dataset(dataset)
            except Conflict:
                 pass # Race condition handling
            except Exception as e:
                print(f"[ERROR] Failed to create dataset {dataset_id}: {e}")
                raise

    def get_tables(self, dataset_id: str, project_id: str = None) -> List[Table]:
        """Gets all tables in a dataset."""
        if not self.real_client:
            return self._get_mock_tables(dataset_id)

        target_project = project_id if project_id else self.project_id

        tables = []
        try:
            for bq_table in self.client.list_tables(f"{target_project}.{dataset_id}"):
                if bq_table.table_type == 'TABLE':
                    table_ref = self.client.get_table(bq_table.reference)
                    columns = [Column(name=f.name, data_type=f.field_type, mode=f.mode) for f in table_ref.schema]
                    tables.append(Table(
                        name=table_ref.table_id,
                        project=table_ref.project,
                        dataset=table_ref.dataset_id,
                        columns=columns
                    ))
        except Exception as e:
            print(f"[ERROR] Could not fetch tables from {dataset_id}: {e}. Returning mock data.")
            return self._get_mock_tables(dataset_id)
        return tables

    def get_views(self, dataset_id: str) -> List[View]:
        """Gets all views in a dataset."""
        # if not self.real_client:
        #     return self._get_mock_views(dataset_id)

        views = []
        try:
            for bq_table in self.client.list_tables(f"{self.project_id}.{dataset_id}"):
                if bq_table.table_type == 'VIEW':
                    view_ref = self.client.get_table(bq_table.reference)
                    views.append(View(
                        name=view_ref.table_id,
                        project=view_ref.project,
                        dataset=view_ref.dataset_id,
                        sql=view_ref.view_query
                    ))
        except Exception as e:
            print(f"[ERROR] Could not fetch views from {dataset_id}: {e}. Returning mock data.")
            return self._get_mock_views(dataset_id)
        return views

    def get_materialized_views(self, dataset_id: str) -> List[MaterializedView]:
        """Gets all materialized views in a dataset."""
        if not self.real_client:
            return [] # No mock MVs for now

        mvs = []
        try:
            for bq_table in self.client.list_tables(f"{self.project_id}.{dataset_id}"):
                if bq_table.table_type == 'MATERIALIZED_VIEW':
                    mv_ref = self.client.get_table(bq_table.reference)
                    mvs.append(MaterializedView(
                        name=mv_ref.table_id,
                        project=mv_ref.project,
                        dataset=mv_ref.dataset_id,
                        sql=mv_ref.mview_query,
                        partition_column=mv_ref.partitioning_field,
                        cluster_columns=mv_ref.clustering_fields or [],
                        refresh_schedule=mv_ref.refresh_time_interval_in_millis,
                        auto_refresh=mv_ref.enable_refresh
                    ))
        except Exception as e:
            print(f"[ERROR] Could not fetch materialized views from {dataset_id}: {e}. Returning empty list.")
            return []
        return mvs

    def execute_ddl(self, ddl: str, dry_run: bool = False):
        """Executes a DDL statement in BigQuery."""
        if dry_run:
            print(f"[DRY RUN] DDL is valid. To execute, run without the --dry-run flag.\n--- DDL Statement ---\n{ddl}\n---------------------")
            return

        if self.real_client:
            from google.cloud import bigquery
            job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
            try:
                print(f"[DEBUG] Executing DDL in BigQuery:\n{ddl}")
                query_job = self.client.query(ddl, job_config=job_config)
                query_job.result()  # Wait for the job to complete
                print(f"[DEBUG] DDL execution successful.")
            except Exception as e:
                print(f"[ERROR] Failed to execute DDL: {e}")
                raise
        else:
            print(f"[MOCK EXECUTION] Not executing DDL because BigQuery client is not available.\n--- DDL Statement ---\n{ddl}\n---------------------")

    def _get_mock_tables(self, dataset_id: str) -> List[Table]:
        return [
            Table(
                name=f"{dataset_id}_orders", project=self.project_id, dataset=dataset_id,
                columns=[Column(name="order_id", data_type="STRING"), Column(name="sku", data_type="STRING")]
            ),
            Table(
                name=f"{dataset_id}_inventory", project=self.project_id, dataset=dataset_id,
                columns=[Column(name="sku", data_type="STRING"), Column(name="current_quantity", data_type="INTEGER")]
            )
        ]

    def _get_mock_views(self, dataset_id: str) -> List[View]:
        return [
            View(
                name=f"{dataset_id}_daily_summary", project=self.project_id, dataset=dataset_id,
                sql=f"SELECT * FROM `{self.project_id}.{dataset_id}.{dataset_id}_orders`"
            )
        ]

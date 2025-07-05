from models.schema_objects import Table, View, MaterializedView

def generate_ddl(schema_object) -> str:
    """Generates the DDL for a schema object."""
    if isinstance(schema_object, Table):
        return generate_table_ddl(schema_object)
    elif isinstance(schema_object, MaterializedView):
        return generate_materialized_view_ddl(schema_object)
    elif isinstance(schema_object, View):
        return generate_view_ddl(schema_object)
    else:
        raise TypeError(f"Unsupported schema object type: {type(schema_object)}")

def generate_table_ddl(table: Table) -> str:
    columns = ",\n  ".join([f"{c.name} {c.data_type}" for c in table.columns])
    return f"CREATE TABLE `{table.project}.{table.dataset}.{table.name}` (\n  {columns}\n);"

def generate_view_ddl(view: View) -> str:
    return f"CREATE VIEW `{view.project}.{view.dataset}.{view.name}` AS\n{view.sql};"

def generate_materialized_view_ddl(mv: MaterializedView) -> str:
    options = []
    if mv.refresh_schedule:
        # BigQuery expects refresh_interval_minutes in milliseconds
        # Assuming mv.refresh_schedule is in milliseconds, or needs conversion
        options.append(f"refresh_interval_minutes={int(mv.refresh_schedule / 60000)}")
    if mv.auto_refresh is not None:
        options.append(f"enable_refresh={str(mv.auto_refresh).lower()}")

    options_str = f"OPTIONS({', '.join(options)})" if options else ""

    partition_by_str = f"PARTITION BY {mv.partition_column}" if mv.partition_column else ""
    cluster_by_str = f"CLUSTER BY {', '.join(mv.cluster_columns)}" if mv.cluster_columns else ""

    return f"""CREATE MATERIALIZED VIEW `{mv.project}.{mv.dataset}.{mv.name}`
{partition_by_str}
{cluster_by_str}
{options_str}
AS
{mv.sql};"""
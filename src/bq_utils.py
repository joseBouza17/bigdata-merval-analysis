from __future__ import annotations

from typing import Optional
import warnings

import pandas as pd
from google.api_core.exceptions import Forbidden, PermissionDenied
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src.config import BIGQUERY_LOCATION, PROJECT_ID


def get_bigquery_client(project_id: str = PROJECT_ID) -> bigquery.Client:
    """Create the main BigQuery client used across the notebooks."""
    return bigquery.Client(project=project_id, location=BIGQUERY_LOCATION)


def ensure_dataset(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    location: str = BIGQUERY_LOCATION,
) -> None:
    """Create the dataset if it does not already exist."""
    dataset_id = f"{project_id}.{dataset_name}"
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        client.create_dataset(dataset)


def table_exists(client: bigquery.Client, table_id: str) -> bool:
    """Return True when the target table already exists in BigQuery."""
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def query_to_dataframe(client: bigquery.Client, query: str) -> pd.DataFrame:
    """Read a query result into pandas, preferring the Storage API when available."""
    row_iterator = client.query(query).result()

    try:
        from google.cloud import bigquery_storage_v1
    except ImportError:
        # This quiet fallback avoids the default notebook warning about missing Storage API extras.
        return row_iterator.to_dataframe(create_bqstorage_client=False)

    # Use the faster Storage API when the dependency is installed.
    try:
        storage_client = bigquery_storage_v1.BigQueryReadClient()
        return row_iterator.to_dataframe(
            bqstorage_client=storage_client,
            create_bqstorage_client=False,
        )
    except (Forbidden, PermissionDenied):
        warnings.warn(
            "BigQuery Storage API access is unavailable for this environment. "
            "Falling back to the REST path.",
            stacklevel=2,
        )
        return row_iterator.to_dataframe(create_bqstorage_client=False)


def delete_rows_for_run_id(client: bigquery.Client, table_id: str, run_id: str) -> None:
    """Remove rows from a previous run so reruns stay tidy."""
    delete_sql = f"delete from `{table_id}` where run_id = @run_id"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
    )
    client.query(delete_sql, job_config=job_config).result()


def upload_dataframe(
    client: bigquery.Client,
    dataframe: pd.DataFrame,
    project_id: str,
    dataset_name: str,
    table_name: str,
    write_disposition: str = "WRITE_TRUNCATE",
    run_id: Optional[str] = None,
) -> None:
    """Upload a dataframe and deduplicate reruns when the table is append-based."""
    if dataframe.empty:
        raise ValueError(f"Refusing to upload empty dataframe to {dataset_name}.{table_name}.")

    table_id = f"{project_id}.{dataset_name}.{table_name}"

    if (
        write_disposition == "WRITE_APPEND"
        and run_id
        and "run_id" in dataframe.columns
        and table_exists(client, table_id)
    ):
        try:
            delete_rows_for_run_id(client, table_id, run_id)
        except Forbidden:
            warnings.warn(
                "Could not delete prior rows for the same run_id. "
                "Appending this run without warehouse-side cleanup.",
                stacklevel=2,
            )

    load_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    if write_disposition == "WRITE_APPEND":
        load_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        ]
    client.load_table_from_dataframe(dataframe, table_id, job_config=load_config).result()

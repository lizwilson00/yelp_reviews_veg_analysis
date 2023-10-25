from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="yelp_ds.reviews_all_prefect",
        project_id="vivid-lane-398705",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    for file_num in range(4):
        df = pd.read_parquet(f"data/reviews/reviews_{file_num}.parquet")
        write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()

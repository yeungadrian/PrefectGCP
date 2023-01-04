import os

import pandas as pd
import requests
from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

config = {
    "url": (
        "https://seekingalpha.com/api/v3/historical_prices?filter[ticker][slug]=sp500tr&=&filter[as_of_date][gte]=Wed%20Oct%2026%201999&"
        "filter[as_of_date][lte]=Tue%20Nov%2029%202099&sort=as_of_date"
    ),
    "local_path": "./data/benchmark",
    "destination_path": "01_primary/",
    "benchmark_path": "./data/benchmark/sp500.parquet",
    "bucket": "aurora-361016-market-data",
}

headers = {
    "Host": "seekingalpha.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:108.0) Gecko/20100101 Firefox/108.0",
    "Accept": "*/*",
    "Accept-Language": "en-GB,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://seekingalpha.com/symbol/SP500TR/historical-price-quotes",
}


@task(retries=2, retry_delay_seconds=60)
def get_benchmark_index() -> pd.DataFrame:
    """
    Download benchmark index from data provider

    Args:
        code (str): Code of fund

    Returns:
        pd.DataFrame: Historical index of benchmark
    """
    data = requests.get(config["url"], headers=headers)
    df = pd.DataFrame(
        pd.DataFrame(data.json()["data"])["attributes"].values.tolist()
    )
    df = df.rename(columns={"as_of_date": "date"})
    return df


@task(retries=2, retry_delay_seconds=60)
def write_files(df: pd.DataFrame) -> None:
    """
    Write files to disk

    Args:
        df (pd.DataFrame): df containing benchmark index

    Returns:
        None:
    """
    logger = get_run_logger()
    logger.info("Writing files to disk")
    if not os.path.exists(config["local_path"]):
        os.makedirs(config["local_path"])
    df.to_parquet(config["benchmark_path"])
    return None


@flow(name="Load US benchmark to GCS", task_runner=ConcurrentTaskRunner())
def download_us_benchmark():
    """
    Prefect flow that:
    1. Download TR SPX
    2. Clean data
    3. Write dataframe to csv
    4. Upload to GCS
    """
    df = get_benchmark_index()
    write_files(df)
    gcs_bucket = GcsBucket(
        bucket=config["bucket"],
        gcp_credentials=GcpCredentials.load("aurora-sa"),
    )
    gcs_bucket.put_directory(config["local_path"], config["destination_path"])

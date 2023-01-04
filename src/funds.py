import os
import random
import time
from typing import List

import pandas as pd
import requests
from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

config = {
    "indices": {
        "F00000UEXJ": {
            "iShares UK Equity Index Fund": "GB00BPFJDB84",
        },
        "F00000OJMA": {
            "iShares US Equity Index Fund": "GB00B5VRGY09",
        },
        "F00000OOB2": {
            "HSBC European Index Fund": "GB00B80QGH28",
        },
        "F00000SSSQ": {
            "iShares Japan Equity Index Fund": "GB00BJL5BZ80",
        },
        "F00000SSSR": {
            "iShares Pacific ex Japan Equity Index Fund": "GB00BJL5C004",
        },
        "F00000UEXM": {
            "iShares Index Linked Gilt Index Fund": "GB00BPFJDG30",
        },
        "F00000UEXI": {
            "iShares Overseas Government Bond Index Fund": "GB00BPFJD859",
        },
    },
    "local_path": "./data/funds",
    "destination_path": "01_primary/",
    "fund_prices": "./data/funds/fund_prices.parquet",
    "fund_codes": "./data/funds/fund_codes.parquet",
    "bucket": "aurora-361016-market-data",
}


@task(retries=2, retry_delay_seconds=60)
def get_historical_indices(code: str) -> pd.DataFrame:
    """
    Download historical indices from data provider

    Args:
        code (str): Code of fund

    Returns:
        pd.DataFrame: Historical indices of fund
    """
    time.sleep(random.random() * 5)
    logger = get_run_logger()
    logger.info(f"Downloading data for {code}")
    url = (
        "https://lt.morningstar.com/api/rest.svc/timeseries_price/9vehuxllxs?"
        f"currencyId=GBP&endDate=2022-12-24&forwardFill=true&frequency=daily&id={code}"
        "&idType=Morningstar&outputType=json&startDate=1900-01-01"
    )
    df = requests.get(url)
    df = pd.DataFrame(df.json()["TimeSeries"]["Security"][0]["HistoryDetail"])
    df = df.rename(columns={"Value": code})
    df = df.drop(columns="OriginalDate")
    return df


@task(retries=2, retry_delay_seconds=60)
def merge_indices(data: List) -> pd.DataFrame:
    """
    Merge all indices into singular df

    Args:
        data (List): Individual historical indices of funds

    Returns:
        pd.DataFrame: Single df with all indices
    """
    logger = get_run_logger()
    logger.info("Aggregating indices")
    indices = data[0]
    for i, n in enumerate(data):
        if i > 0:
            indices = indices.merge(n, how="outer", on="EndDate")
    return indices


@task(retries=2, retry_delay_seconds=60)
def clean_indices(indices: pd.DataFrame) -> pd.DataFrame:
    """
    Clean indices to prepare for writing

    Args:
        indices (pd.DataFrame): Raw joined indices

    Returns:
        pd.DataFrame: Cleaned indices
    """
    indices = indices.sort_values(by="EndDate").reset_index(drop=True)
    indices = indices.rename(columns={"EndDate": "date"})
    indices[list(config["indices"].keys())] = indices[
        list(config["indices"].keys())
    ].astype(float)
    return indices


@task(retries=2, retry_delay_seconds=60)
def fund_mapping() -> pd.DataFrame:
    """
    Create fund mapping between code and name

    Returns:
        pd.DataFrame: Df containing code-name mapping
    """
    fund_codes = list(config["indices"].keys())
    fund_names = [
        list(config["indices"][i].keys())[0] for i in config["indices"].keys()
    ]
    df = pd.DataFrame({"Code": fund_codes, "Company": fund_names})
    return df


@task(retries=2, retry_delay_seconds=60)
def write_files(indices: pd.DataFrame, fund_df: pd.DataFrame) -> None:
    """
    Write files to disk

    Args:
        indices (pd.DataFrame): df containing indices
        fund_df (pd.DataFrame): df containing code-name mapping

    Returns:
        None:
    """
    logger = get_run_logger()
    logger.info("Writing files to disk")
    if not os.path.exists(config["local_path"]):
        os.makedirs(config["local_path"])
    indices.to_parquet(config["fund_prices"])
    fund_df.to_parquet(config["fund_codes"])
    return None


@flow(name="Load fund indices to GCS", task_runner=ConcurrentTaskRunner())
def daily_fund_indices():
    """
    Prefect flow that:
    1. Download indices from data vendor
    2. Merge and clean data
    3. Generate fund code - fund name mapping
    4. Write dataframe to parquet
    5. Upload to GCS
    """
    data = []
    for i in config["indices"].keys():
        df = get_historical_indices(i)
        data.append(df)
    indices = merge_indices(data)
    indices = clean_indices(indices)
    fund_df = fund_mapping()
    write_files(indices, fund_df)
    gcs_bucket = GcsBucket(
        bucket=config["bucket"],
        gcp_credentials=GcpCredentials.load("aurora-sa"),
    )
    gcs_bucket.put_directory(config["local_path"], config["destination_path"])

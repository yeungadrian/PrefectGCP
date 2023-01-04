import io
import os
import zipfile

import pandas as pd
import requests
from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

config = {
    "daily_url": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip",
    "monthly_url": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip",
    "local_path": "./data/ff_factors",
    "destination_path": "01_primary/",
    "daily_path": "./data/ff_factors/ff_daily.parquet",
    "monthly_path": "./data/ff_factors/ff_monthly.parquet",
    "bucket": "aurora-361016-market-data",
}


def get_csv_zip(url: str) -> bytes:
    """
    Get zipfile from URL

    Args:
        url (str): URL

    Returns:
        bytes: Bytes representation of zipfile
    """
    data = requests.get(url)
    return data.content


def read_zip(zip: bytes) -> pd.DataFrame:
    """
    Read zipfile contents into df

    Args:
        zip (bytes): Zipfile as bytes

    Returns:
        pd.DataFrame: Df containing raw ff data
    """
    zf = zipfile.ZipFile(io.BytesIO(zip))
    df = pd.read_csv(zf.open(zf.filelist[0].filename), skiprows=3)
    return df


@task(retries=2, retry_delay_seconds=60)
def get_ff_df(url: str) -> pd.DataFrame:
    """
    Get dataframe from url that returns zipfile

    Args:
        url (str): URL

    Returns:
        pd.DataFrame: French fama data
    """
    zip = get_csv_zip(url)
    df = read_zip(zip)
    return df


@task(retries=2, retry_delay_seconds=60)
def clean_daily_ff(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean daily ff df to prepare for saving

    Args:
        df (pd.DataFrame): Raw daily df

    Returns:
        pd.DataFrame: Cleaned daily df
    """
    df = df.rename(columns={"Unnamed: 0": "date"})
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d").dt.strftime(
        "%Y-%m-%d"
    )
    return df


@task(retries=2, retry_delay_seconds=60)
def clean_monthly_ff(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean monthly ff df to prepare for saving

    Args:
        df (pd.DataFrame): Raw monthly df

    Returns:
        pd.DataFrame: Cleaned monthly df
    """
    df = df.rename(columns={"Unnamed: 0": "date"})
    last_entry = df.loc[
        df.date == " Annual Factors: January-December "
    ].index.values[0]
    df = df.iloc[0:last_entry]
    df["date"] = (
        pd.to_datetime(df["date"], format="%Y%m") + pd.offsets.MonthEnd()
    ).dt.strftime("%Y-%m-%d")
    for k in df.columns:
        if k != "date":
            df[k] = df[k].astype(float)
    df = df.rename(columns={"Mkt-RF": "MktRF"})
    return df


@task(retries=2, retry_delay_seconds=60)
def write_files(daily_ff: pd.DataFrame, monthly_ff: pd.DataFrame) -> None:
    """
    Write files to disk

    Args:
        daily_ff (pd.DataFrame): df containing daily ff factors
        monthly_ff (pd.DataFrame): df containing monthly ff factors

    Returns:
        None:
    """
    logger = get_run_logger()
    logger.info("Writing files to disk")
    if not os.path.exists(config["local_path"]):
        os.makedirs(config["local_path"])
    daily_ff.to_parquet(config["daily_path"])
    monthly_ff.to_parquet(config["monthly_path"])
    return None


@flow(
    name="Load french fama factors to GCS", task_runner=ConcurrentTaskRunner()
)
def download_ff_factors():
    """
    Prefect flow that:
    1. Download FF factors from URL
    2. Clean data
    3. Write dataframe to parquet
    4. Upload to GCS
    """
    daily_ff = get_ff_df(config["daily_url"])
    daily_ff = clean_daily_ff(daily_ff)
    monthly_ff = get_ff_df(config["monthly_url"])
    monthly_ff = clean_monthly_ff(monthly_ff)
    write_files(daily_ff, monthly_ff)
    gcs_bucket = GcsBucket(
        bucket=config["bucket"],
        gcp_credentials=GcpCredentials.load("aurora-sa"),
    )
    gcs_bucket.put_directory(config["local_path"], config["destination_path"])

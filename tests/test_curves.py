import pytest
from prefect import flow
from prefect.testing.utilities import prefect_test_harness

from src.curves import (
    clean_curve,
    config,
    find_gilt_curve,
    find_treasury_curve,
    open_zip,
    read_historical_sheet,
    request_csv,
    request_zip,
)


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield


@flow()
def test_backfill_us():
    curve = request_csv(config["us"]["url_historical"])
    curve = find_treasury_curve(curve)
    curve = clean_curve(curve, location="US")
    records = curve.to_dict(orient="records")
    assert type(records[0]["date"]) == str
    assert type(records[0]["value"]) == float


@flow()
def test_backfill_uk():
    response = request_zip(config["uk"]["url_historical"])
    zip_file = open_zip(response)
    file = zip_file.namelist()[1]
    curve = read_historical_sheet(zip_file, file)
    curve = find_gilt_curve(curve)
    curve = clean_curve(curve, location="UK")
    records = curve.to_dict(orient="records")
    assert type(records[0]["date"]) == str
    assert type(records[0]["value"]) == float

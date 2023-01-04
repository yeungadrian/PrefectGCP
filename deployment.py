from prefect.deployments import Deployment
from prefect.filesystems import GCS
from prefect.orion.schemas.schedules import CronSchedule

from src.benchmark import download_us_benchmark
from src.french_fama import download_ff_factors
from src.funds import daily_fund_indices

gcs_block = GCS.load("dev")

deployment_fund_indices = Deployment.build_from_flow(
    flow=daily_fund_indices,
    name="test",
    parameters={},
    schedule=(
        CronSchedule(
            cron="0 12 5 * *",
            timezone="Europe/London",
        )
    ),
    # infra_overrides={"env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}},
    work_queue_name="test",
    storage=gcs_block,
)

deployment_fund_indices = Deployment.build_from_flow(
    flow=download_us_benchmark,
    name="test",
    parameters={},
    schedule=(
        CronSchedule(
            cron="0 12 5 * *",
            timezone="Europe/London",
        )
    ),
    # infra_overrides={"env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}},
    work_queue_name="test",
    storage=gcs_block,
)

deployment_fund_indices = Deployment.build_from_flow(
    flow=download_ff_factors,
    name="test",
    parameters={},
    schedule=(
        CronSchedule(
            cron="0 12 5 * *",
            timezone="Europe/London",
        )
    ),
    # infra_overrides={"env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}},
    work_queue_name="test",
    storage=gcs_block,
)

if __name__ == "__main__":
    deployment_fund_indices.apply()

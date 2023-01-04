# PrefectGCP

ETL "flows" using prefect for financial data into Google Cloud Storage.

## Uses:
-  [Prefect 2.0](https://app.prefect.cloud/auth/login)
- Google Cloud Storage (Object storage)
- Prefect agent deployed on local k8s cluster (https://github.com/yeungadrian/MLOps)

## Financial Data
- Daily UK Fund prices
- French Fama Five factors

## CI / CD
- Github actions for testing with pre-commit and pytest
- Deployment to Prefect Cloud

## Set up the environment
1. Install [Poetry](https://python-poetry.org/docs/#installation)
2. Set up the environment:
```bash
make activate
make setup
```

## Install new packages
To install new PyPI packages, run:
```bash
poetry add <package-name>
```
poetry export -f requirements.txt --output requirements.txt --without-hashes

## Useful Scripts
- vm_step.sh: Fresh linux vm setup scripts
- gcloud.sh: Service account auth / setting up buckets

## Backlog
- [ ] Setup GCP k8s cluster
- [ ] Deploy agent to k8s cluster
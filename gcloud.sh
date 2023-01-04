# Login using service account
gcloud auth activate-service-account --key-file=/Users/yeungadrian/aurora-361016-33547fb51f05.json

# Create bucket
gcloud storage buckets create gs://aurora-361016-market-data --default-storage-class=STANDARD --location=us-central1
gcloud storage buckets create gs://aurora-361016-prefect --default-storage-class=STANDARD --location=us-central1
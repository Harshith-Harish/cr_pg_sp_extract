a python-based flask application to trigger a PostgreSQL Stored Procedure, export the output as a .csv file,
and save it in a specified GCS bucket in GCP. Subsequently, an email will be sent with the GCS URI file path. This
solution can be hosted on cloud run in GCP and scheduled as a job in cloud scheduler.

FROM python:3.9

ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
WORK_DIR $APP_HOME
COPY . ./


RUN pip install -r requirements.txt


CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 gcp_cloud_run_stored_proc:gcp_cloud_run_stored_proc
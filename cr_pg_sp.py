import psycopg2
import datetime as dt
import json
import logging
import os
import stat
import psycopg2
import sys
import datetime as dt
from flask import Flask, request, jsonify
from distutils.log import INFO
from google.cloud import secretmanager
from google.cloud import storage
from google.auth.transport.requests import AuthorizedSession
from google.resumable_media import requests, common
from google.cloud import storage

from flask import request, jsonify
import datetime as dt
import json
import logging
import os
import stat
import psycopg2
from google.cloud import storage

logging.basicConfig(level=INFO)
gcp_cloud_run_stored_proc = Flask(__name__)

#file_time_stamp = dt.datetime.strptime(str(dt.datetime.now()), '%Y-%m-%d %H:%M:%S.%f').strftime('%Y%m%d%H%M%S')

class objectstreamupload(object):
    def __init__(
            self, 
            client: storage.Client,
            bucket_name: str,
            blob_name: str,
            chunk_size: int=256 * 1024
        ):
        self._client = client
        self._bucket = self._client.bucket(bucket_name)
        self._blob = self._bucket.blob(blob_name)

        self._buffer = b''
        self._buffer_size = 0
        self._chunk_size = chunk_size
        self._read = 0

        self._transport = AuthorizedSession(
            credentials=self._client._credentials
        )
        self._request = None  # type: requests.ResumableUpload

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            self.stop()

    def start(self):
        url = (
            f'https://www.googleapis.com/upload/storage/v1/b/'
            f'{self._bucket.name}/o?uploadType=resumable'
        )
        self._request = requests.ResumableUpload(
            upload_url=url, chunk_size=self._chunk_size
        )
        self._request.initiate(
            transport=self._transport,
            content_type='application/octet-stream',
            stream=self,
            stream_final=False,
            metadata={'name': self._blob.name},
        )

    def stop(self):
        self._request.transmit_next_chunk(self._transport)

    def write(self, data: bytes) -> int:
        data_len = len(data)
        self._buffer_size += data_len
        self._buffer += data
        del data
        while self._buffer_size >= self._chunk_size:
            try:
                self._request.transmit_next_chunk(self._transport)
            except common.InvalidResponse:
                self._request.recover(self._transport)
        return data_len

    def read(self, chunk_size: int) -> bytes:
        # I'm not good with efficient no-copy buffering so if this is
        # wrong or there's a better way to do this let me know! :-)
        to_read = min(chunk_size, self._buffer_size)
        memview = memoryview(self._buffer)
        self._buffer = memview[to_read:].tobytes()
        self._read += to_read
        self._buffer_size -= to_read
        return memview[:to_read].tobytes()

    def tell(self) -> int:
        return self._read


storage_client = storage.Client()

#Function To Read Secret Details From  SM
def fetch_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/YOUR_PROJECT_ID/secrets/{secret_id}/versions/latest"  # Update YOUR_PROJECT_ID
    try:
        response = client.access_secret_version(name=name)
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logging.error(f"Failed to get secret for key {secret_id}: {e}")
        raise  # Re-raise exception to be handled by the calling function
   
   

#Function to read config file and return dict
def fetch_conf_details(bucket_name, conf_file_name):
    logging.info(f"Reading config file from cloud storage: {conf_file_name}")
    client = storage.Client()  # Ensure storage_client is properly initialized here
    bucket = client.get_bucket(bucket_name)
    try:
        blob = bucket.blob(conf_file_name)
        with blob.open("r") as file:
            file_content = file.read()
        return file_content
    except Exception as e:
        logging.error(f"Failed to read config file {conf_file_name}: {e}")
        raise  # Re-raise exception to be handled by the calling function


#gs://pg_procedure_py/scripts/config_file.json
#End Point For Loading Delimieted File
@gcp_cloud_run_stored_proc.route("/stored_proc", methods=['GET'])
def sp_call():
    # Get current timestamp
    file_time_stamp = dt.datetime.now().strftime('%Y%m%d%H%M%S')

    # Read configuration file path from request
    try:
        conf_path = request.args.get('conf_path')
        if not conf_path:
            raise ValueError("Configuration path not provided")
        logging.info(f"Reading Config File From Path: {conf_path}")
        
        bucket_name, conf_file_path = conf_path.split("/", 2)[2], conf_path.split("/", 2)[-1]
        conf_file_name = conf_file_path.lstrip("/")
    except Exception as e:
        logging.error(f"Invalid Request: {e}")
        return jsonify({"error": str(e)}), 400

    # Read configuration details
    try:
        conf_details = fetch_conf_details(bucket_name, conf_file_name)
        conf_dict = json.loads(conf_details)
    except Exception as e:
        logging.error(f"Failed reading the configuration file: {e}")
        return jsonify({"error": "Configuration file read failed"}), 500

    # Extract configuration details
    try:
        config_keys = ['host_ip', 'db_username', 'db_password', 'database', 'port',
                        'server_cert', 'client_cert', 'client_key_cert', 
                        'gcs_extract_bucket', 'gcs_extract_path', 'stored_procedure', 
                        'view', 'function_name']
        config_values = {key: conf_dict.get(key) for key in config_keys}
        
        if None in config_values.values():
            raise ValueError("Some configuration values are missing")

        # Fetch secrets from secret manager
        for key in ['host_ip', 'db_username', 'db_password', 'database', 'port',
                    'server_cert', 'client_cert', 'client_key_cert']:
            config_values[key] = fetch_secret(config_values[key])
        
        # Define file names for certificates
        root_cert_file_name = "/tmp/root_cert.pem"
        client_cert_file_name = "/tmp/client_cert.pem"
        client_cert_key_file_name = "/tmp/client_key_cert.pem"

        # Write certificates to files
        with open(root_cert_file_name, "w") as f:
            f.write(config_values['server_cert'])
        with open(client_cert_file_name, "w") as f:
            f.write(config_values['client_cert'])
        with open(client_cert_key_file_name, "w") as f:
            f.write(config_values['client_key_cert'])

        # Set file permissions
        for file_name in [root_cert_file_name, client_cert_file_name, client_cert_key_file_name]:
            os.chmod(file_name, stat.S_IREAD)

    except Exception as e:
        logging.error(f"Error processing configuration or secrets: {e}")
        return jsonify({"error": "Configuration processing failed"}), 500

    # Establish database connection
    try:
        conn = psycopg2.connect(
            host=config_values['host_ip'],
            user=config_values['db_username'],
            password=config_values['db_password'],
            dbname=config_values['database'],
            port=config_values['port'],
            sslmode='verify-ca',
            sslrootcert=root_cert_file_name,
            sslcert=client_cert_file_name,
            sslkey=client_cert_key_file_name
        )
        conn.autocommit = True
        cursor = conn.cursor()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Failed to connect to PostgreSQL: {error}")
        return jsonify({"error": "Database connection failed"}), 500

    # Call the stored procedure
    try:
        cursor.execute(f"CALL {config_values['stored_procedure']};")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Failed to call stored procedure: {error}")
        conn.rollback()
        return jsonify({"error": "Stored procedure execution failed"}), 500

    # Export data to GCS
    try:
        cursor.execute(f"SELECT * FROM {config_values['view']};")
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        updated_column_names = ','.join(column_names)

        csv_data = updated_column_names + "\n" + "\n".join([",".join(map(lambda x: str(x).replace(",", ""), row)) for row in rows])

        gcs_export_file_path = f"{config_values['gcs_extract_path']}/{config_values['view']}_{file_time_stamp}.csv"

        # Upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(config_values['gcs_extract_bucket'])
        blob = bucket.blob(gcs_export_file_path)
        blob.upload_from_string(csv_data, content_type="text/csv")

        logging.info("View Data successfully exported to GCS bucket")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Failed exporting data to GCS: {error}")
        conn.rollback()
        return jsonify({"error": "Data export to GCS failed"}), 500
    finally:
        conn.close()

    return "Success", 200
   
   

if __name__ == "__main__":
    gcp_cloud_run_stored_proc.run(host="0.0.0.0", port=8080)


import psycopg2
import time
import json
import logging
import stat
import sys
import os
import datetime as dt
from flask import Flask, request, jsonify
from distutils.log import INFO
from google.cloud import secretmanager
from google.cloud import storage
from google.auth.transport.requests import AuthorizedSession
from google.resumable_media import requests, common
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
def fetch_secret(id):
    client = secretmanager.SecretManagerServiceClient()
    id = {"name": f"{id}/versions/latest"}
    try:
        response = client.access_secret_version(id)
    except Exception as e:
        logging.error("Failed to Get SM for key : "+str(id))
        sys.exit(1)
    return response.payload.data.decode("UTF-8")
   
   

#Function to read config file and return dict
def fetch_conf_details(bucket_name, conf_file_name):
    logging.info("Reading config file from cloud storage : "+str(conf_file_name))
    bucket = storage_client.get_bucket(bucket_name)
    try:
        blob = bucket.blob(conf_file_name)
        with blob.open("r") as file:
            file_content =  file.read()
    except Exception as e:
        logging.error("Failed to read config File : "+str(conf_file_name))
        logging.error(e)
        sys.exit(1)
    return file_content


#gs://pg_procedure_py/scripts/config_file.json
#End Point For Loading Delimieted File
@gcp_cloud_run_stored_proc.route("/stored_proc" , methods = ['GET'])    
def sp_call():

    file_time_stamp = dt.datetime.strptime(str(dt.datetime.now()), '%Y-%m-%d %H:%M:%S.%f').strftime('%Y%m%d%H%M%S')
    
    try:
        conf_path = request.args.get('conf_path')
        logging.info(" Reading Config File From Path : "+str(conf_path))    
        bucket_name = str(conf_path).split("/")[2]
        conf_file_path = str(conf_path).split(bucket_name)[-1] #
    except Exception as e:
        logging.error("Invalid Request... : "+str(e))
        sys.exit(1)    
   
    #Removing first / from path
    conf_file_name= conf_file_path.replace("/","", 1)
   
    #Read Config File Details
    try:
        conf_details = fetch_conf_details(bucket_name, conf_file_name)
        conf_dict = json.loads(conf_details)
    except Exception as e:
        logging.error("Failed reading the conf file : "+str(e))
        sys.exit(1)

    try:    
        host_ip = conf_dict.get('host_ip')
        db_username = conf_dict.get('db_username')
        db_password = conf_dict.get('db_password')
        database =  conf_dict.get('database')
        port = conf_dict.get('port')
        server_cert = conf_dict.get('server_cert')
        client_cert = conf_dict.get('client_cert')
        client_key_cert = conf_dict.get('client_key_cert')
        gcs_extract_bucket = conf_dict.get('gcs_extract_bucket')
        gcs_extract_path = conf_dict.get('gcs_extract_path')
        stored_procedure = conf_dict.get('stored_procedure')
        view = conf_dict.get('view')
        function_name = conf_dict.get('function_name')
        #table_name = conf_dict.get('table_name')
    except Exception as e:
        logging.error("Failed reading the conf file : "+str(e))
        sys.exit(1)

    #Reading configuration and SSl from SM
    try:
        host_ip=fetch_secret(host_ip)
        db_username=fetch_secret(db_username)
        db_password=fetch_secret(db_password)
        database=fetch_secret(database)
        port=fetch_secret(port)
        server_cert = fetch_secret(server_cert)
        client_cert = fetch_secret(client_cert)
        client_key_cert = fetch_secret(client_key_cert)
    except Exception as e:
        logging.error('Error occured reading from secret manager : '+str(e))
        sys.exit(1)

    #.pem files
    with open(root_cert_file_name, "w") as f1:
        f1.write(server_cert)
   
    with open(client_cert_file_name, "w") as f1:
        f1.write(client_cert)
   
    with open(client_cert_key_file_name, "w") as f1:
        f1.write(client_key_cert)

    os.chmod(root_cert_file_name, stat.S_IREAD)
    os.chmod(client_cert_file_name, stat.S_IREAD)
    os.chmod(client_cert_key_file_name, stat.S_IREAD)
    #filemode = stat.S_IMODE(os.stat(client_cert_key_file_name).st_mode)


    logging.info("host_ip : "+str(host_ip))
    logging.info("database : "+str(database))
    logging.info("db_username : "+str(db_username))
    logging.info("db_password : "+str(db_password))
    logging.info("port : "+str(port))
    logging.info("gcs_extract_bucket : "+str(gcs_extract_bucket))
    logging.info("gcs_extract_path : "+str(gcs_extract_path))
    logging.info("stored_procedure : "+str(stored_procedure))
    logging.info("function_name : "+str(function_name))
    logging.info("view : "+str(view))
    logging.info("table_name : "+str(table_name))
   
    connection_established = False
    try:
        if not connection_established:
            conn = psycopg2.connect(host=host_ip, user=db_username, password=db_password, dbname=database, port=port, sslmode='verify-ca', sslrootcert=root_cert_file_name, sslcert=client_cert_file_name, sslkey=client_cert_key_file_name)
            cursor = conn.cursor()
            conn = conn
            conn.autocommit = True
           
            connection_established = True
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("Failed creating connection with PostgreSQL.")
        logging.error(error)
        sys.exit(1)      

    #Calling Stored Procedure
    try:
        cursor.execute("CALL "+stored_procedure+ " ;")
        logging.info("--------------------------------------------")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("Failed to call stored procedure.")
        logging.error(error)
        conn.rollback()
        cursor.close()
        sys.exit(1)
       
    #Export the updated data to csv.
    try:
        cursor.execute("Select * from "+view+";")
        rows = cursor.fetchall()
        #Fetchi Column Names
        column_names = [desc[0] for desc in cursor.description]
        updated_column_names = ','.join(column_names)
       
        #File Path to store CSV
        gcs_export_file_path = gcs_extract_path + view +"_"+file_time_stamp+".csv"
       
        #Convert Rows to CSV data
        #csv_data = updated_column_names+"\n"+"\n".join([",".join(map(str, row)) for row in rows])
        csv_data = updated_column_names+"\n"+"\n".join([",".join(map(lambda x::str(x).replace(",",""),row)) for row in rows])
        
        #to load small size files to GCS uncomment below block of code
        #bucket = storage_client.get_bucket(gcs_extract_bucket)
        #blob = bucket.blob(gcs_export_file_path)
        #blob.upload_from_string(csv_data, content_type="text/csv")
        #logging.info("View Data successfully exported to GCS bucket!!!!!!")
        
        with objectstreamupload(client=storage_client, bucket_name= gcs_extract_bucket, blob_name= gcs_export_file_path) as t:
            t.write(bytes(csv_data,'UTF-8'))
        logging.info("View Data successfully exported to GCS bucket!!!!!!")
         
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("Failed exporting data to GCS bucket")
        logging.error(error)
        conn.rollback()
        conn.close()
        sys.exit(1)

    return "success"
   
   

if __name__ == "__main__":
    gcp_cloud_run_stored_proc.run(host="0.0.0.0", port=8080)


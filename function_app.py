import logging, time
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError
from io import StringIO
import pandas as pd
import pymssql
import re, os, traceback
from datetime import datetime
import pyodbc
from dotenv import load_dotenv
from database import insert_metadata, insert_weights
from config import server_name, user_name, pwd, db_name, CONTAINER_NAME, INPUT_FOLDER, OUTPUT_FOLDER, FAULTY_FOLDER, STORAGE_CONNECTION_STRING
load_dotenv()

app = func.FunctionApp()

''' TimerTrigger function '''
@app.function_name(name="TimerTrigger")
@app.timer_trigger(schedule="0 */10 * * * *", arg_name="myTimer")
def timer_trigger1(myTimer: func.TimerRequest) -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if myTimer.past_due:
        print("The timer is past due!")

    DOWNLOAD_TIMEOUT = 240  # 4 minutes
    COPY_TIMEOUT = 300      # 5 minutes
    DELETE_TIMEOUT = 180    # 3 minutes
    # Initialize Blob Service Client
    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blobs = container_client.list_blobs(name_starts_with=INPUT_FOLDER)
    blobsList = list(container_client.list_blobs(name_starts_with=INPUT_FOLDER))  

    wld_pattern = r'^WLD\d+$'
    required_columns = ['Sample Number', 'Variety', 'Formulation', 'Number of Samples']

    """ Establishes a connection to the database with retry logic. """
    def dbConnect(server_name, user_name, pwd, db_name, max_retries=3, retry_delay=5):
        for attempt in range(max_retries):
            try:
                # Attempt to establish the connection
                conn = pymssql.connect(
                    server=server_name,
                    user=user_name,
                    password=pwd,
                    database=db_name,
                    timeout=10  
                )
                logging.info(f"Successfully connected to the database '{db_name}' on attempt {attempt + 1}.")
                return conn
            except pymssql.InterfaceError as ie:
                logging.error(f"Database interface error: {ie} on attempt {attempt + 1}")
            except pymssql.DatabaseError as de:
                logging.error(f"Database error: {de} on attempt {attempt + 1}")
            except pymssql.OperationalError as oe:
                logging.error(f"Operational error (e.g., network issue): {oe} on attempt {attempt + 1}")
            except Exception as e:
                logging.error(f"Unexpected error: {e} on attempt {attempt + 1}")

            # Exponential backoff logic for retries
            if attempt < max_retries - 1:
                delay = retry_delay * (2 ** attempt)  # Exponential backoff
                logging.info(f"Retrying connection in {delay} seconds... (Attempt {attempt + 2}/{max_retries})")
                time.sleep(delay)
        
        logging.error(f"Failed to connect to the database '{db_name}' after {max_retries} attempts.")
        return None


    """ Function to get file metadata (last modified date) """
    def get_file_metadata(blob):
        try:
            # Directly get the 'last_modified' timestamp from the BlobProperties
            modification_time = blob['last_modified']
            return modification_time
        except Exception as e:
            logging.error(f"Error reading file metadata for {blob}: {e}")
            return None

    """ Function to calculate average weight loss """
    def calculate_avg_weight_loss(group):
        non_null_weights = group['Weight'].dropna()
        if len(non_null_weights) > 1:
            first_weight = non_null_weights.iloc[0]
            last_weight = non_null_weights.iloc[-1]
            total_days = group['WLD'].iloc[-1] - group['WLD'].iloc[0] + 1
            avg_weight_loss = ((first_weight - last_weight) / first_weight) * 100 / total_days
        else:
            avg_weight_loss = None
        group['avg_weight_loss'] = avg_weight_loss
        return group
    
    """ Function to extract file_code """
    def extract_file_code(blob):
        try:
            # Ensure we get the blob name as a string
            blob_name = str(blob.name).strip()  
            file_name = blob_name.split('/')[-1]  
            if isinstance(file_name, str):
                # Check if the file name starts with a number
                match = re.match(r'^\d+', file_name)  
                if match:
                    file_code = match.group(0)  
                    return file_code
                else:
                    # If the file name does not start with a number, log and move to faulty folder
                    logging.warning(f"File name {file_name} does not start with a number.")
                    return None
            else:
                logging.error(f"Blob name is not a string: {file_name}")
                return None
        except Exception as e:
            logging.error(f"Error extracting file code from {blob}: {e}")
            return None

    """ insertion of metadata and weight records into the database """
    def data_insertion(file_metadata_records, weight_records, conn):
        data_inserted = False  # Track if data insertion is successful
        try:            
            insert_metadata(file_metadata_records, conn)
            logging.info("insert_metadata data inserted successfully.")

            insert_weights(weight_records, conn)
            logging.info("Weight data inserted successfully.")
            # Commit the transaction if no errors occurred
            conn.commit()
            data_inserted = True  # Set success flag

        except Exception as e:
            # Log the error with detailed traceback for debugging
            logging.error(f"Database insertion error: {traceback.format_exc()}")
            try:
                conn.rollback()  # Rollback the transaction on error
                logging.info("Transaction rolled back.")
            except Exception as rollback_error:
                logging.error(f"Rollback failed: {traceback.format_exc()}")

        finally:
            try:
                conn.close()  # Ensure the database connection is closed
                logging.info("Database connection closed.")
            except Exception as close_error:
                logging.error(f"Error while closing the database connection: {traceback.format_exc()}")
            logging.info(f"data_inserted is {data_inserted}")

        return data_inserted
    # Move processed files to the archive
    # def archive_blob(blob):
    #     logging.info(f"archive blob name: {blob}")
    #     try:
    #         copied_blob_client = container_client.get_blob_client(blob.name)
    #         copied_blob_name = blob.name.replace(INPUT_FOLDER, OUTPUT_FOLDER)
    #         container_client.get_blob_client(copied_blob_name).start_copy_from_url(copied_blob_client.url)
    #         copied_blob_client.delete_blob()
    #     except Exception as e:
    #         logging.error(f"Failed to archive blob {blob.name}: {e}")
    """ Function to move files to the faulty folder in case file_code is None ans required columns does not get in csv """
    def faulty_blob(blob):
        logging.info(f"Processing file: {blob}")
        try:
            faulty_blob_client = container_client.get_blob_client(blob.name)
            faulty_blob_name = blob.name.replace(INPUT_FOLDER, FAULTY_FOLDER)
            container_client.get_blob_client(faulty_blob_name).start_copy_from_url(faulty_blob_client.url)
            faulty_blob_client.delete_blob()
        except Exception as e:
            logging.error(f"Failed to faulty blob {blob.name}: {e}")

    """ etl() function which handles transormation and data insertion. """
    def etl():
        data_frames = []
        try:
            logging.info(f"etl blob")
            for blob in blobs:
                blob_client = container_client.get_blob_client(blob.name)
                download_stream = blob_client.download_blob()
                file_code = extract_file_code(blob)
                logging.info(f"file code is {file_code}")
                # Skip further processing if file_code is None
                if file_code is None:
                    logging.info(f"file code is {file_code}")
                    logging.warning(f"faulty: Skipping processing for blob {blob.name} due to invalid file code {file_code}.")
                    faulty_blob(blob)
                    continue

                file_date = get_file_metadata(blob)
                df = pd.read_csv(StringIO(download_stream.content_as_text()))  
                # df['Filename'] = blob.name
                df['Filename'] = os.path.basename(blob.name)
                df['UploadDate'] = file_date
                df['file_code'] = extract_file_code(blob)
                if not all(col in df.columns for col in required_columns) or df.get('file_code') is None:
                    logging.warning(f"faulty: Skipping file {blob} because it is missing required columns: {', '.join(required_columns)}")
                    faulty_blob(blob)
                    continue 

                if 'WLDO' in df.columns:
                    df['WLDO'] = df['WLDO'].str.strip().replace('WLDO', 'WLD0')

                if 'Number of Samples' not in df.columns:
                    df['Number of Samples'] = pd.NA
                wld_columns = [col for col in df.columns if re.match(wld_pattern, col)]
                melted_df = df.melt(
                    id_vars=['Sample Number', 'Variety', 'Formulation', 'Number of Samples', 'Filename', 'UploadDate', 'file_code'],
                    value_vars=wld_columns,
                    var_name='WLD',
                    value_name='Weight'
                )
                melted_df['WLD'] = melted_df['WLD'].str.extract(r'(\d+)').astype(int)
                melted_df['DateDifference'] = melted_df['UploadDate'] - pd.to_timedelta(melted_df['WLD'], unit='D')
                melted_df = melted_df.sort_values(by=['Sample Number', 'WLD']).reset_index(drop=True)
                melted_df['group_id'] = melted_df['Sample Number']
                melted_df = melted_df.groupby('group_id').apply(calculate_avg_weight_loss)
                data_frames.append(melted_df)

                if data_frames:
                    final_df = pd.concat(data_frames, ignore_index=True)
                    output_columns = [
                        'Filename', 'Variety', 'Formulation', 'Number of Samples', 'UploadDate',
                        'DateDifference', 'WLD', 'Weight', 'group_id', 'file_code', 'avg_weight_loss'
                    ]
                    final_output = final_df[output_columns]
                    file_metadata_records = final_output[['Filename', 'Variety', 'Formulation', 'Number of Samples', 'UploadDate', 'group_id', 'file_code']].drop_duplicates()
                    weight_records = final_output[['group_id', 'file_code', 'WLD', 'Weight', 'DateDifference']].drop_duplicates(subset=['group_id', 'file_code', 'WLD', 'Weight', 'DateDifference'])
                    conn = dbConnect(server_name, user_name, pwd, db_name)
                    data_inserted = data_insertion(file_metadata_records, weight_records, conn)
                    if data_inserted == True:
                            logging.info(f"===============[[[[[[data_inserted successfully of blob {blob}]]]]]]===============")
                            blob_client = container_client.get_blob_client(blob.name)
                            copied_blob_name = blob.name.replace(INPUT_FOLDER, OUTPUT_FOLDER)
                            copied_blob_client = container_client.get_blob_client(copied_blob_name)
                            copied_blob_client.start_copy_from_url(blob_client.url, timeout=COPY_TIMEOUT)
                            blob_client.delete_blob(timeout=DELETE_TIMEOUT)
                    else:
                        logging.error(f"POLL of Data insertion did not complete.")
                        # faulty_blob(blob)
        
                else:
                    logging.info("No blobs were processed.")

        except ResourceNotFoundError:
            logging.error("The specified container does not exist.")
        except Exception as e:
            logging.error(f"Error during ETL process: {str(e)}")
        finally:
            logging.info("ETL Blob process completed successfully.")
            return  
    etl()

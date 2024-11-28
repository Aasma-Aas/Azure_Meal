import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from io import StringIO
import pandas as pd
import pymssql
import re, os, traceback
from datetime import datetime
import pyodbc
from dotenv import load_dotenv
load_dotenv()
from database import table_file_metadata, table_weight
from config import server_name, user_name, pwd, db_name, CONTAINER_NAME, INPUT_FOLDER, OUTPUT_FOLDER, FAULTY_FOLDER, STORAGE_CONNECTION_STRING

app = func.FunctionApp()

''' TimerTrigger function '''
@app.function_name(name="TimerTrigger")
@app.timer_trigger(schedule="0 */10 * * * *", arg_name="myTimer")
def timer_trigger1(myTimer: func.TimerRequest) -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if myTimer.past_due:
        print("The timer is past due!")

    # Initialize Blob Service Client
    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blobs = container_client.list_blobs(name_starts_with=INPUT_FOLDER)
    wld_pattern = r'^WLD\d+$'
    # Define the required columns for validation
    required_columns = ['Sample Number', 'Variety', 'Formulation', 'Number of Samples']
    # Connect to the database
    def dbConnect():
        try:
            conn = pymssql.connect(
                server=server_name,
                user=user_name,
                password=pwd,
                database=db_name
            )
            logging.info("Successfully connected to the database.")
            return conn
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            return None

    # Function to get file metadata (last modified date)
    def get_file_metadata(blob):
        try:
            # Directly get the 'last_modified' timestamp from the BlobProperties
            modification_time = blob['last_modified']
            return modification_time
        except Exception as e:
            logging.error(f"Error reading file metadata for {blob}: {e}")
            return None

    # Function to calculate average weight loss
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

    # Function to extract file code from filename
    def extract_file_code(blob):
        try:
            blob_name = blob.name  # Ensure we get the blob name as a string
            if isinstance(blob_name, str):
                match = re.search(r'\d+', blob_name)
                if match:
                    return match.group(0)  # Return the first matched number
            else:
                logging.error(f"Blob name is not a string: {blob_name}")
                return None
        except Exception as e:
            logging.error(f"Error extracting file code from {blob}: {e}")
            return None
            
    # Move processed files to the archive
    def archive_blob(blob):
        logging.info(f"Processing file: {blob}")
        try:
            copied_blob_client = container_client.get_blob_client(blob.name)
            copied_blob_name = blob.name.replace(INPUT_FOLDER, OUTPUT_FOLDER)
            container_client.get_blob_client(copied_blob_name).start_copy_from_url(copied_blob_client.url)
            copied_blob_client.delete_blob()
        except Exception as e:
            logging.error(f"Failed to archive blob {blob.name}: {e}")

    def faulty_blob(blob):
        logging.info(f"Processing file: {blob}")
        try:
            faulty_blob_client = container_client.get_blob_client(blob.name)
            faulty_blob_name = blob.name.replace(INPUT_FOLDER, OUTPUT_FOLDER)
            container_client.get_blob_client(faulty_blob_name).start_copy_from_url(faulty_blob_client.url)
            faulty_blob_client.delete_blob()
        except Exception as e:
            logging.error(f"Failed to archive blob {blob.name}: {e}")


    data_frames = []
    for blob in blobs:
        blob_client = container_client.get_blob_client(blob.name)
        logging.info(f"Attempting to download blob: {blob.name}")
        download_stream = blob_client.download_blob()
        logging.info(f"Downloaded blob: {blob.name}")

        file_date = get_file_metadata(blob)
        logging.info(f"Processing file: {blob} - Last modified date: {file_date}")
        df = pd.read_csv(StringIO(download_stream.content_as_text()))  
        # df['Filename'] = blob.name
        df['Filename'] = os.path.basename(blob.name)
        df['UploadDate'] = file_date
        df['file_code'] = extract_file_code(blob)
        if not all(col in df.columns for col in required_columns):
            logging.warning(f"Skipping file {blob} because it is missing required columns: {', '.join(required_columns)}")
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
        # Clean up WLD column and calculate DateDifference
        melted_df['WLD'] = melted_df['WLD'].str.extract(r'(\d+)').astype(int)
        melted_df['DateDifference'] = melted_df['UploadDate'] - pd.to_timedelta(melted_df['WLD'], unit='D')
        # Sort by Sample Number and WLD
        melted_df = melted_df.sort_values(by=['Sample Number', 'WLD']).reset_index(drop=True)
        # Create group_id and calculate average weight loss
        melted_df['group_id'] = melted_df['Sample Number']
        melted_df = melted_df.groupby('group_id').apply(calculate_avg_weight_loss)
        data_frames.append(melted_df)
       

    if data_frames:
        copied_blob_name = blob.name.replace(INPUT_FOLDER, OUTPUT_FOLDER)
        logging.info(f"Processing file copied_blob_name: {copied_blob_name}")
        container_client.get_blob_client(copied_blob_name).start_copy_from_url(blob_client.url)
        blob_client.delete_blob()
        
        final_df = pd.concat(data_frames, ignore_index=True)
        output_columns = [
            'Filename', 'Variety', 'Formulation', 'Number of Samples', 'UploadDate',
            'DateDifference', 'WLD', 'Weight', 'group_id', 'file_code', 'avg_weight_loss'
        ]
        final_output = final_df[output_columns]
        # output_file = r"E:\MyAzureFunctionApp\output.csv"
        # final_output.to_csv(output_file, index=False)
        file_metadata_records = final_output[['Filename', 'Variety', 'Formulation', 'Number of Samples', 'UploadDate', 'group_id', 'file_code']].drop_duplicates()
        weight_records = final_output[['group_id', 'file_code', 'WLD', 'Weight', 'DateDifference']].drop_duplicates(subset=['group_id', 'file_code', 'WLD', 'Weight', 'DateDifference'])
        conn = dbConnect()
        table_file_metadata(file_metadata_records, conn)
        table_weight(weight_records, conn)

        # for blob in blobs:
        #     archive_blob(blob)

    else:
        logging.info("No blobs were processed.")
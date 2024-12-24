import os
import logging
import traceback
import pymssql

def insert_metadata(file_metadata_records, conn):
    # Ensure connection is valid
    if conn is None:
        logging.error("Database connection is None. Aborting insert.")
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")  
    except pymssql.OperationalError as e:
        logging.error(f"Database connection is not active. Error: {e}")
        return
    try:
        for _, row in file_metadata_records.iterrows():
            try:
                 # Check if the record exists using the condition
                cursor.execute("""
                    SELECT COUNT(1) FROM dbo.File_Metadata
                    WHERE group_id = %s
                    AND file_code = %s
                """, (row['group_id'], row['file_code']))
                
                record_exists = cursor.fetchone()[0]
                
                if record_exists == 0:  
                    cursor.execute("""
                        INSERT INTO dbo.File_Metadata (Filename, Variety, Formulation, Number_of_Samples, UploadDate, group_id, file_code)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (row['Filename'], row['Variety'], row['Formulation'], row['Number_of_Samples'], row['UploadDate'], row['group_id'], row['file_code']))
                    logging.info(f"Inserted File_Metadata record: {row.to_dict()}")
                else:
                    logging.info(f"Skipped File_Metadata record (duplicate): {row.to_dict()}")
                
            except Exception as e:
                logging.error(f"Error during database insertion: {traceback.format_exc()}")
    except pymssql.Error as e:
        logging.error(f"Error during insertion: {e}")
    except pymssql.Error as db_error:
        logging.error(f"Database error: {db_error}")
    finally:
        conn.commit()
        cursor.close()

def insert_weights(weight_records, conn):
    # Check if the connection is None
    if conn is None:
        logging.error("Database connection is None. Aborting insert.")
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")  
    except pymssql.OperationalError as e:
        logging.error(f"Database connection is not active. Error: {e}")
        return
    
    try:
        for _, row in weight_records.iterrows():
            try:
                # Check if the record exists using the condition
                cursor.execute("""
                    SELECT COUNT(1) FROM dbo.WeightLoss
                    WHERE group_id = %s
                    AND file_code = %s
                    AND WLD = %s
                    AND Weight = %s
                    AND DateDifference = %s
                """, (row['group_id'], row['file_code'], row['WLD'], row['Weight'], row['DateDifference']))
                
                record_exists = cursor.fetchone()[0]
                
                if record_exists == 0:  
                    cursor.execute("""
                        INSERT INTO dbo.WeightLoss (group_id, file_code, WLD, Weight, DateDifference)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (row['group_id'], row['file_code'], row['WLD'], row['Weight'], row['DateDifference']))
                    logging.info(f"Inserted weight record: {row.to_dict()}")
                else:
                    logging.info(f"Skipped weight record (duplicate): {row.to_dict()}")

            except Exception as e:
                logging.error(f"Error processing weight record {row.to_dict()}: {e}")
    except pymssql.Error as db_error:
        logging.error(f"Database error: {db_error}")
    finally:
        conn.commit()
        cursor.close()

from io import StringIO
import pandas as pd
import pymssql
import re, os, traceback
from datetime import datetime
from dotenv import load_dotenv 


class MEAL_DATABASE():
    # # Connect to the database
    # def dbConnect():
    #     try:
    #         conn = pymssql.connect(
    #             server=server_name,
    #             user=user_name,
    #             password=pwd,
    #             database=db_name
    #         )
    #         logging.info("Successfully connected to the database.")
    #         return conn
    #     except Exception as e:
    #         logging.error(f"Database connection error: {e}")
    #         return None
    def dbConnect():
        retries = 3
        while retries > 0:
            try:
                conn = pymssql.connect(
                server=server_name,
                user=user_name,
                password=pwd,
                database=db_name
                )
                return conn
            except Exception as e:
                retries -= 1
                logging.error(f"Database connection error: {e}. Retries left: {retries}")
                if retries == 0:
                    raise

    def table_file_metadata(file_metadata_records):
        conn = dbConnect()
        if conn is None:
            logging.error("Failed to connect to the database. Aborting insert.")
            return  # Exit if connection failed
        cursor = conn.cursor()
        try:
            for _, row in file_metadata_records.iterrows():
                try:
                    logging.info(f"Inserting row into File_Metadata: {row.to_dict()}")
                    cursor.execute("""
                        INSERT INTO dbo.File_Metadata (Filename, Variety, Formulation, Number_of_Samples, UploadDate, group_id, file_code)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (row['Filename'], row['Variety'], row['Formulation'], row['Number of Samples'], row['UploadDate'], row['group_id'], row['file_code']))
                    logging.info(f"Inserted row into File_Metadata successfully.")
                    cursor.execute("SELECT SCOPE_IDENTITY()")
                except Exception as e:
                    logging.error(f"Error during database insertion: {traceback.format_exc()}")
        except pymssql.Error as e:
            logging.error(f"Error during insertion: {e}")
        finally:
            conn.commit()
            cursor.close()
            conn.close()

    def table_weight(weight_records):
        conn = dbConnect()
        if conn is None:
            logging.error("Failed to connect to the database. Aborting insert.")
            return  # Exit if connection failed
        cursor = conn.cursor()
        try:
            for _, row in weight_records.iterrows():
                try:
                    group_id = int(row['group_id'])
                    file_code = row['file_code']
                    WLD = int(row['WLD'])
                    Weight = int(row['Weight'])
                    DateDifference = row['DateDifference']

                    cursor.execute("""
                        SELECT COUNT(1)
                        FROM dbo.WeightLoss
                        WHERE group_id = %s AND file_code = %s AND WLD = %s AND Weight = %s AND DateDifference = %s
                    """, (group_id, file_code, WLD, Weight, DateDifference))
                    exists = cursor.fetchone()[0]

                    if exists == 0:
                        cursor.execute("""
                            INSERT INTO dbo.WeightLoss (group_id, file_code, WLD, Weight, DateDifference)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (group_id, file_code, WLD, Weight, DateDifference))

                        logging.info(f"Successfully inserted row: {row.to_dict()}")
                    else:
                        logging.info(f"Duplicate found for row: {row.to_dict()}, skipping insertion.")
                except Exception as e:
                    logging.error(f"Failed to insert row into Weight: {e}")
        except pymssql.Error as e:
            logging.error(f"Error during insertion: {e}")
        finally:
            conn.commit()
            cursor.close()
            conn.close()

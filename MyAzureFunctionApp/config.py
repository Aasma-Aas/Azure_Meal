# config.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection details
server_name = os.environ.get('SERVER')
user_name = os.environ.get('USER_NAME')
pwd = os.environ.get('PASSWORD')
db_name = os.environ.get('DATABASE')

# Other configurations, such as storage connection string
STORAGE_CONNECTION_STRING = os.environ.get('AzureWebJobsStorage')
CONTAINER_NAME = 'csv-files'
INPUT_FOLDER = 'InputFiles/'
OUTPUT_FOLDER = 'Archive/'
FAULTY_FOLDER = 'Faulty/'

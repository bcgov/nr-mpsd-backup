import dotenv
import os

# the path to the env file
envPath = '.env'
if os.path.exists(envPath):
    print("loading dot env...")
    dotenv.load_dotenv()

# postgres constants
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWD=os.environ['POSTGRES_PASSWD']
POSTGRES_INSTANCE=os.environ['POSTGRES_INSTANCE']
# top level folder of MPSD
SRC_ROOT_DIR = os.environ['SRC_ROOT_DIR']

# object store constants
OBJ_STORE_SECRET = os.environ['OBJ_STORE_SECRET']
OBJ_STORE_USER = os.environ['OBJ_STORE_USER']
OBJ_STORE_HOST = os.environ['OBJ_STORE_HOST']

# FROM SNOWPACK - Constant used for getDirectoryDate 
# used to identify directories that contain dates in them
# DIRECTORY_DATE_REGEX = '^[0-9]{4}\.{1}[0-9]{2}\.{1}[0-9]{2}$'  # noqa: W605


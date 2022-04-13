import dotenv
import os
import logging

# LOGGER statements in this file do not write to the log. Not sure why
# In other backupMPSD module they are in functions. Is that it?
# either way convert these to print statements for now

LOGGER = logging.getLogger(__name__)
print("loading constants")

# the path to the env file - testing purposes only
env_Folder = r"\\sfp.idir.bcgov\U161\NHEIDECK$\secrets\mpsd\archive\mpsddlvr_env_file"
env_Path = os.path.join(env_Folder, '.env')

if os.path.exists(env_Path):
    print("loading dot env...")
    dotenv.load_dotenv(env_Path)
else:
    print("envPath does not exist")

# postgres constants
POSTGRES_DB = os.environ['POSTGRES_DB']
POSTGRES_ID = os.environ['POSTGRES_ID']
POSTGRES_HOST = os.environ['POSTGRES_HOST']
POSTGRES_PORT = os.environ['POSTGRES_PORT']
POSTGRES_SECRET = os.environ['POSTGRES_SECRET']

# object store constants
OBJ_STORE_SECRET = os.environ['OBJ_STORE_SECRET']
OBJ_STORE_USER = os.environ['OBJ_STORE_USER']
OBJ_STORE_HOST = os.environ['OBJ_STORE_HOST']
OBJ_STORE_BUCKET = os.environ['OBJ_STORE_BUCKET']

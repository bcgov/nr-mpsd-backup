import dotenv
import os
import logging

# LOGGER statements in this file do not write to the log. Not sure why
# In other backupMPSD module they are in functions. Is that it?
# either way convert these to print statements in development for now

LOGGER = logging.getLogger(__name__)
LOGGER.debug("loading constants")

try:
    from . import envpath
    LOGGER.debug("Imported envpath.py")
except:
    LOGGER.error("envpath.py not imported / not found")

# the path to the env file
env_Path = os.path.join(envpath.env_Folder, '.env')
if os.path.exists(env_Path):
    LOGGER.debug("loading dot env...")
    dotenv.load_dotenv(env_Path)
else:
    LOGGER.debug("envPath does not exist")

# path to save .log and .dmp during development
WORKING_FOLDER = os.environ['WORKING_FOLDER']

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
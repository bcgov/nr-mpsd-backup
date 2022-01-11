import dotenv
import os
import logging
LOGGER = logging.getLogger(__name__)

# the path to the env file
envPath = os.path.join(os.path.dirname(__file__), '..', '.venv')
if os.path.exists(envPath):
    LOGGER.debug("loading dot env...")
    dotenv.load_dotenv()
else:
    LOGGER.debug("envPath does not exist")

POSTGRES_DB = os.environ['POSTGRES_DB']
POSTGRES_ID = os.environ['POSTGRES_ID']
POSTGRES_HOST = os.environ['POSTGRES_HOST']
POSTGRES_PORT = os.environ['POSTGRES_PORT']
POSTGREST_SECRET = os.environ['POSTGRES_SECRET']

# object store constants
#OBJ_STORE_SECRET = os.environ['OBJ_STORE_SECRET']
#OBJ_STORE_USER = os.environ['OBJ_STORE_USER']
#OBJ_STORE_HOST = os.environ['OBJ_STORE_HOST']


import dotenv
import os

# the path to the env file
envPath = '.env'
if os.path.exists(envPath):
    print("loading dot env...")
    dotenv.load_dotenv()


POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWD=os.environ['POSTGRES_PASSWD']
POSTGRES_INSTANCE=os.environ['POSTGRES_INSTANCE']

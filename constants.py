import dotenv
import os

envPath = '.env'
if os.path.exists(envPath):
    print("loading dot env...")
    dotenv.load_dotenv()

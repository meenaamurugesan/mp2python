import os
from dotenv import load_dotenv


load_dotenv()  # reads variables from a .env file and sets them in os.environ



def get_db_url():
    POSTGRES_USERNAME = os.environ["POSTGRES_USERNAME"]
    POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
    POSTGRES_SERVER = os.environ["POSTGRES_SERVER"]
    POSTGRES_DATABASE = os.environ["POSTGRES_DATABASE"]

    DATABASE_URL = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}/{POSTGRES_DATABASE}"

    return DATABASE_URL

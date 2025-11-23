import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def create_db_engine():

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    if not all([db_user, db_password, db_host, db_port, db_name ]):
        raise ValueError (f"ERROR during loading data from .env file")

    connection_str = (f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

    try:
        engine = create_engine(connection_str) #db engine creation
        with engine.connect() as connection:
            print(f"connection to db using engine ok")
        return engine
    except Exception as e:
        print(f"error during engine creation: {e}")
        return None


def main():

    engine = create_db_engine()
    if engine:
        print('db engine is ready to work')

if __name__ == "__main__":
    main()
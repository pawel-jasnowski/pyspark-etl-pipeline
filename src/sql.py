# src/sql.py
import os
# from datetime import datetime

import psycopg2 #type: ignore
from dotenv import load_dotenv

load_dotenv()

def create_db_connection():

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    if not all([db_user, db_password, db_host, db_port, db_name]):
        raise ValueError(f"ERROR during loading data from .env file")

    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )

        return conn
    except Exception as e:
        print(f"error during engine creation: {e}")
        return None


def main():

    try:
        conn = create_db_connection()
        with conn.cursor() as cursor: # context manager
            cursor.execute(
                f"select * from transactions"
                f" where alert_reason ='High Risk Country Transaction'"
                f" and country_code ='AF';"
            )
            records = cursor.fetchall()
            for row in records:
                print(row)
            print("closing context manager")

            ############################ using parameters

            # query = """
            #                SELECT *
            #                FROM transactions
            #                WHERE alert_reason = %s
            #                AND country_code = %s;
            #            """
            # params = ("High Risk Country Transaction", "AF")
            #
            # cursor.execute(query, params)
            # # ---------------------
            #
            # records = cursor.fetchall()

            #########################################3

    except psycopg2.Error as e:
        print(f"error: {e}")
    finally:
        if conn:
            conn.close()
            print("closing connection to DB")


if __name__ == "__main__":
    main()

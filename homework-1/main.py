"""Скрипт для заполнения данными таблиц в БД Postgres."""
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def variables_for_convert_data_from_csv_to_sql() -> dict:
    """Get variables for converting data from csv to SQL."""

    load_dotenv()

    return {
        'user_postgres': os.getenv('USER_POSTGRES'),
        'password_postgres': os.getenv('PASSWORD_POSTGRES'),
        'path_customers_data': Path(Path(__file__).parent, 'north_data', 'customers_data.csv'),
        'path_employees_data': Path(Path(__file__).parent, 'north_data', 'employees_data.csv'),
        'path_orders_data': Path(Path(__file__).parent, 'north_data', 'orders_data.csv'),
        'query': """
                COPY %s FROM STDIN WITH
                CSV
                HEADER
                DELIMITER AS ','        
        """,
        'name_database_customers': 'customers',
        'name_database_employees': 'employees',
        'name_database_orders': 'orders'
    }


def connect_to_database():
    """Connecting to database."""

    return psycopg2.connect(
        host='localhost',
        database='north',
        user=variables_for_convert_data_from_csv_to_sql()['user_postgres'],
        password=variables_for_convert_data_from_csv_to_sql()['password_postgres']
    )


VARIABLES_CONV = variables_for_convert_data_from_csv_to_sql()
CONN = connect_to_database()


def convert_data_from_csv_to_sql(path_csv, query, database) -> None:
    """Convert data from csv to SQL database."""

    with CONN:
        with CONN.cursor() as cur:
            #  Read csv from a file
            with open(path_csv, newline='', encoding='utf-8') as f:
                cur.copy_expert(sql=query % database, file=f)


def main() -> None:
    """Run converting data from csv to SQL."""

    try:
        # Convert Customers data from csv to SQL database.
        convert_data_from_csv_to_sql(
            path_csv=VARIABLES_CONV['path_customers_data'],
            query=VARIABLES_CONV['query'],
            database=VARIABLES_CONV['name_database_customers']
        )

        # Convert Employees data from csv to SQL database.
        convert_data_from_csv_to_sql(
            path_csv=VARIABLES_CONV['path_employees_data'],
            query=VARIABLES_CONV['query'],
            database=VARIABLES_CONV['name_database_employees']
        )

        # Convert Orders data from csv to SQL database.
        convert_data_from_csv_to_sql(
            path_csv=VARIABLES_CONV['path_orders_data'],
            query=VARIABLES_CONV['query'],
            database=VARIABLES_CONV['name_database_orders']
        )

    finally:
        CONN.close()


if __name__ == '__main__':
    main()

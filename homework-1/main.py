"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def variables_for_convert_data_from_csv_to_sql():
    """Get variables for converting data from csv to SQL."""

    load_dotenv()

    return {
        'user_postgres': os.getenv('USER_POSTGRES'),
        'password_postgres': os.getenv('PASSWORD_POSTGRES'),
        'path_customers_data': Path(Path(__file__).parent, 'north_data', 'customers_data.csv'),
        'path_employees_data': Path(Path(__file__).parent, 'north_data', 'employees_data.csv'),
        'path_orders_data': Path(Path(__file__).parent, 'north_data', 'orders_data.csv'),
        'name_execute_customers_data': 'INSERT INTO customers VALUES (%s, %s, %s)',
        'name_execute_employees_data': 'INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)',
        'name_execute_orders_data': 'INSERT INTO orders VALUES (%s, %s, %s, %s, %s)',
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


def convert_customers_data_from_csv_to_sql(path_csv, name_execute: str) -> None:
    """Convert Customers data from csv to SQL database."""

    with CONN:
        with CONN.cursor() as cur:
            #  Read csv from a file
            with open(path_csv, newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # without title table
                for row in reader:
                    cur.execute(name_execute, (row[0], row[1], row[2]))


def convert_employees_data_from_csv_to_sql(path_csv, name_execute: str) -> None:
    """Convert Employees data from csv to SQL database."""

    with CONN:
        with CONN.cursor() as cur:
            #  Read csv from a file
            with open(path_csv, newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # without title table
                for row in reader:
                    cur.execute(name_execute, (row[0], row[1], row[2], row[3], row[4], row[5]))


def convert_orders_data_from_csv_to_sql(path_csv, name_execute: str) -> None:
    """Convert Orders data from csv to SQL database."""

    with CONN:
        with CONN.cursor() as cur:
            #  Read csv from a file
            with open(path_csv, newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # without title table
                for row in reader:
                    cur.execute(name_execute, (row[0], row[1], row[2], row[3], row[4]))


def main() -> None:
    """Run converting data from csv to SQL."""

    try:
        convert_customers_data_from_csv_to_sql(
            path_csv=VARIABLES_CONV['path_customers_data'],
            name_execute=VARIABLES_CONV['name_execute_customers_data']
        )

        convert_employees_data_from_csv_to_sql(
            path_csv=VARIABLES_CONV['path_employees_data'],
            name_execute=VARIABLES_CONV['name_execute_employees_data']
        )

        convert_orders_data_from_csv_to_sql(
            path_csv=VARIABLES_CONV['path_orders_data'],
            name_execute=VARIABLES_CONV['name_execute_orders_data']
        )

    finally:
        CONN.close()


if __name__ == '__main__':
    main()

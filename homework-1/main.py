"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

path_customers_data = Path(Path(__file__).parent, 'north_data', 'customers_data.csv')
path_employees_data = Path(Path(__file__).parent, 'north_data', 'employees_data.csv')
path_orders_data = Path(Path(__file__).parent, 'north_data', 'orders_data.csv')

name_execute_customers_data = 'INSERT INTO customers VALUES (%s, %s, %s)'
name_execute_employees_data = 'INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)'
name_execute_orders_data = 'INSERT INTO orders VALUES (%s, %s, %s, %s, %s)'


def convert_customers_data_from_csv_to_sql(path_csv, name_execute: str) -> None:
    """Convert Customers data from csv to SQL database."""

    load_dotenv()
    user_postgres = os.getenv('USER_POSTGRES')
    password_postgres = os.getenv('PASSWORD_POSTGRES')

    conn = psycopg2.connect(
        host='localhost',
        database='north',
        user=user_postgres,
        password=password_postgres
    )
    try:
        with conn:
            with conn.cursor() as cur:
                #  Read csv from a file
                with open(path_csv, newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader)  # without title table
                    for row in reader:
                        cur.execute(name_execute, (row[0], row[1], row[2]))
    finally:
        conn.close()


def convert_employees_data_from_csv_to_sql(path_csv, name_execute: str) -> None:
    """Convert Employees data from csv to SQL database."""

    load_dotenv()
    user_postgres = os.getenv('USER_POSTGRES')
    password_postgres = os.getenv('PASSWORD_POSTGRES')

    conn = psycopg2.connect(
        host='localhost',
        database='north',
        user=user_postgres,
        password=password_postgres
    )
    try:
        with conn:
            with conn.cursor() as cur:
                #  Read csv from a file
                with open(path_csv, newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader)  # without title table
                    for row in reader:
                        cur.execute(name_execute, (row[0], row[1], row[2], row[3], row[4], row[5]))
    finally:
        conn.close()


def convert_orders_data_from_csv_to_sql(path_csv, name_execute: str) -> None:
    """Convert Orders data from csv to SQL database."""

    load_dotenv()
    user_postgres = os.getenv('USER_POSTGRES')
    password_postgres = os.getenv('PASSWORD_POSTGRES')

    conn = psycopg2.connect(
        host='localhost',
        database='north',
        user=user_postgres,
        password=password_postgres
    )
    try:
        with conn:
            with conn.cursor() as cur:
                #  Read csv from a file
                with open(path_csv, newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader)  # without title table
                    for row in reader:
                        cur.execute(name_execute, (row[0], row[1], row[2], row[3], row[4]))
    finally:
        conn.close()


if __name__ == '__main__':
    convert_customers_data_from_csv_to_sql(
        path_csv=path_customers_data,
        name_execute=name_execute_customers_data
    )

    convert_employees_data_from_csv_to_sql(
        path_csv=path_employees_data,
        name_execute=name_execute_employees_data
    )

    convert_orders_data_from_csv_to_sql(
        path_csv=path_orders_data,
        name_execute=name_execute_orders_data
    )

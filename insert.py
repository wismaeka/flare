# insert raw data into airflow

import psycopg2

def write_to_postgresql():
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="localhost",
        port="5434"
    )
    cur = conn.cursor()

    # CSV to Postgresql
    with open('data_raw.csv', 'r') as f:
        cur.copy_expert("COPY table_m3 (ship_mode, segment, Country, City, State, postal_code, Region, Category, sub_category, Sales, Quantity, Discount, Profit) FROM STDIN WITH CSV HEADER", f)

    conn.commit()
    cur.close()
    conn.close()


print(write_to_postgresql())
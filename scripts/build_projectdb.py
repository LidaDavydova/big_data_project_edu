"""
Project database initialization script.

Creates schema, imports data, populates tables, and runs validation queries.
"""
import os
from pprint import pprint
import glob

import psycopg2 as psql

csv_parts = sorted(glob.glob("data/anac_brazil_sample.csv/part-*"))

# Read password from secrets file
file = os.path.join("secrets", ".psql.pass")
with open(file, "r", encoding="utf-8") as file:
    password=file.read().rstrip()

# build connection string
conn_string = (
    "host=hadoop-04.uni.innopolis.ru "
    "port=5432 "
    "user=team12 "
    "dbname=team12_projectdb "
    f"password={password}"
)

# Connect to the remote dbms
with psql.connect(conn_string) as conn:
    # Create a cursor for executing psql commands
    cur = conn.cursor()
    # Read the commands from the file and execute them.
    with open(os.path.join("sql","create_tables.sql"), 'r', encoding="utf-8") as file:
        content = file.read()
        cur.execute(content)
    conn.commit()
                
    # Read the commands from the file and execute them.
    with open(os.path.join("sql", "import_data.sql"), "r", encoding="utf-8") as f:
        copy_sql = f.read()

    for part_file in csv_parts:
        with open(part_file, "r", encoding="utf-8") as data:
            cur.copy_expert(copy_sql, data)

    # If the sql statements are CRUD then you need to commit the change
    conn.commit()

    pprint(conn)
    # Read the sql commands from the file
    with open(os.path.join("sql", "test_database.sql"), 'r', encoding="utf-8") as file:
        commands = file.readlines()
        for command in commands:
            cur.execute(command)
            # Read all records and print them
            pprint(cur.fetchall())
            
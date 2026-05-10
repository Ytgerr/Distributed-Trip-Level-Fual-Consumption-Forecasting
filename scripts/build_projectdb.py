"""
Stage I - Builds main schema in psql
"""

from pathlib import Path
from pprint import pprint
import os

import psycopg2 as psql
from tqdm import tqdm


# Read password from secrets file
file = os.path.join("secrets", ".psql.pass")
with open(file, "r", encoding="utf-8") as file:
    password = file.read().rstrip()

# build connection string
CONN_STRING = f"host=hadoop-04.uni.innopolis.ru port=5432 user=team15 dbname=team15_projectdb password={password}"

with psql.connect(CONN_STRING) as conn:
    # Create a cursor for executing psql commands
    cur = conn.cursor()
    # Read the commands from the file and execute them.
    with open(os.path.join("sql", "create_tables.sql"), encoding="utf-8") as file:
        content = file.read()
        cur.execute(content)
    conn.commit()

    # Read the commands from the file and execute them.
    with open(os.path.join("sql", "import_data.sql"), encoding="utf-8") as file:
        # We assume that the COPY commands in the file are ordered (1.depts, 2.emps)
        print("Ingesting Vehicles...")
        commands = file.readlines()
        with open(
            os.path.join("data", "vehicles.csv"), "r", encoding="utf-8"
        ) as vehicles:
            cur.copy_expert(commands[0], vehicles)
        print("Vehicles Ingested")

        trips_files = list(Path("data").glob("*_week.csv"))

        print("Ingesting Trips...")
        for trips_file in tqdm(trips_files):
            with open(trips_file, "r", encoding="utf-8") as trips:
                cur.copy_expert(commands[1], trips)
        print("Trips Ingested")

    # If the sql statements are CRUD then you need to commit the change
    conn.commit()

    print("Current Connection Object:")
    pprint(conn)
    cur = conn.cursor()

    print("Database check..")
    # Read the sql commands from the file
    with open(os.path.join("sql", "test_database.sql"), encoding="utf-8") as file:
        commands = file.readlines()
        for command in commands:
            cur.execute(command)
            # Read all records and print them
            pprint(cur.fetchall())

    print("All Done correctly!")

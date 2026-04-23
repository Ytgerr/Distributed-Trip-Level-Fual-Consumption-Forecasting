import psycopg2 as psql
from pprint import pprint
import os


# file = os.path.join("secrets", ".psql.pass")
# with open(file, "r") as file:
#         password=file.read().rstrip()


# conn_string="host=hadoop-04.uni.innopolis.ru port=5432 user=teamx dbname=teamx_projectdb password={}".format(pass)

conn_string = "host=localhost user=postgres port=5433 dbname=postgres password=password"

with psql.connect(conn_string) as conn:
    # Create a cursor for executing psql commands
    cur = conn.cursor()
    # Read the commands from the file and execute them.
    with open(os.path.join("sql", "create_tables.sql")) as file:
        content = file.read()
        cur.execute(content)
    conn.commit()

    # Read the commands from the file and execute them.
    with open(os.path.join("sql", "import_data.sql")) as file:
        # We assume that the COPY commands in the file are ordered (1.depts, 2.emps)
        commands = file.readlines()
        with open(os.path.join("data", "vehicles.csv"), "r") as depts:
            cur.copy_expert(commands[0], depts)
        print("Vehicles Ingested")
        with open(os.path.join("data", "trips.csv"), "r") as emps:
            cur.copy_expert(commands[1], emps)
        print("Trips Ingested")

    # If the sql statements are CRUD then you need to commit the change
    conn.commit()

    pprint(conn)
    cur = conn.cursor()
    # Read the sql commands from the file
    with open(os.path.join("sql", "test_database.sql")) as file:
        commands = file.readlines()
        for command in commands:
            cur.execute(command)
            # Read all records and print them
            pprint(cur.fetchall())

import asyncio
import random
import sys
import time
import psycopg2

def get_db_connection():
    return psycopg2.connect(user="grahamlewis",
                            password="",
                            host="127.0.0.1",
                            port="5432",
                            database="single_db_conn_investigation")

def initial_db_setup():
    print("Setting up database...")
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        cursor.execute("""CREATE TABLE IF NOT EXISTS message_store (
                              id SERIAL PRIMARY KEY,
                              session_id TEXT NOT NULL,
                              message TEXT NOT NULL,
                              message_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
                          );""")
        connection.commit()

        cursor.execute("DELETE FROM message_store;")
        connection.commit()

        for _ in range(1, 12):
            cursor.execute(f"""INSERT INTO message_store (session_id, message)
                               VALUES (%s, 'Hello World');""", (f"ABC-{random.randint(1, 4)}",))
            connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
    print("Database setup complete!")

async def request(thread_id):
    print(f"{thread_id} Started")
    start_time = time.perf_counter()
    session_id = f"ABC-{thread_id}"

    print("Getting database connection...")
    try:
        connection = get_db_connection()
        print(
            f"Database connection {id(connection)} : {sys.getsizeof(connection)} bytes")

        await asyncio.sleep(random.randint(4, 15))

        cursor = connection.cursor()
        cursor.execute(f"""SELECT session_id, message
                           FROM message_store
                           WHERE session_id = %s
                           ORDER BY id;""", (session_id,))
        print(cursor.fetchall())
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        end_time = time.perf_counter() - start_time
        print(f"{thread_id} Finished - {end_time:0.2f} seconds")

        if (connection):
            print(f"Cleaning up database connection {id(connection)}")
            cursor.close()
            connection.close()

async def main():
    initial_db_setup()

    await asyncio.gather(
        request(1),
        request(2),
        request(3),
        request(4)
    )

if __name__ == "__main__":
    asyncio.run(main())

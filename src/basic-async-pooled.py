import asyncio
import random
import sys
import time
import psycopg2
from psycopg2.pool import SimpleConnectionPool

# def get_db_connection():
#     return psycopg2.connect(user="grahamlewis",
#                             password="",
#                             host="127.0.0.1",
#                             port="5432",
#                             database="single_db_conn_investigation")

def initial_db_setup(pool):
    print("Setting up database...")
    try:
        connection = pool.getconn()
        print(
            f"> Checking out db_conn {id(connection)} : {sys.getsizeof(connection)} bytes")
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
            print(
                f"< Checking in db_conn {id(connection)} : {sys.getsizeof(connection)} bytes")
            pool.putconn(connection)
    print("Database setup complete!")


async def request(thread_id, pool):
    print(f"{thread_id} Started")

    start_time = time.perf_counter()
    session_id = f"ABC-{thread_id}"

    print("Getting database connection...")
    connection = pool.getconn()

    try:
        print(
            f"> Checking out db_conn {id(connection)} : {sys.getsizeof(connection)} bytes")

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
            cursor.close()
            print(
                f"< Checking in db_conn {id(connection)} : {sys.getsizeof(connection)} bytes")
            pool.putconn(connection)


async def main():
    db_url = "postgres://grahamlewis:@127.0.0.1:5432/single_db_conn_investigation"
    pool = SimpleConnectionPool(minconn = 1, maxconn = 2, dsn = db_url)
    print(f"Pool : {sys.getsizeof(pool)} bytes")
    # print(f"Stats : {pool.get_stats()}")

    initial_db_setup(pool)

    await asyncio.gather(
        request(1, pool),
        request(2, pool)
    )

    await asyncio.sleep(15)

    await asyncio.gather(
        request(3, pool),
        request(4, pool)
    )

if __name__ == "__main__":
    asyncio.run(main())

import psycopg2
import threading
import time


db_params = {
    "dbname": "lab1",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}


def reset_counter():
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    cursor.execute("UPDATE user_counter SET counter = 0 WHERE user_id = 1")
    connection.commit()
    cursor.close()
    connection.close()


def get_counter():
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
    counter = cursor.fetchone()[0]
    cursor.close()
    connection.close()
    return counter


def lost_update():
    reset_counter()
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    for _ in range(10000):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
        counter = cursor.fetchone()[0] + 1
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (counter,))
        connection.commit()
    cursor.close()
    connection.close()



def in_place_update():
    reset_counter()
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    for _ in range(10000):
        cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1")
        connection.commit()
    cursor.close()
    connection.close()



def row_level_locking():
    reset_counter()
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    for _ in range(10000):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
        counter = cursor.fetchone()[0] + 1
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (counter,))
        connection.commit()
    cursor.close()
    connection.close()



def optimistic_concurrency_control():
    reset_counter()
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    for _ in range(10000):
        while True:
            cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
            counter, version = cursor.fetchone()
            counter += 1
            cursor.execute("UPDATE user_counter SET counter = %s, version = %s WHERE user_id = 1 AND version = %s", (counter, version + 1, version))
            connection.commit()
            if cursor.rowcount > 0:
                break
    cursor.close()
    connection.close()


def run_test(func, threads=10):
    start_time = time.time()
    thread_list = []
    for _ in range(threads):
        t = threading.Thread(target=func)
        t.start()
        thread_list.append(t)
    for t in thread_list:
        t.join()
    end_time = time.time()
    print(f"{func.__name__} execution time: {end_time - start_time:.2f} seconds")
    print(f"{func.__name__}: counter = {get_counter()}")

if __name__ == "__main__":
    run_test(lost_update)
    run_test(in_place_update)
    run_test(row_level_locking)
    run_test(optimistic_concurrency_control)





def reset_versions():
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    cursor.execute("UPDATE user_counter SET version = 0")
    connection.commit()
    cursor.close()
    connection.close()
reset_versions()
import psycopg2 as ps
import time
import random
import threading

# Параметри підключення до БД
DB_PARAMS = {
    "dbname": "counter",
    "user": "postgres",
    "password": "adopted29v",
    "host": "localhost",
    "port": "5432",
}
USER_ID = 1  # Ідентифікатор користувача
NUM_UPDATES = 100000  # Кількість оновлень каунтера

# Створення 10 потоків для взаємодії з базою даних
def threads_func(base):
    start_time = time.time()  # Початок вимірювання часу
    threads = []
    thread_times = []

    def thread_target():
        thread_start_time = time.time()
        base()
        thread_end_time = time.time()
        thread_times.append(thread_end_time - thread_start_time)

    for i in range(10):
        thread = threading.Thread(target=thread_target)
        threads.append(thread)
        thread.start()

    # Очікування завершення всіх потоків
    for thread in threads:
        thread.join()

    end_time = time.time()  # Кінець вимірювання часу
    print(f"Загальний час виконання {base.__name__}: {end_time - start_time:.4f} секунд")
    for i, thread_time in enumerate(thread_times):
        print(f"Час виконання потоку {i+1}: {thread_time:.4f} секунд")

def lost_update():
    conn = ps.connect(**DB_PARAMS)
    cursor = conn.cursor()

    for _ in range(NUM_UPDATES):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s", (USER_ID,))
        counter = cursor.fetchone()[0]  # Отримуємо поточне значення
        counter += 1  # Збільшуємо на 1

        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, USER_ID))
        conn.commit()  # Фіксуємо зміни

    cursor.close()
    conn.close()

def in_place_update():
    conn = ps.connect(**DB_PARAMS)
    cursor = conn.cursor()

    for _ in range(NUM_UPDATES):
        cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1")
        conn.commit()  # Фіксуємо зміни

def row_level_locking():
    conn = ps.connect(**DB_PARAMS)
    cursor = conn.cursor()

    for _ in range(NUM_UPDATES):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
        counter = cursor.fetchone()[0]
        counter = counter + 1
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, USER_ID))
        conn.commit()

def optimistic_concurrency_control():
    conn = ps.connect(**DB_PARAMS)
    cursor = conn.cursor()

    for _ in range(NUM_UPDATES):
        while True:
            cursor.execute("SELECT counter,version FROM user_counter WHERE user_id = %s",(USER_ID,))
            counter, version = cursor.fetchone()
            counter = counter + 1
            cursor.execute("UPDATE user_counter SET counter = %s, version = %s WHERE user_id = %s AND version = %s",(counter,version+1,1,version))
            conn.commit()
            count = cursor.rowcount
            if count > 0:
                break


"""
На додаткові бали можна ще перевірити різницю в часі виконання операцій,
якщо в таблиці буде, нехай, 100 000 рядків, апдейт виконуватиметься то першого,
то рендомного (довільного не однакового) рядка. 
Яка буде різниця в часі виконання всіх запитів та у кількості лайків.
Ця перевірка стосується пошуку за кластерним індексом.
"""

def random_update():
    conn = ps.connect(**DB_PARAMS)
    cursor = conn.cursor()

    for _ in range(NUM_UPDATES):
        random_id = random.randint(1, 100000)  # Випадковий user_id
        cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s", (random_id,))
        conn.commit()

if __name__ == "__main__":
 # threads_func(lost_update)
 # threads_func(in_place_update)
 # threads_func(row_level_locking)
  threads_func(optimistic_concurrency_control)
 # threads_func(random_update)








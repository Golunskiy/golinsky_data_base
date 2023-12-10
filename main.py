import psycopg2
import time
import sys

USER = 'postgres'
PASSWORD = 'postgres'
DBNAME = 'DB'
MAXIMUM_NUMBER_TO_CONNECT = 10  # "INF" щоб повторювати запит необмежену кількість разів
MAXIMUM_NUMBER_OF_LINES = 1000  # "INF" якщо максимальну кількість рядків
FILE_PATH = r'Odata2019File.csv'


def connect_to_DB(max_iter=MAXIMUM_NUMBER_TO_CONNECT):
    print("Trying to connect to the database")
    iter = 0
    while True:
        iter += 1
        try:
            connect = psycopg2.connect(user=USER, password=PASSWORD, dbname=DBNAME)
            cursor = connect.cursor()
            print('Connect successful\n')
            return connect, cursor

        except:
            print('Connecting, please wait a moment..')
            time.sleep(5)

        if iter == max_iter and max_iter != "INF":
            print("The database is currently unavailable")
            sys.exit()


def create_table(connect, cursor, column_names):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS data2019(
            %s text PRIMARY KEY
        );
        """ % column_names[0]
    )

    for col in column_names[1:]:
        if 'ball100' in col:
            cursor.execute('ALTER TABLE data2019 ADD COLUMN %s float' % col)
        elif 'ball' in col or 'birth' in col or 'scale' in col:
            cursor.execute('ALTER TABLE data2019 ADD COLUMN %s int' % col)
        else:
            cursor.execute('ALTER TABLE data2019 ADD COLUMN %s text' % col)

    connect.commit()
    print('Table is created\n')


def delete_table(connect, cursor):
    cursor.execute(
        """ DROP TABLE IF EXISTS data2019; """
    )
    connect.commit()


def convert_to_number(value):
    if ',' in value or '.' in value:
        try:
            # Перевірка на число з комою
            number = float(value.replace(',', '.'))
            return number
        except ValueError:
            pass
    else:
        try:
            # Перевірка на ціле число
            number = int(value)
            return number
        except ValueError:
            pass

    return value


def insert_with_error_checking(connect, cursor, sql_query, values):
    try:
        cursor.execute(sql_query, values)
        connect.commit()
        return connect, cursor
    except:
        print('DB error')

        connect, cursor = connect_to_DB()

    cursor.execute(sql_query, values)
    connect.commit()
    return connect, cursor


def fill_table(connect, cursor, file, column_names, max_len=MAXIMUM_NUMBER_OF_LINES):
    cursor.execute("DELETE FROM data2019")

    print("Loading:")
    for i, values in enumerate(file):
        if i == max_len and max_len != "INF":
            break

        if i % 200 == 0 and i != 0:
            print(f"Loaded {i} rows")

        values = values.replace('"', '').split(';')
        values[-1] = values[-1][:-1]  # треба прибрати \n вкінці

        # Заміна 'null' на None та перетворення чисел на відповідний тип
        values = [None if val.lower() == 'null' else convert_to_number(val) for val in values]
        sql_query = f"INSERT INTO data2019 ({', '.join(column_names)}) VALUES ({', '.join(['%s'] * len(values))})"

        # Записуємо у БД
        connect, cursor = insert_with_error_checking(connect, cursor, sql_query, values)

    print(f"Loaded {i} rows")
    return connect, cursor


def main():
    connect, cursor = connect_to_DB()

    file = open(FILE_PATH, 'r')
    column_names = file.readline().lower().replace('"', '').split(';')

    delete_table(connect, cursor)
    create_table(connect, cursor, column_names)

    # Повертаємо connect, cursor на майбутнє, бо вони могли змінитись
    connect, cursor = fill_table(connect, cursor, file, column_names)


if __name__ == "__main__":
    main()

import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

# Load biến môi trường từ .env file
load_dotenv()

def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name,
            charset='utf8mb4',  # Sử dụng mã hóa UTF-8
            use_unicode=True
        )
        cursor = connection.cursor()
        cursor.execute("SET NAMES 'utf8mb4';")
        cursor.execute("SET CHARACTER SET utf8mb4;")
        cursor.execute("SET character_set_connection=utf8mb4;")
        print("Kết nối MySQL thành công với mã hóa UTF-8")
    except Error as e:
        print(f"Không thể kết nối MySQL: {e}")
    return connection

def update_erc_by_month(connection, month, year):
    try:
        cursor = connection.cursor(dictionary=True)
        select_query = f"SELECT Id, ERC FROM XK WHERE MONTH(DATE) = {month} AND YEAR(DATE) = {year} AND LENGTH(ERC) = 9"
        cursor.execute(select_query)

        rows = cursor.fetchall()
        if not rows:
            print(f"No data found for month {month}, year {year}")
            return

        count = 0
        for row in rows:
            id_value = row['Id']
            erc_value = row['ERC']
            new_erc_value = '0' + erc_value

            update_query = f"UPDATE XK SET ERC = '{new_erc_value}' WHERE Id = {id_value}"
            cursor.execute(update_query)

            count += 1
            if count % 100 == 0:  # Commit sau mỗi 100 dòng
                connection.commit()
                print(f"Committed after {count} records")

        connection.commit()  # Commit lần cuối sau khi cập nhật xong
        print(f"Updated {count} records for month {month}, year {year}")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()


def update_data_by_year(connection, year):
    # Cập nhật từng tháng trong năm 2023
    for month in range(1, 13):
        print(f"Processing month {month} of year {year}")
        update_erc_by_month(connection, month, year)

# Thêm hàm để xóa dữ liệu của tháng 4 và tháng 10
def delete_data_for_months(connection, year, months):
    try:
        cursor = connection.cursor()
        for month in months:
            delete_query = f"DELETE FROM XK WHERE MONTH(DATE) = {month} AND YEAR(DATE) = {year}"
            cursor.execute(delete_query)
            connection.commit()
            print(f"Deleted records for month {month}, year {year}")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()

# Sử dụng hàm kết nối và thực hiện update theo tháng
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

connection = create_connection(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)

if connection:
    # update_data_by_year(connection, 2023)  # Chạy cập nhật cho năm 2023

    # Xóa dữ liệu của tháng 4 và tháng 10
    delete_data_for_months(connection, 2023, [4, 10])

    # Đóng kết nối sau khi hoàn thành
    if connection.is_connected():
        connection.close()
        print("MySQL connection is closed")

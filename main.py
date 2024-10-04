import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os
import tkinter as tk
from tkinter import filedialog
import threading
from tqdm import tqdm
from tkinter import ttk

load_dotenv()

# Hàm tạo kết nối MySQL
def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name,
            allow_local_infile=True,  # Bật cho phép LOAD DATA LOCAL
            charset='utf8mb4',  # Thêm tham số để sử dụng UTF-8
            use_unicode=True  # Đảm bảo MySQL xử lý đúng kiểu Unicode
        )
        cursor = connection.cursor()
        cursor.execute("SET NAMES 'utf8mb4';")
        cursor.execute("SET CHARACTER SET utf8mb4;")
        cursor.execute("SET character_set_connection=utf8mb4;")
        print("Kết nối MySQL thành công với mã hóa UTF-8")
    except Error as e:
        print(f"Không thể kết nối MySQL: {e}")
    return connection

# Hàm chuyển đổi Excel sang CSV
def convert_excel_to_csv(file_path):
    df = pd.read_excel(file_path)
    csv_file_path = file_path.replace('.xlsx', '.csv').replace('.xls', '.csv')
    df.to_csv(csv_file_path, index=False, encoding='utf-8')
    return csv_file_path

# Hàm nhập dữ liệu vào MySQL bằng Bulk Insert
def bulk_insert_from_csv(connection, csv_file_path, progress_label, progress_bar):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)

    # Add leading zero to ERC if its length is 9
    df['ERC'] = df['ERC'].apply(lambda x: f'0{x}' if len(str(x)) == 9 else x)

    # Save the modified DataFrame back to the CSV file
    df.to_csv(csv_file_path, index=False, encoding='utf-8')

    cursor = connection.cursor()
    insert_query = f"""
    LOAD DATA LOCAL INFILE '{csv_file_path}'
    INTO TABLE XK
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '\"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (CDS, CustomsOffice, C1, C2, C3, TPSMode, TradeType, DATE, HOUR, 
    DateUpdated, HourUpdated, ERC, Exportor, Importor, ImportCountry, 
    BLNumber, Quantity, UOM, GWeight, WeightUOM, FinalDestination, 
    POL, Value, TaxableValue, TaxValue, CDSLine, Note, 
    CDSCompletedDate, CDSCompletedHour, CDSCancelDate, CDSCancelHour, 
    Officer, Officer2, HSCode, Commodity, UnitQuantity, 
    UnitCost, InvoiceBL, Currency, InvoiceValue, TaxableValue2, 
    TaxUnit, TaxRate, TaxClass, Tax, RefDoc1, RefDoc2, CreatedDate)
    """
    
    cursor.execute(insert_query)
    connection.commit()
    print(f"Dữ liệu đã được nhập thành công từ {csv_file_path}")

# Hàm chọn file Excel và bắt đầu import
def start_import():
    filenames = filedialog.askopenfilenames(title="Chọn tệp Excel", filetypes=[("Excel files", "*.xlsx *.xls")])
    if filenames:
        progress_label['text'] = "Đang xử lý..."  =
        progress_bar['value'] = 0  
        
        threading.Thread(target=process_files, args=(filenames,)).start()

# Hàm xử lý nhiều tệp Excel
def process_files(filenames):
    DB_HOST = os.getenv('DB_HOST')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')
    print(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)

    connection = create_connection(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)

    if connection:
        total_files = len(filenames)
        for idx, file in enumerate(filenames, start=1):
            csv_file_path = convert_excel_to_csv(file)  
            bulk_insert_from_csv(connection, csv_file_path, progress_label, progress_bar)
            progress_bar['value'] = (idx / total_files) * 100  
        progress_label['text'] = f"Đã thêm {total_files} tệp thành công!"  
        progress_bar['value'] = 100 

# Tạo giao diện chính bằng Tkinter
root = tk.Tk()
root.title("Import dữ liệu từ Excel")

# Nút chọn tệp
select_files_btn = tk.Button(root, text="Chọn tệp Excel", command=start_import)
select_files_btn.pack(pady=10)

# Nhãn hiển thị tiến trình
progress_label = tk.Label(root, text="Chưa bắt đầu")
progress_label.pack(pady=10)

# Thanh tiến trình
progress_bar = ttk.Progressbar(root, orient="horizontal", length=300, mode="determinate")
progress_bar.pack(pady=10)

# Chạy giao diện
root.mainloop()

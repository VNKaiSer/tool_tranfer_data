import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os
import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox 
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
            allow_local_infile=True,
            charset='utf8mb4',
            use_unicode=True
        )
        cursor = connection.cursor()
        cursor.execute("SET NAMES 'utf8mb4';")
        cursor.execute("SET CHARACTER SET utf8mb4;")
        cursor.execute("SET character_set_connection=utf8mb4;")
        print("Kết nối MySQL thành công với mã hóa UTF-8")
    except Error as e:
        messagebox.showerror("Lỗi kết nối MySQL", f"Không thể kết nối MySQL: {e}")
    return connection

# Hàm chuyển đổi Excel sang CSV
def convert_excel_to_csv(file_path):
    try:
        df = pd.read_excel(file_path)
        csv_file_path = file_path.replace('.xlsx', '.csv').replace('.xls', '.csv')
        df.to_csv(csv_file_path, index=False, encoding='utf-8')
        return csv_file_path
    except Exception as e:
        messagebox.showerror("Lỗi chuyển đổi", f"Lỗi khi chuyển đổi tệp Excel sang CSV: {e}")
        return None

# Kiểm tra thứ tự cột
def validate_columns(df, expected_columns):
    file_columns = list(df.columns)
    
    # Kiểm tra cột thiếu
    missing_columns = set(expected_columns) - set(file_columns)
    extra_columns = set(file_columns) - set(expected_columns)
    
    if missing_columns or extra_columns:
        error_message = "Thứ tự cột không khớp.\n"
        if missing_columns:
            error_message += f"Các cột thiếu: {', '.join(missing_columns)}\n"
        if extra_columns:
            error_message += f"Các cột thừa: {', '.join(extra_columns)}"
        
        messagebox.showerror("Lỗi cột", error_message)
        return False
        
    return True

def bulk_insert_from_csv(connection, csv_file_path, progress_label, progress_bar):
    try:
        df = pd.read_csv(csv_file_path, low_memory=False)

        # Kiểm tra thứ tự cột
        expected_columns = [
            "CDS", "CustomsOffice", "C1", "C2", "C3", "TPSMode", "TradeType", 
            "Date", "Hour", "DateUpdated", "HourUpdated", "ERC", "Importer", 
            "ImporterPhone", "ImporterNotify", "Exporter", "EmportCountry", 
            "BL", "BL2", "BL3", "BL4", "BL5", "Quantity", "QuantityUom", 
            "GWeight", "WeightUom", "NumberContainer", "POD", "POL", 
            "TransportName", "ArrivalDate", "PaymentMethod", "InvoiceValue", 
            "LocalInvoiceValue", "TaxValue", "CdsLine", "PermitDate", 
            "PermitHour", "CdsCompletedDate", "CompletedHour", "CdsCancelDate", 
            "CancelHour", "Officer", "Officer2", "HSCode", "Commodity", 
            "Quantity2", "QuantityUom2", "InvoiceDetails", "UnitCost", 
            "Curr", "CurrQuantityUom", "InvoiceValueVnd", "TaxableValue", 
            "TaxUnit", "TaxRate", "TaxValue2", "OriginCountry", "DocRef", 
            "ImportClass", "ImportTaxCode", "TaxIncentiveCode"
        ]

        
        if not validate_columns(df, expected_columns):
            progress_label['text'] = "Thứ tự cột không đúng. Vui lòng chọn lại file."
            return False  # Trả về False khi không hợp lệ

        # Add leading zero to ERC if its length is 9
        df['ERC'] = df['ERC'].apply(lambda x: f'0{x}' if len(str(x)) == 9 else x)

        # Ghi lại CSV sau khi sửa đổi
        df.to_csv(csv_file_path, index=False, encoding='utf-8')

        cursor = connection.cursor()
        insert_query = f"""
        LOAD DATA LOCAL INFILE '{csv_file_path}'
        INTO TABLE NK
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '\"'
        LINES TERMINATED BY '\n'
        IGNORE 1 ROWS
        (CDS, CustomsOffice, C1, C2, C3, TPSMode, TradeType, 
        Date, Hour, DateUpdated, HourUpdated, ERC, Importer, 
        ImporterPhone, ImporterNotify, Exporter, EmportCountry, 
        BL, BL2, BL3, BL4, BL5, Quantity, QuantityUom, 
        GWeight, WeightUom, NumberContainer, POD, POL, 
        TransportName, ArrivalDate, PaymentMethod, InvoiceValue, 
        LocalInvoiceValue, TaxValue, CdsLine, PermitDate, 
        PermitHour, CdsCompletedDate, CompletedHour, CdsCancelDate, 
        CancelHour, Officer, Officer2, HSCode, Commodity, 
        Quantity2, QuantityUom2, InvoiceDetails, UnitCost, 
        Curr, CurrQuantityUom, InvoiceValueVnd, TaxableValue, 
        TaxUnit, TaxRate, TaxValue2, OriginCountry, DocRef, 
        ImportClass, ImportTaxCode, TaxIncentiveCode);
        """
        cursor.execute(insert_query)
        connection.commit()
        print(f"Dữ liệu đã được nhập thành công từ {csv_file_path}")
        return True  # Trả về True khi nhập thành công
    except Error as e:
        messagebox.showerror("Lỗi khi nhập dữ liệu", f"Lỗi khi import dữ liệu: {e}")
        connection.rollback()  # Rollback nếu gặp lỗi
        progress_label['text'] = "Lỗi khi nhập dữ liệu."  # Cập nhật nhãn với thông báo lỗi
        return False  # Trả về False khi gặp lỗi
    except Exception as e:
        messagebox.showerror("Lỗi không xác định", f"Lỗi không xác định: {e}")
        return False  # Trả về False khi gặp lỗi không xác định



# Hàm chọn file Excel và bắt đầu import
def start_import():
    filenames = filedialog.askopenfilenames(title="Chọn tệp Excel", filetypes=[("Excel files", "*.xlsx *.xls")])
    if filenames:
        progress_label['text'] = "Đang xử lý..."
        progress_bar['value'] = 0  
        
        threading.Thread(target=process_files, args=(filenames,)).start()

# Hàm xử lý nhiều tệp Excel
def process_files(filenames):
    DB_HOST = os.getenv('DB_HOST')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')

    connection = create_connection(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)

    if connection:
        total_files = len(filenames)
        for idx, file in enumerate(filenames, start=1):
            csv_file_path = convert_excel_to_csv(file)  
            if csv_file_path:  # Kiểm tra nếu chuyển đổi thành công
                bulk_insert_from_csv(connection, csv_file_path, progress_label, progress_bar)
                progress_bar['value'] = (idx / total_files) * 100  
                connection.commit()  # Commit sau khi xử lý mỗi tệp
            else:
                messagebox.showerror("Lỗi chuyển đổi", f"Có lỗi xảy ra khi chuyển đổi tệp Excel sang CSV: {file}")
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

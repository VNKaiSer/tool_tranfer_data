import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

# Kết nối tới cơ sở dữ liệu MySQL
db_connection = mysql.connector.connect(
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    database=os.getenv('DB_NAME'),
    allow_local_infile=True  # Cho phép sử dụng LOAD DATA LOCAL INFILE
)

cursor = db_connection.cursor()

# Đường dẫn tới tệp CSV
csv_file_path = '/path/to/your/file.csv'

# Chuẩn bị câu lệnh LOAD DATA LOCAL INFILE
sql_insert_query = f"""
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

# Thực hiện chèn dữ liệu
cursor.execute(sql_insert_query)

# Commit thay đổi
db_connection.commit()

# Đóng kết nối
cursor.close()
db_connection.close()

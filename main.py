import pandas as pd
import mysql.connector
from mysql.connector import Error

def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Kết nối MySQL thành công")
    except Error as e:
        print(f"Không thể kết nối MySQL: {e}")
    return connection

import pandas as pd
import numpy as np

def insert_data(connection, data):
    cursor = connection.cursor()
    if 'TaxRate' in data.columns:
        data['TaxRate'] = data['TaxRate'].str.replace('%', '', regex=False).astype(float)
    insert_query = """
    INSERT INTO CustomsDeclaration (
        CDS, CustomsOffice, C1, C2, C3, TPSMode, TradeType, DATE, HOUR, 
        DateUpdated, HourUpdated, ERC, Exportor, Importor, ImportCountry, 
        BLNumber, Quantity, UOM, GWeight, WeightUOM, FinalDestination, 
        POL, Value, TaxableValue, TaxValue, CDSLine, Note, 
        CDSCompletedDate, CDSCompletedHour, CDSCancelDate, CDSCancelHour, 
        Officer, Officer2, HSCode, Commodity, UnitQuantity, 
        UnitCost, InvoiceBL, Currency, InvoiceValue, TaxableValue2, 
        TaxUnit, TaxRate, TaxClass, Tax, RefDoc1, RefDoc2
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Thay thế tất cả giá trị NaN bằng None
    data = data.where(pd.notnull(data), None)
    
    # Chuyển mỗi hàng thành tuple và thực hiện truy vấn
    for row in data.itertuples(index=False, name=None):
        # Chuyển đổi tuple thành danh sách
        row_list = list(row)
        
        # Thay thế NaN bằng None
        for i in range(len(row_list)):
            if pd.isna(row_list[i]):  # Kiểm tra xem giá trị có phải là NaN không
                row_list[i] = None  # Thay thế bằng None
        
        # Chuyển lại thành tuple
        arr = tuple(row_list)
        
        cursor.execute(insert_query, arr)
    
    connection.commit()
    print(f"Đã thêm {len(data)} dòng dữ liệu")



def main():
    file_path = './data/2023.01.XK.xlsx'
    
    df = pd.read_excel(file_path)
    
    connection = create_connection("localhost", "root", "02052002Dat@", "cds")
    
    if connection:
        chunk_size = 100000 
        for start in range(0, len(df), chunk_size):
            chunk = df[start:start+chunk_size]
            
            insert_data(connection, chunk)
            print(f"Đã thêm {start} dòng dữ liệu")

if __name__ == "__main__":
    main()

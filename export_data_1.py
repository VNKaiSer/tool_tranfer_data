import mysql.connector
from mysql.connector import Error
import pandas as pd
import os
import time
from datetime import datetime, timedelta

def get_unique_filename(base_name, extension, max_attempts=100):
    """Tạo tên file duy nhất với số thứ tự nếu trùng lặp"""
    attempt = 0
    filename = f"{base_name}.{extension}"
    while os.path.exists(filename) and attempt < max_attempts:
        attempt += 1
        filename = f"{base_name}_{attempt}.{extension}"
    return filename

def fetch_data(start_date, end_date, table_name, period_label, tpsmode_condition):
    try:
        # Kết nối đến cơ sở dữ liệu MySQL
        db_connection = mysql.connector.connect(
            host='185.217.126.86',
            user='root',
            password='Ryou@2803',
            database='Ryou_MySQL_ImExMarket'
        )

        if db_connection.is_connected():
            print("Đã kết nối đến cơ sở dữ liệu")

            # Tạo một đối tượng cursor để thực hiện truy vấn
            cursor = db_connection.cursor()

            # Xây dựng điều kiện cho cột TPSMode
            tpsmode_query = ""
            if tpsmode_condition:
                tpsmode_query = f"AND xk.TPSMode = '{tpsmode_condition}'"
            else:
                tpsmode_query = ""

            # Câu lệnh SQL với khoảng thời gian động và tên bảng, đặt TPSMode trước CustomsOffice
            query = f"""
            SELECT 
                DATE_FORMAT(DATE, '%Y-%m') AS Month,
                xk.TPSMode,
                xk.CustomsOffice,
                xk.CDS,
                xk.TradeType,
                xk.ERC,
                xk.Exportor,
                xk.Importor, 
                xk.ImportCountry,
                xk.BLNumber,
                xk.FinalDestination,
                xk.POL,
                xk.HSCode,
                SUM(xk.InvoiceValue) AS InvoiceValue,
                xk.WeightUOM,
                -- Sử dụng MAX để đảm bảo tuân thủ quy tắc ONLY_FULL_GROUP_BY
                MAX(xk.GWEIGHT / uhc.Unique_HSCODE_Count) AS GweightUnique
            FROM 
                `{table_name}` xk
            INNER JOIN 
                (
                    -- Đếm số lượng HSCODE không trùng lặp cho mỗi CDS
                    SELECT 
                        CDS,
                        COUNT(DISTINCT HSCode) AS Unique_HSCODE_Count
                    FROM 
                        `{table_name}`
                    WHERE DATE >= '{start_date}' AND DATE <= '{end_date}'
                    GROUP BY 
                        CDS
                ) uhc
            ON 
                xk.CDS = uhc.CDS
            WHERE 
                xk.DATE >= '{start_date}' AND xk.DATE <= '{end_date}' {tpsmode_query}
            GROUP BY 
                Month, xk.TPSMode, xk.CustomsOffice, xk.CDS, xk.TradeType, xk.ERC, 
                xk.Exportor, xk.Importor, xk.ImportCountry, xk.BLNumber, xk.FinalDestination, 
                xk.POL, xk.HSCode, xk.WeightUOM;
            """

            # Đo thời gian truy vấn
            print(f"Đang thực hiện truy vấn trên bảng {table_name} từ {start_date} đến {end_date}...")
            start_time = time.time()  # Bắt đầu đếm thời gian

            # Thực hiện truy vấn
            cursor.execute(query)

            # Lấy kết quả
            results = cursor.fetchall()
            
            # Kết thúc đếm thời gian
            end_time = time.time()
            query_duration = end_time - start_time
            print(f"Thời gian truy vấn: {query_duration:.2f} giây")
            print(f"Đã nhận được kết quả, tổng số hàng: {len(results)}")

            # Tạo DataFrame từ kết quả, giữ nguyên kiểu dữ liệu từ MySQL, đặt TPSMode trước CustomsOffice
            df = pd.DataFrame(results, columns=[
                'Month', 'TPSMode', 'CustomsOffice', 'CDS', 'TradeType', 'ERC', 'Exporter', 'Importer', 'ImportCountry', 
                'BLNumber', 'FinalDestination', 'POL', 'HSCode', 'InvoiceValue', 'WeightUOM', 'GweightUnique'
            ])

            # Chuyển đổi các cột sang kiểu dữ liệu mong muốn
            df['CDS'] = df['CDS'].astype(str)  # Chuyển đổi 'CDS' sang chuỗi (text)
            df['CustomsOffice'] = df['CustomsOffice'].astype(str)  # Chuyển đổi 'CustomsOffice' sang chuỗi (text)
            df['InvoiceValue'] = df['InvoiceValue'].astype(float)  # Đảm bảo 'InvoiceValue' là kiểu float
            df['GweightUnique'] = df['GweightUnique'].astype(float)  # Đảm bảo 'Gweight' là kiểu float    

            # Lấy năm-tháng từ start_date và end_date
            start_month = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m')
            end_month = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m')

            # Điều chỉnh tên TPSMode trong file_name nếu có điều kiện
            if tpsmode_condition:
                tpsmode_suffix = f"_TPSMode_{tpsmode_condition}"
            else:
                tpsmode_suffix = ""

            # Đặt tên tệp Excel dựa trên tên cột Month và CustomsOffice & TPS, thêm khoảng thời gian
            base_file_name = f"MonthStaging_{table_name}{tpsmode_suffix}_{start_month}_to_{end_month}_{period_label}"
            output_file = get_unique_filename(base_file_name, 'xlsx')

            # Xuất DataFrame ra tệp Excel với mã hóa utf-8
            df.to_excel(output_file, index=False, engine='openpyxl')
            print(f"Kết quả đã được xuất ra tệp: {output_file}")

    except Error as e:
        print(f"Lỗi kết nối đến cơ sở dữ liệu: {e}")
    
    finally:
        # Đóng cursor và kết nối chỉ khi chúng đã được khởi tạo
        if 'cursor' in locals() and cursor:
            cursor.close()
        if db_connection.is_connected():
            db_connection.close()
            print("Kết nối đã được đóng")

def generate_monthly_reports(start_date_str, end_date_str, table_name, tpsmode_condition):
    """Tạo báo cáo theo từng tháng từ khoảng thời gian chỉ định"""
    try:
        # Phân tích chuỗi thành đối tượng datetime
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    except ValueError:
        print("Định dạng nhập không hợp lệ. Vui lòng nhập theo định dạng YYYY-MM-DD.")
        return

    if start_date > end_date:
        print("Ngày bắt đầu không thể lớn hơn ngày kết thúc.")
        return

    # Ghi nhận thời gian bắt đầu
    overall_start_time = time.time()
    print(f"Bắt đầu tạo báo cáo vào lúc: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    current_date = start_date

    while current_date <= end_date:
        # Tính toán ngày bắt đầu và kết thúc của mỗi tháng
        next_month = current_date.replace(day=28) + timedelta(days=4)  # Đảm bảo chuyển đến tháng sau
        last_day_of_month = next_month - timedelta(days=next_month.day)  # Ngày cuối cùng của tháng hiện tại

        # Phần 1: Từ ngày 1 đến ngày 7
        part_1_start = current_date.replace(day=1)
        part_1_end = current_date.replace(day=7)
        fetch_data(part_1_start.strftime('%Y-%m-%d'), part_1_end.strftime('%Y-%m-%d'), table_name, "Day1-7", tpsmode_condition)

        # Phần 2: Từ ngày 8 đến ngày 14
        part_2_start = current_date.replace(day=8)
        part_2_end = current_date.replace(day=14)
        fetch_data(part_2_start.strftime('%Y-%m-%d'), part_2_end.strftime('%Y-%m-%d'), table_name, "Day8-14", tpsmode_condition)

        # Phần 3: Từ ngày 15 đến ngày 21
        part_3_start = current_date.replace(day=15)
        part_3_end = current_date.replace(day=21)
        fetch_data(part_3_start.strftime('%Y-%m-%d'), part_3_end.strftime('%Y-%m-%d'), table_name, "Day15-21", tpsmode_condition)

        # Phần 4: Từ ngày 22 đến ngày 28
        part_4_start = current_date.replace(day=22)
        part_4_end = current_date.replace(day=28)
        fetch_data(part_4_start.strftime('%Y-%m-%d'), part_4_end.strftime('%Y-%m-%d'), table_name, "Day22-28", tpsmode_condition)

        # Phần 5: Từ ngày 29 đến cuối tháng
        part_5_start = current_date.replace(day=29)
        fetch_data(part_5_start.strftime('%Y-%m-%d'), last_day_of_month.strftime('%Y-%m-%d'), table_name, "Day29-End", tpsmode_condition)

        # Chuyển đến tháng tiếp theo
        current_date = next_month.replace(day=1)  # Đặt lại ngày đầu tháng

    # Ghi nhận thời gian kết thúc
    overall_end_time = time.time()
    total_duration = overall_end_time - overall_start_time
    print(f"Kết thúc tạo báo cáo vào lúc: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Tổng thời gian thực hiện: {total_duration:.2f} giây")

def get_user_input():
    """Hàm để nhận đầu vào từ người dùng theo định dạng ngày bắt đầu, kết thúc, tên bảng và điều kiện TPSMode"""
    table_name = input("Nhập tên bảng để thực hiện truy vấn: ")
    start_input = input("Nhập ngày bắt đầu (YYYY-MM-DD): ")
    end_input = input("Nhập ngày kết thúc (YYYY-MM-DD): ")
    tpsmode_condition = input("Nhập điều kiện cho cột TPSMode (để trống nếu lấy tất cả): ")

    generate_monthly_reports(start_input, end_input, table_name, tpsmode_condition)

if __name__ == "__main__":
    # Nhận đầu vào từ người dùng
    get_user_input()

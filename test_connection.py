"""
Script test để kiểm tra kết nối database và cấu trúc bảng
Chạy script này trước khi chạy unique_TPSMode_POD.py
"""

import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

load_dotenv()


def test_source_database():
    """Test kết nối và kiểm tra bảng nguồn"""
    print("=" * 60)
    print("🔍 KIỂM TRA DATABASE NGUỒN")
    print("=" * 60)

    try:
        config = {
            "host": os.getenv("DB_HOST_SOURCE"),
            "user": os.getenv("DB_USER_SOURCE"),
            "password": os.getenv("DB_PASSWORD_SOURCE"),
            "database": os.getenv("DB_NAME_SOURCE"),
        }

        print(f"📁 Host: {config['host']}")
        print(f"📁 Database: {config['database']}")
        print(f"👤 User: {config['user']}")

        conn = mysql.connector.connect(**config)
        cursor = conn.cursor(dictionary=True)

        print("✅ Kết nối database nguồn thành công!")

        # Kiểm tra bảng NK2024 có tồn tại không
        cursor.execute("SHOW TABLES LIKE 'NK2024'")
        table_exists = cursor.fetchone()

        if not table_exists:
            print("❌ Bảng NK2024 không tồn tại!")
            return False

        print("✅ Bảng NK2024 tồn tại")

        # Kiểm tra cấu trúc bảng
        cursor.execute("DESCRIBE NK2024")
        columns = cursor.fetchall()

        print("\n📋 Cấu trúc bảng NK2024:")
        for col in columns:
            print(f"   - {col['Field']}: {col['Type']}")

        # Kiểm tra các cột cần thiết
        column_names = [col["Field"] for col in columns]
        required_columns = ["Id", "TPSMode", "POD"]  # Lưu ý: Id với chữ I viết hoa
        missing_columns = [col for col in required_columns if col not in column_names]

        if missing_columns:
            print(f"❌ Thiếu các cột: {', '.join(missing_columns)}")
            return False

        print(f"✅ Các cột cần thiết đều có: {', '.join(required_columns)}")

        # Đếm số record
        cursor.execute("SELECT COUNT(*) as total FROM NK2024")
        total = cursor.fetchone()["total"]
        print(f"📊 Tổng số records: {total:,}")

        # Kiểm tra dữ liệu mẫu
        cursor.execute(
            """
            SELECT Id, TPSMode, POD 
            FROM NK2024 
            WHERE TPSMode IS NOT NULL AND POD IS NOT NULL
            LIMIT 5
        """
        )
        samples = cursor.fetchall()

        if samples:
            print(f"\n📝 Dữ liệu mẫu ({len(samples)} records):")
            for row in samples:
                print(
                    f"   ID: {row['Id']}, TPSMode: {row['TPSMode']}, POD: {row['POD']}"
                )
        else:
            print("⚠️ Không có dữ liệu hợp lệ (TPSMode và POD đều NULL)")

        # Đếm số DISTINCT TPSMode, POD
        cursor.execute(
            """
            SELECT COUNT(DISTINCT TPSMode, POD) as unique_count
            FROM NK2024
            WHERE TPSMode IS NOT NULL AND POD IS NOT NULL
        """
        )
        unique = cursor.fetchone()["unique_count"]
        print(f"📊 Số cặp (TPSMode, POD) unique: {unique:,}")

        cursor.close()
        conn.close()

        return True

    except Error as e:
        print(f"❌ Lỗi database nguồn: {e}")
        return False


def test_target_database():
    """Test kết nối và kiểm tra bảng đích"""
    print("\n" + "=" * 60)
    print("🎯 KIỂM TRA DATABASE ĐÍCH")
    print("=" * 60)

    try:
        config = {
            "host": os.getenv("DB_HOST_TARGET"),
            "user": os.getenv("DB_USER_TARGET"),
            "password": os.getenv("DB_PASSWORD_TARGET"),
            "database": os.getenv("DB_NAME_TARGET"),
        }

        print(f"📁 Host: {config['host']}")
        print(f"📁 Database: {config['database']}")
        print(f"👤 User: {config['user']}")

        conn = mysql.connector.connect(**config)
        cursor = conn.cursor(dictionary=True)

        print("✅ Kết nối database đích thành công!")

        # Kiểm tra bảng VNPort_TPSMode có tồn tại không
        cursor.execute("SHOW TABLES LIKE 'VNPort_TPSMode'")
        table_exists = cursor.fetchone()

        if not table_exists:
            print("⚠️ Bảng VNPort_TPSMode không tồn tại!")
            print("\n📝 SQL để tạo bảng:")
            print(
                """
CREATE TABLE VNPort_TPSMode (
    id INT AUTO_INCREMENT PRIMARY KEY,
    TPSMode VARCHAR(255),
    Port VARCHAR(255),
    UNIQUE KEY unique_tps_port (TPSMode, Port)
);
            """
            )
            return False

        print("✅ Bảng VNPort_TPSMode tồn tại")

        # Kiểm tra cấu trúc bảng
        cursor.execute("DESCRIBE VNPort_TPSMode")
        columns = cursor.fetchall()

        print("\n📋 Cấu trúc bảng VNPort_TPSMode:")
        for col in columns:
            print(f"   - {col['Field']}: {col['Type']}")

        # Kiểm tra các cột cần thiết
        column_names = [col["Field"] for col in columns]
        required_columns = ["TPSMode", "Port"]
        missing_columns = [col for col in required_columns if col not in column_names]

        if missing_columns:
            print(f"❌ Thiếu các cột: {', '.join(missing_columns)}")
            return False

        print(f"✅ Các cột cần thiết đều có: {', '.join(required_columns)}")

        # Kiểm tra UNIQUE key
        cursor.execute("SHOW INDEX FROM VNPort_TPSMode WHERE Key_name != 'PRIMARY'")
        indexes = cursor.fetchall()

        if indexes:
            print("\n🔑 Indexes:")
            for idx in indexes:
                print(f"   - {idx['Key_name']}: {idx['Column_name']}")
        else:
            print("⚠️ Không có UNIQUE key, dữ liệu có thể bị trùng lặp")

        # Đếm số record hiện có
        cursor.execute("SELECT COUNT(*) as total FROM VNPort_TPSMode")
        total = cursor.fetchone()["total"]
        print(f"📊 Số records hiện có: {total:,}")

        # Hiển thị dữ liệu mẫu nếu có
        if total > 0:
            cursor.execute("SELECT * FROM VNPort_TPSMode LIMIT 5")
            samples = cursor.fetchall()
            print(f"\n📝 Dữ liệu mẫu ({len(samples)} records):")
            for row in samples:
                print(f"   TPSMode: {row['TPSMode']}, Port: {row['Port']}")

        cursor.close()
        conn.close()

        return True

    except Error as e:
        print(f"❌ Lỗi database đích: {e}")
        return False


def main():
    print("\n" + "=" * 60)
    print("🧪 TEST KẾT NỐI VÀ CẤU TRÚC DATABASE")
    print("=" * 60 + "\n")

    source_ok = test_source_database()
    target_ok = test_target_database()

    print("\n" + "=" * 60)
    print("📊 KẾT QUẢ TỔNG HỢP")
    print("=" * 60)

    if source_ok and target_ok:
        print("✅ Tất cả kiểm tra đều thành công!")
        print("🚀 Bạn có thể chạy unique_TPSMode_POD.py")
    else:
        print("❌ Có lỗi, vui lòng kiểm tra lại:")
        if not source_ok:
            print("   - Database nguồn có vấn đề")
        if not target_ok:
            print("   - Database đích có vấn đề")


if __name__ == "__main__":
    main()

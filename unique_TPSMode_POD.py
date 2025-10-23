import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

# Load các biến môi trường từ file .env
load_dotenv()

BATCH_SIZE = 3_000_000
TABLE_SOURCE = "NK2025"
TABLE_TARGET = "VNPort_TPSMode"
MAX_WORKERS = 4  # Số lượng threads chạy song song (điều chỉnh tùy theo server)

# Lock để đảm bảo print không bị xung đột
print_lock = Lock()


def get_db_config_source():
    """Lấy thông tin kết nối database nguồn từ biến môi trường"""
    return {
        "host": os.getenv("DB_HOST_SOURCE"),
        "user": os.getenv("DB_USER_SOURCE"),
        "password": os.getenv("DB_PASSWORD_SOURCE"),
        "database": os.getenv("DB_NAME_SOURCE"),
    }


def get_db_config_target():
    """Lấy thông tin kết nối database đích từ biến môi trường"""
    return {
        "host": os.getenv("DB_HOST_TARGET"),
        "user": os.getenv("DB_USER_TARGET"),
        "password": os.getenv("DB_PASSWORD_TARGET"),
        "database": os.getenv("DB_NAME_TARGET"),
    }


def process_batch(batch_info):
    """
    Xử lý một batch dữ liệu
    - SELECT từ database nguồn
    - INSERT vào database đích
    Mỗi thread có connection riêng để tránh conflict
    """
    batch_num, batch_start, batch_end = batch_info
    conn_source = None
    conn_target = None

    try:
        # Tạo connection đến database nguồn
        config_source = get_db_config_source()
        conn_source = mysql.connector.connect(**config_source)
        cursor_source = conn_source.cursor(dictionary=True)

        # Tạo connection đến database đích
        config_target = get_db_config_target()
        conn_target = mysql.connector.connect(**config_target)
        cursor_target = conn_target.cursor()

        with print_lock:
            print(f"🚀 Thread {batch_num}: Đang xử lý id {batch_start} -> {batch_end}")

        start_time = time.time()

        # Bước 1: SELECT DISTINCT từ database nguồn
        select_query = f"""
            SELECT DISTINCT TPSMode, POD
            FROM {TABLE_SOURCE}
            WHERE Id BETWEEN {batch_start} AND {batch_end}
            AND TPSMode IS NOT NULL 
            AND POD IS NOT NULL
        """
        cursor_source.execute(select_query)
        data = cursor_source.fetchall()

        rows_affected = 0
        values = []

        # Bước 2: INSERT vào database đích
        if data:
            insert_query = f"""
                INSERT IGNORE INTO {TABLE_TARGET} (TPSMode, Port)
                VALUES (%s, %s)
            """
            # Chuyển đổi dict thành tuple, lọc NULL values
            w
            values = [
                (row["TPSMode"], row["POD"])
                for row in data
                if row["TPSMode"] and row["POD"]
            ]

            if values:
                with print_lock:
                    print(
                        f"   ↳ Batch #{batch_num}: Chuẩn bị insert {len(values)} unique records..."
                    )
                    # In ra 2 record đầu để debug
                    if len(values) >= 2:
                        print(f"   ↳ Sample data: {values[0]}, {values[1]}")

                cursor_target.executemany(insert_query, values)
                conn_target.commit()
                rows_affected = cursor_target.rowcount

                with print_lock:
                    print(
                        f"   ↳ Batch #{batch_num}: INSERT trả về rowcount = {rows_affected}"
                    )
            else:
                with print_lock:
                    print(
                        f"   ⚠️ Batch #{batch_num}: Không có dữ liệu hợp lệ sau khi lọc NULL"
                    )
        else:
            with print_lock:
                print(f"   ⚠️ Batch #{batch_num}: SELECT không trả về dữ liệu nào")

        elapsed_time = time.time() - start_time

        with print_lock:
            print(
                f"✅ Batch #{batch_num} hoàn tất: "
                f"đọc {len(data)} rows, insert {len(values)} values, "
                f"affected {rows_affected} rows "
                f"({elapsed_time:.2f}s)"
            )

        return {
            "batch_num": batch_num,
            "rows_read": len(data),
            "rows_affected": rows_affected,
            "success": True,
            "elapsed_time": elapsed_time,
        }

    except Error as e:
        with print_lock:
            print(f"❌ Batch #{batch_num} lỗi: {e}")
        return {
            "batch_num": batch_num,
            "rows_read": 0,
            "rows_affected": 0,
            "success": False,
            "error": str(e),
        }

    finally:
        # Đóng connection nguồn
        if conn_source and conn_source.is_connected():
            cursor_source.close()
            conn_source.close()

        # Đóng connection đích
        if conn_target and conn_target.is_connected():
            cursor_target.close()
            conn_target.close()


def main():
    try:
        # Kết nối đến database nguồn để lấy max_id
        config_source = get_db_config_source()
        conn_source = mysql.connector.connect(**config_source)
        cursor_source = conn_source.cursor(dictionary=True)

        print("🔍 Đang kiểm tra dữ liệu từ database nguồn...")

        # Lấy max id từ bảng nguồn (lưu ý: cột là Id với chữ I viết hoa)
        cursor_source.execute(f"SELECT MAX(Id) AS max_id FROM {TABLE_SOURCE}")
        max_id = cursor_source.fetchone()["max_id"]

        cursor_source.close()
        conn_source.close()

        if not max_id:
            print("⚠️ Bảng nguồn trống, không có dữ liệu.")
            return

        print(f"📊 Tổng số records: {max_id:,}")
        print(f"📁 Database nguồn: {config_source['database']} → Table: {TABLE_SOURCE}")
        print(
            f"📁 Database đích: {os.getenv('DB_NAME_TARGET')} → Table: {TABLE_TARGET}"
        )
        print(f"⚙️  Batch size: {BATCH_SIZE:,}")
        print(f"🔢 Số threads: {MAX_WORKERS}")

        # Tạo danh sách các batch cần xử lý
        batches = []
        batch_start = 1
        batch_num = 1

        while batch_start <= max_id:
            batch_end = min(batch_start + BATCH_SIZE - 1, max_id)
            batches.append((batch_num, batch_start, batch_end))
            batch_start = batch_end + 1
            batch_num += 1

        total_batches = len(batches)
        print(f"📦 Tổng số batches: {total_batches}")

        # Validation: Kiểm tra các batch không overlap
        print("\n🔍 Kiểm tra batch ranges để đảm bảo không trùng ID:")
        for i, (num, start, end) in enumerate(batches):
            print(f"   Batch #{num}: Id {start:,} → {end:,} ({end - start + 1:,} ids)")

            # Validate: batch sau phải bắt đầu sau batch trước
            if i > 0:
                prev_end = batches[i - 1][2]
                if start != prev_end + 1:
                    print(
                        f"   ⚠️  WARNING: Gap detected! Previous end: {prev_end}, Current start: {start}"
                    )
                if start <= prev_end:
                    print(
                        f"   ❌ ERROR: Overlap detected! Previous end: {prev_end}, Current start: {start}"
                    )
                    raise ValueError(
                        "Batch ranges overlap! Dừng để tránh xử lý trùng lặp."
                    )

        print("✅ Tất cả batch ranges hợp lệ, không có overlap")
        print("=" * 60)

        # Xử lý song song với ThreadPoolExecutor
        start_time = time.time()
        total_rows_read = 0
        total_rows_inserted = 0
        success_count = 0
        failed_count = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit tất cả các batch jobs
            future_to_batch = {
                executor.submit(process_batch, batch): batch for batch in batches
            }

            # Xử lý kết quả khi hoàn thành
            for future in as_completed(future_to_batch):
                result = future.result()
                if result["success"]:
                    success_count += 1
                    total_rows_read += result["rows_read"]
                    total_rows_inserted += result["rows_affected"]
                else:
                    failed_count += 1

        total_time = time.time() - start_time

        # Tổng kết
        print("=" * 60)
        print("🎉 HOÀN TẤT!")
        print(f"✅ Thành công: {success_count}/{total_batches} batches")
        if failed_count > 0:
            print(f"❌ Thất bại: {failed_count}/{total_batches} batches")
        print(f"📖 Tổng số hàng đọc từ nguồn: {total_rows_read:,}")
        print(f"📊 Tổng số hàng thêm mới vào đích: {total_rows_inserted:,}")
        print(f"⏱️  Tổng thời gian: {total_time:.2f}s")
        if total_rows_inserted > 0:
            print(f"🚀 Tốc độ trung bình: {total_rows_inserted/total_time:.0f} rows/s")

    except Error as e:
        print(f"❌ Lỗi MySQL: {e}")
    except Exception as e:
        print(f"❌ Lỗi không xác định: {e}")


if __name__ == "__main__":
    main()

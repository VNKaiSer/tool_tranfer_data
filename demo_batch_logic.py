"""
Demo script để hiển thị logic chia batch
Giải thích tại sao chạy song song KHÔNG bị trùng ID
"""


def demo_batch_division(max_id, batch_size):
    """
    Demo cách chia batch để đảm bảo không overlap
    """
    print("=" * 70)
    print(f"DEMO: Chia {max_id:,} records thành các batch {batch_size:,}")
    print("=" * 70)

    batches = []
    batch_start = 1
    batch_num = 1

    while batch_start <= max_id:
        batch_end = min(batch_start + batch_size - 1, max_id)
        batches.append((batch_num, batch_start, batch_end))

        print(f"\nBatch #{batch_num}:")
        print(f"   Start ID: {batch_start:,}")
        print(f"   End ID:   {batch_end:,}")
        print(f"   Query:    WHERE Id BETWEEN {batch_start} AND {batch_end}")
        print(f"   Số IDs:   {batch_end - batch_start + 1:,}")

        # Chuẩn bị cho batch tiếp theo
        batch_start = batch_end + 1
        batch_num += 1

    print("\n" + "=" * 70)
    print("KIỂM TRA OVERLAP")
    print("=" * 70)

    # Kiểm tra overlap
    has_overlap = False
    for i in range(len(batches) - 1):
        current_batch = batches[i]
        next_batch = batches[i + 1]

        current_end = current_batch[2]
        next_start = next_batch[1]

        print(f"\nBatch #{current_batch[0]} kết thúc ở ID {current_end:,}")
        print(f"Batch #{next_batch[0]} bắt đầu ở ID {next_start:,}")

        if next_start == current_end + 1:
            print(f"✅ OK: Không overlap (gap = 0)")
        elif next_start > current_end + 1:
            gap = next_start - current_end - 1
            print(f"⚠️  WARNING: Có gap {gap:,} IDs bị bỏ qua")
            has_overlap = True
        else:
            overlap = current_end - next_start + 1
            print(f"❌ ERROR: OVERLAP {overlap:,} IDs!")
            has_overlap = True

    print("\n" + "=" * 70)
    if not has_overlap:
        print("✅ KẾT LUẬN: Tất cả batches hợp lệ, KHÔNG có overlap")
        print("🚀 AN TOÀN để chạy song song!")
    else:
        print("❌ KẾT LUẬN: Có vấn đề với logic chia batch")
    print("=" * 70)

    return batches


def explain_parallel_execution():
    """
    Giải thích tại sao chạy song song an toàn
    """
    print("\n\n" + "=" * 70)
    print("GIẢI THÍCH: TẠI SAO CHẠY SONG SONG KHÔNG BỊ TRÙNG?")
    print("=" * 70)

    print(
        """
📌 NGUYÊN TẮC:
   Mỗi batch xử lý một RANGE ID khác nhau, KHÔNG OVERLAP

📌 VÍ DỤ VỚI 15,000,000 RECORDS, BATCH_SIZE = 3,000,000:

   Thread 1: WHERE Id BETWEEN 1        AND 3,000,000
   Thread 2: WHERE Id BETWEEN 3,000,001 AND 6,000,000  
   Thread 3: WHERE Id BETWEEN 6,000,001 AND 9,000,000
   Thread 4: WHERE Id BETWEEN 9,000,001 AND 12,000,000
   Thread 5: WHERE Id BETWEEN 12,000,001 AND 15,000,000

📌 TẠI SAO AN TOÀN?

   1. Mỗi thread query từ DATABASE NGUỒN với range riêng biệt
   2. Không có ID nào xuất hiện trong 2 ranges
   3. WHERE Id BETWEEN start AND end → MySQL tự lọc đúng range
   4. Threads chạy song song nhưng XỬ LÝ DỮ LIỆU KHÁC NHAU

📌 SO SÁNH:

   ❌ SAI (nếu code như này):
      Thread 1: SELECT * FROM table LIMIT 1000000
      Thread 2: SELECT * FROM table LIMIT 1000000
      → Cả 2 đều lấy 1 triệu records ĐẦU TIÊN → TRÙNG!

   ✅ ĐÚNG (code hiện tại):
      Thread 1: SELECT * FROM table WHERE Id BETWEEN 1 AND 3000000
      Thread 2: SELECT * FROM table WHERE Id BETWEEN 3000001 AND 6000000
      → 2 threads lấy RANGE KHÁC NHAU → KHÔNG TRÙNG!

📌 VỀ INSERT:

   - INSERT IGNORE INTO target_table
   - Nếu có UNIQUE constraint (TPSMode, Port)
   - MySQL tự động bỏ qua nếu trùng
   - Nhưng với logic chia batch đúng, sẽ KHÔNG có trùng từ các thread

📌 KẾT LUẬN:
   
   ✅ Code HOÀN TOÀN AN TOÀN cho parallel processing
   ✅ Mỗi thread xử lý dữ liệu RIÊNG BIỆT
   ✅ Không có race condition hoặc data duplication
    """
    )
    print("=" * 70)


if __name__ == "__main__":
    # Demo với các trường hợp khác nhau

    print("\n🔸 CASE 1: 15 triệu records, batch 3 triệu")
    demo_batch_division(15_000_000, 3_000_000)

    print("\n\n🔸 CASE 2: 10 triệu records, batch 3 triệu (batch cuối < 3 triệu)")
    demo_batch_division(10_000_000, 3_000_000)

    print("\n\n🔸 CASE 3: 2.5 triệu records, batch 3 triệu (chỉ 1 batch)")
    demo_batch_division(2_500_000, 3_000_000)

    # Giải thích
    explain_parallel_execution()

    print("\n\n💡 TIP: Chạy script này để hiểu rõ logic trước khi chạy script chính!")
    print("💡 Command: python demo_batch_logic.py\n")

# Hướng dẫn Load Test và So sánh Couchbase vs MongoDB

## Tổng quan

Hệ thống load testing này được thiết kế để:
- Test hiệu năng của Couchbase và MongoDB với số lượng concurrent users lớn
- So sánh hiệu năng giữa hai database
- Tạo báo cáo chi tiết với biểu đồ so sánh

## Cài đặt

1. Cài đặt các dependencies:
```bash
pip install -r requirements.txt
```

2. Cấu hình kết nối database trong file `.env`:
```
COUCHBASE_HOST=your_couchbase_host
COUCHBASE_PORT=8091
COUCHBASE_USER=Administrator
COUCHBASE_PASSWORD=your_password

MONGODB_HOST=your_mongodb_host
MONGODB_PORT=27017
MONGODB_USER=admin
MONGODB_PASSWORD=your_password
MONGODB_TLS=false
```

3. Đảm bảo file `src/dbsetting.json` có cấu hình đúng các buckets/collections cần test:
```json
{
    "xyz": ["rms_events", "rms_view"],
    "testdb": ["rms_journal", "rms_rating_model", "rms_read_model", "rms_write_model"]
}
```

## Sử dụng

### Chạy Load Test đầy đủ

Chạy script chính để test tất cả các buckets/collections:

```bash
cd src
python run_load_test.py
```

Script này sẽ:
1. Test với các mức concurrent users: 10, 50, 100, 200, 500, 1000, 2000, 5000
2. Mỗi test chạy trong 30 giây
3. Tạo báo cáo HTML với biểu đồ cho mỗi bucket/collection

### Tùy chỉnh Test

Bạn có thể chỉnh sửa file `run_load_test.py` để thay đổi:

- **Số lượng concurrent users**: Sửa biến `user_counts`
- **Thời gian test**: Sửa biến `duration_seconds` (None = chạy 1 query mỗi user)
- **Loại query**: Sửa biến `query_type` ('count', 'select_all', 'select_paginated')

### Sử dụng LoadTester trực tiếp

```python
from load_test import LoadTester

# Tạo tester
tester = LoadTester(
    bucket_name="rms_events",
    collection_name="rms_events",
    db_name="xyz"
)

# Chạy test với 100 concurrent users trong 30 giây
stats = tester.run_concurrent_test(
    num_users=100,
    query_type="select_all",
    duration_seconds=30
)

# Xem kết quả
print(f"Couchbase avg response time: {stats['couchbase']['avg_response_time']*1000:.2f}ms")
print(f"MongoDB avg response time: {stats['mongodb']['avg_response_time']*1000:.2f}ms")
```

## Kết quả

Sau khi chạy test, các file sau sẽ được tạo trong thư mục `reports/`:

1. **Báo cáo HTML**: `{bucket_name}_load_test_report.html`
   - Báo cáo chi tiết với tất cả các biểu đồ
   - So sánh từng metric giữa Couchbase và MongoDB
   - Bảng kết quả chi tiết cho từng mức concurrent users

2. **Biểu đồ PNG**: `reports/charts/{bucket_name}_*.png`
   - Response time comparison
   - Throughput comparison
   - Success rate comparison
   - Percentiles (P50, P95, P99)
   - Overall comparison
   - Response time distribution

3. **JSON Results**: `reports/{bucket_name}_test_results_{num_users}users.json`
   - Dữ liệu raw của từng test scenario

## Metrics được đo

- **Response Time**: Thời gian phản hồi (min, max, avg, median, P95, P99)
- **Throughput**: Số queries mỗi giây (QPS)
- **Success Rate**: Tỷ lệ queries thành công (%)
- **Total Queries**: Tổng số queries đã thực thi
- **Records Returned**: Tổng số records trả về

## Lưu ý

1. **Tài nguyên hệ thống**: Test với số lượng users lớn (5000+) có thể tiêu tốn nhiều tài nguyên. Đảm bảo hệ thống có đủ RAM và CPU.

2. **Thời gian test**: Mỗi test scenario với 30 giây và nhiều users có thể mất thời gian. Tổng thời gian có thể lên đến vài giờ tùy vào số lượng buckets.

3. **Kết nối database**: Đảm bảo cả Couchbase và MongoDB đều có thể truy cập và có dữ liệu để test.

4. **Index**: Đảm bảo Couchbase có primary index cho các buckets được test.

## Troubleshooting

### Lỗi kết nối
- Kiểm tra file `.env` có đúng thông tin kết nối
- Kiểm tra network connectivity đến database servers
- Kiểm tra firewall settings

### Lỗi timeout
- Tăng timeout trong config nếu queries mất quá nhiều thời gian
- Giảm số lượng concurrent users nếu hệ thống không đủ mạnh

### Lỗi index
- Đảm bảo primary index đã được tạo cho Couchbase buckets
- Chạy script tạo index trước khi test

## Ví dụ Output

Báo cáo HTML sẽ bao gồm:
- Tóm tắt kết quả với các cards hiển thị metrics chính
- 6 biểu đồ so sánh chi tiết
- Bảng so sánh chi tiết cho từng mức concurrent users
- Kết luận về database nào tốt hơn cho từng metric

## Tùy chỉnh nâng cao

Bạn có thể tùy chỉnh các loại query test bằng cách:
- Thêm custom query trong `LoadTester._execute_couchbase_query()`
- Thêm custom filter trong `LoadTester._execute_mongodb_query()`
- Tạo các test scenarios khác nhau trong `run_load_test.py`


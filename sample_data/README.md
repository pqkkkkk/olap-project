# Sample Data for Power BI Visualization

Thư mục này chứa dữ liệu mẫu để phục vụ trực quan hóa trong Power BI, cho phép team visualization làm việc song song với team data processing.

## 📁 Files

### 1. `raw_transactions.csv`
Dữ liệu giao dịch thô - giống như format từ POS Simulator.

**Schema:**
| Column | Type | Description |
|--------|------|-------------|
| User | int | ID người dùng (0-9) |
| Card | string | Số thẻ tín dụng |
| Year, Month, Day | int | Ngày giao dịch |
| Time | string | Thời gian (HH:mm:ss) |
| Amount | string | Số tiền USD (có $ prefix) |
| Use Chip | string | Chip/Swipe/Online Transaction |
| Merchant Name | string | Tên merchant |
| Merchant City | string | Thành phố |
| Merchant State | string | Bang (2 ký tự) |
| Zip | string | Mã ZIP |
| MCC | string | Merchant Category Code |
| Errors? | string | Loại lỗi (nếu có) |
| Is Fraud? | string | Yes/No |

### 2. `processed_transactions.csv`
Dữ liệu đã được xử lý bởi Spark - sẵn sàng cho Power BI.

**Schema bổ sung:**
| Column | Type | Description |
|--------|------|-------------|
| Amount_USD | decimal | Số tiền USD (đã clean) |
| Amount_VND | decimal | Số tiền VND (quy đổi) |
| Exchange_Rate | int | Tỉ giá USD/VND (25057) |
| Transaction_Date | datetime | Timestamp đầy đủ |
| Date_Formatted | string | dd/mm/yyyy |
| Time_Formatted | string | HH:mm:ss |
| Hour | int | Giờ (0-23) |
| Day_of_Week | string | Tên ngày |
| Is_Weekend | string | Yes/No |
| Processed_Timestamp | datetime | Thời điểm xử lý |

## 📊 Thống kê dữ liệu mẫu

- **Tổng giao dịch:** 126
- **Số users:** 10 (User 0-9)
- **Khoảng thời gian:** 15/01/2024 - 20/01/2024 (6 ngày)
- **Fraud transactions:** 7 (~5.6%)
- **Error transactions:** 4 (~3.2%)
- **Weekend transactions:** 24 (~19%)
- **Cities:** 20+ thành phố khác nhau
- **Merchants:** 60+ merchants khác nhau

## 🔍 Use Cases cho Visualization

### Task 3 (Yêu cầu 1-5):
1. ✅ **Thời điểm có nhiều giao dịch nhất** → Dùng cột `Hour`
2. ✅ **Thành phố có tổng giá trị cao nhất** → Dùng `Merchant_City` + `Amount_VND`
3. ✅ **Merchant có số lượng/giá trị cao nhất** → Dùng `Merchant_Name` + `Amount_VND`
4. ✅ **Tỷ lệ fraud cao bất thường** → Dùng `Is_Fraud` + `Merchant_City`
5. ✅ **User có nhiều giao dịch liên tiếp** → Dùng `User` + `Hour`

### Task 4 (Yêu cầu 6-10):
6. ✅ **Giao dịch giá trị lớn** → Filter `Amount_USD > 500`
7. ✅ **Xu hướng fraud** → Dùng `Is_Fraud` + `Use_Chip` + `Hour`
8. ✅ **Khác biệt weekday vs weekend** → Dùng `Is_Weekend` + `Day_of_Week`
9. ✅ **User có nhiều lỗi/fraud** → Dùng `User` + `Errors` + `Is_Fraud`
10. ✅ **Đề xuất cải tiến** → Tổng hợp từ các phân tích trên

## 💡 Tips cho Power BI

```dax
// Tính tổng giá trị VND
Total_VND = SUM('transactions'[Amount_VND])

// Tính fraud rate
Fraud_Rate = 
DIVIDE(
    COUNTROWS(FILTER('transactions', 'transactions'[Is_Fraud] = "Yes")),
    COUNTROWS('transactions')
) * 100

// Peak hour
Peak_Hour = 
TOPN(1, VALUES('transactions'[Hour]), COUNTROWS('transactions'), DESC)
```

## ⚠️ Lưu ý

- Dữ liệu mẫu này được tạo để phục vụ mục đích trực quan hóa
- Khi data processing hoàn thành, thay thế bằng dữ liệu thực từ HDFS
- Tỉ giá USD/VND sử dụng: **25,057** (tham khảo VCB)

# Mô tả bài toán 
Một công ty tài chính muốn xây dựng hệ thống quản lý dữ liệu giao dịch thông qua thẻ tín dụng 
(credit card), dữ liệu được phát sinh từ các máy POS đặt tại các cửa hàng mua sắm, nhà hàng, bất 
cứ nơi nào thanh toán không dùng tiền mặt. Công ty muốn xây dựng hệ thống xử lý dữ liệu theo 
thời gian thực, khi một giao dịch được phát sinh, dữ liệu được gửi đên hệ thống, tiến hành kiểm 
tra dữ liệu có lỗi hay không? Is Fraud = Yes xác định lỗi và giao dịch này xem như không thành 
công, không cần xử lý tiếp. Khi một giao dịch thành công thì tiến hành lưu trữ các thông tin 
Credit Card, ngày giao dịch theo định dạng: dd/mm/yyyy, thời gian theo định dạng: hh:mm:ss, 
Merchant name (nơi xảy ra giao dịch), Merchant City (thành phố nơi giao dịch), Số tiền chuyển 
sang VNĐ, theo tỉ giá được cập nhật mỗi ngày. Cuối ngày, tất cả giao dịch được được thống kê 
như sau: Cho biết tổng số lượng giá trị từng merchant name, đếm số lượng giao dịch mỗi 
merchant name, tất cả thống kê theo ngày, tháng và năm. Tất cả thông tin thống kê này được 
trực quan hóa qua công cụ hoặc hệ thống chuyên biệt.  
# Các tình huống giả định 
Hãy giả định rằng giao dịch được phát sinh mỗi khi người dùng dùng credit card quẹt trên các 
máy POS tại các cửa hàng mua sắm, nhà hàng,.... Mỗi giao dịch này được gửi qua hệ thống kafka 
theo thời gian thực. Sinh viên dùng Kafka để mô phỏng từng giao dịch được phát sinh với các 
thông tin được cho trước dạng csv. (tập tin sẽ được đính kèm) 
Cấu trúc thông tin như sau: 
User,Card,Year,Month,Day,Time,Amount,Use Chip,Merchant Name,Merchant City,Merchant State,Zip,MCC,Errors?,Is Fraud? 
Kafka sẽ đọc từng dòng csv và gửi qua topic được định nghĩa trước để giả lập một giao dịch được 
phát sinh từ máy POS. 
   
 
 
# Yêu cầu tối thiểu đồ án 
## Công nghệ: 
1. Sử dụng kafka để đọc dữ liệu csv từng dòng và gửi thông tin này đến topic định nghĩa 
trước theo chu kì thời gian ngẫu nhiên trong phạm vi từ 1s đến 5s. 
2. Sử dụng spark streaming để đọc dữ liệu từ kafka theo thời gian thực, nghĩa là bất cứ 
thông tin nào từ kafka được xử lý tức thì, các xử lý bao gồm lọc dữ liệu, biến đổi thông 
tin, tính toán dữ liệu bao gồm lấy tỉ giá mới nhất từ các web site chính thống như 
vietcombank, dùng Web Scraping và API (trường hợp API lỗi, không ổn định thì có 
phương án dự phòng là Web Scraping). 
3. Sử dụng Hadoop để lưu trữ các thông tin được xử lý từ Spark và là nơi lưu trữ thông tin 
được xử lý để có thể trực quan hóa dữ liệu và thống kê ở giai đoạn sau. 
4. Sử dụng Power BI để đọc dữ liệu từ Hadoop (dạng csv), thống kê dữ liệu theo mô tả bài 
toán và hiển thị dữ liệu một cách trực quan.  
5. Sử dụng Air Flow để lên lịch quá trình đọc và hiển thị dữ liệu từ Power BI sao cho dữ liệu 
luôn được update mỗi ngày. 
## Phân tích thời gian thực (*): 
1. Thời điểm nào trong ngày có nhiều giao dịch nhất? Có khung giờ nào giao dịch bất thường 
không? 
2. Thành phố nào có tổng giá trị giao dịch cao nhất? Có liên hệ với dân số hoặc vị trí không? 
3. Merchant nào có số lượng hoặc giá trị giao dịch cao nhất? 
4. Thành phố hoặc merchant nào có tỷ lệ fraud cao bất thường? 
5. Người dùng nào có nhiều giao dịch liên tiếp trong thời gian ngắn? 
6. Giao dịch có giá trị lớn thường xảy ra vào thời điểm nào? Ở đâu? 
7. Có xu hướng nào trong các giao dịch bị fraud không? (giờ, merchant, city,...) 
8. Có sự khác biệt nào giữa giao dịch ngày thường và cuối tuần? 
9. Có người dùng nào bị nhiều lỗi hoặc bị gắn cờ fraud nhiều hơn mức trung bình? 
10.  Từ các phân tích trên, hãy đề xuất cải tiến cho hệ thống để giảm gian lận hoặc tối ưu vận 
hành.
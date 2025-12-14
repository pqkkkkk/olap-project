# Credit Card Transaction Processing System

Hệ thống xử lý giao dịch thẻ tín dụng theo thời gian thực sử dụng Kafka, Spark Streaming, Hadoop và Airflow.

## 🚀 Các Services trong Project

| Service | Port | Mô tả |
|---------|------|-------|
| **Kafka Broker** | 9092, 9094 | Message broker để stream giao dịch |
| **Kafka UI** | 5050 | Giao diện quản lý Kafka |
| **Spark Master** | 7077, 8088 | Spark cluster master node |
| **Spark Worker** | 8081 | Spark worker node |
| **Hadoop Namenode** | 9870 | HDFS namenode UI |
| **Hadoop Datanode** | 9864 | HDFS datanode |
| **Hadoop ResourceManager** | 8088 | YARN resource manager |
| **Hadoop NodeManager** | - | YARN node manager |
| **Airflow Webserver** | 8082 | Airflow UI để lập lịch |
| **Airflow Scheduler** | - | Lập lịch và thực thi DAGs |
| **Airflow PostgreSQL** | 5432 | Database cho Airflow |

## 📋 Yêu cầu hệ thống

- Docker Desktop
- RAM tối thiểu: 8GB (khuyến nghị 16GB)
- CPU: Tối thiểu 4 cores
- Dung lượng đĩa: 20GB trống

## 🔧 Cài đặt và Khởi động

### 1. Khởi động tất cả services

```powershell
# Khởi động tất cả services
docker-compose up -d

# Xem logs của tất cả services
docker-compose logs -f

# Xem logs của service cụ thể
docker-compose logs -f airflow-webserver
docker-compose logs -f spark-master
```

### 2. Truy cập các giao diện web

- **Kafka UI**: http://localhost:5050
- **Spark Master UI**: http://localhost:8088
- **Spark Worker UI**: http://localhost:8081
- **Hadoop Namenode**: http://localhost:9870
- **Airflow UI**: http://localhost:8082
  - Username: `airflow`
  - Password: `airflow`

### 3. Dừng services

```powershell
# Dừng tất cả services
docker-compose down

# Dừng và xóa volumes (reset hoàn toàn)
docker-compose down -v
```

## 📁 Cấu trúc thư mục

```
Week09&10 - Project/
├── docker-compose.yml          # File docker compose chính
├── airflow/                    # Airflow configuration
│   ├── dags/                   # Airflow DAGs (workflows)
│   ├── logs/                   # Airflow logs
│   ├── plugins/                # Airflow plugins
│   └── config/                 # Airflow config files
├── scripts/                    # Spark scripts
├── data/                       # CSV data files
├── hdfs/                       # HDFS data
│   ├── namenode/
│   └── datanode/
└── README.md
```

## 🛠️ Workflow xử lý dữ liệu

1. **Producer (Kafka)**: Đọc CSV và gửi từng giao dịch vào Kafka topic
2. **Spark Streaming**: Đọc real-time từ Kafka, xử lý và lọc dữ liệu
3. **Hadoop HDFS**: Lưu trữ dữ liệu đã xử lý
4. **Airflow**: Lập lịch các task xử lý dữ liệu hàng ngày
5. **Power BI**: Trực quan hóa dữ liệu từ HDFS

## 🔍 Troubleshooting

### Lỗi port đã được sử dụng
```powershell
# Kiểm tra port đang được sử dụng
netstat -ano | findstr :8082
netstat -ano | findstr :9092

# Dừng tất cả containers
docker-compose down
```

### Airflow không khởi động được
```powershell
# Xem logs chi tiết
docker-compose logs airflow-init
docker-compose logs airflow-webserver

# Reset Airflow database
docker-compose down -v
docker-compose up -d
```

### Tạo thư mục cần thiết
```powershell
# Tạo các thư mục Airflow
mkdir -p airflow/dags airflow/logs airflow/plugins airflow/config
mkdir -p scripts data hdfs/namenode hdfs/datanode
```

## 📝 Ghi chú

- Lần đầu khởi động có thể mất 2-3 phút để initialize database
- Airflow UI mặc định: username/password = `airflow`/`airflow`
- Tất cả DAGs được đặt trong thư mục `airflow/dags/`
- Logs của Airflow lưu trong `airflow/logs/`

## 🔗 Tài liệu tham khảo

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Hadoop HDFS Documentation](https://hadoop.apache.org/docs/stable/)

"""
Spark Streaming Consumer - Đọc và xử lý dữ liệu từ Kafka
Đóng vai trò như Consumer trong kiến trúc streaming
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp,
    when, date_format, year, month, dayofmonth, hour, minute,
    regexp_replace, trim, lit, udf, dayofweek, length, make_date
)
import os
from pathlib import Path
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType
)
import logging

from exchange_rate_service import ExchangeRateService

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkStreamingConsumer:
    def __init__(self, 
                 app_name="CreditCardStreamingConsumer",
                 kafka_bootstrap_servers="kafka-broker:9092",
                 kafka_topic="credit-card-transactions"):
        """
        Khởi tạo Spark Streaming Consumer
        
        Args:
            app_name: Tên ứng dụng Spark
            kafka_bootstrap_servers: Địa chỉ Kafka broker
            kafka_topic: Topic để đọc dữ liệu
        """
        self.app_name = app_name
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.spark = None

        # Tạo base directories nếu chưa tồn tại
        base_path = Path("data")
        self.output_path = base_path / "output"
        self.checkpoint_path = base_path / "checkpoint"
        
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path.mkdir(parents=True, exist_ok=True)
        
        # Tạo subdirectories
        for subdir in ["valid_transactions"]:
            (self.output_path / subdir).mkdir(parents=True, exist_ok=True)
            (self.checkpoint_path / subdir).mkdir(parents=True, exist_ok=True)


        self.exchange_service = ExchangeRateService()
        self.current_rate = self.exchange_service.get_exchange_rate()
        logger.info(f"💱 Tỉ giá hiện tại: {self.current_rate:,.0f} VND/USD")

        # HDFS configuration
        self.hdfs_namenode = "namenode:8020"
        self.hdfs_base_path = f"hdfs://{self.hdfs_namenode}/credit-card/processed"
        self.hdfs_checkpoint_path = f"hdfs://{self.hdfs_namenode}/credit-card/checkpoint"
    
    
    def register_exchange_rate_udf(self):
        """
        Register UDF để convert USD sang VND
        """
        current_rate = self.current_rate
        
        @udf(returnType=DoubleType())
        def convert_usd_to_vnd(amount_usd):
            """Convert USD to VND"""
            if amount_usd is None or amount_usd <= 0:
                return None
            return float(amount_usd * current_rate)
        
        logger.info(f"✅ Đã register UDF convert_usd_to_vnd (rate: {current_rate:,.0f})")
        return convert_usd_to_vnd
        
    def register_datetime_key_udf(self):
        """
        ✅ Register UDF để tạo DateTime_Hour_Key
        Format: YYYY-MM-DD-HH (ví dụ: 2024-01-15-08)
        """
        @udf(returnType=StringType())
        def create_datetime_hour_key(year, month, day, hour):
            if year is None or month is None or day is None or hour is None:
                return None
            # ✅ Format: YYYY-MM-DD-HH (NOT YYYYMMDDHH)
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}-{int(hour):02d}"
        
        logger.info(f"✅ Đã register UDF create_datetime_hour_key (format: YYYY-MM-DD-HH)")
        return create_datetime_hour_key
    
    def register_day_of_week_udf(self):
        """
        ✅ Register UDF để lấy tên ngày trong tuần
        """
        @udf(returnType=StringType())
        def get_day_of_week(day_of_week_num):
            if day_of_week_num is None:
                return None
            days = ["Sunday", "Monday", "Tuesday", "Wednesday", 
                   "Thursday", "Friday", "Saturday"]
            # Spark dayofweek: 1=Sunday, 2=Monday, ..., 7=Saturday
            return days[int(day_of_week_num) - 1]
        
        logger.info(f"✅ Đã register UDF get_day_of_week")
        return get_day_of_week
    
    def register_is_weekend_udf(self):
        """
        ✅ Register UDF để xác định weekend
        """
        @udf(returnType=StringType())
        def check_is_weekend(day_of_week_num):
            if day_of_week_num is None:
                return None
            # 1=Sunday, 7=Saturday
            return "Yes" if int(day_of_week_num) in [1, 7] else "No"
        
        logger.info(f"✅ Đã register UDF check_is_weekend")
        return check_is_weekend
      
    def create_spark_session(self):
        """Tạo Spark Session với cấu hình Kafka"""
        try:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.jars.packages", 
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                .config("spark.sql.streaming.checkpointLocation", 
                        "/tmp/spark-checkpoint") \
                .config("spark.sql.shuffle.partitions", "3") \
                .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info(f"✅ Spark Session đã được tạo: {self.app_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Không thể tạo Spark Session: {e}")
            return False
    
    def define_schema(self):
        """
        Định nghĩa schema cho dữ liệu transaction từ CSV
        """
        return StructType([
            StructField("User", StringType(), True),
            StructField("Card", StringType(), True),
            StructField("Year", IntegerType(), True),
            StructField("Month", IntegerType(), True),
            StructField("Day", IntegerType(), True),
            StructField("Time", StringType(), True),
            StructField("Amount", StringType(), True),  # String vì có dấu $
            StructField("Use Chip", StringType(), True),
            StructField("Merchant Name", StringType(), True),
            StructField("Merchant City", StringType(), True),
            StructField("Merchant State", StringType(), True),
            StructField("Zip", StringType(), True),
            StructField("MCC", StringType(), True),
            StructField("Errors?", StringType(), True),
            StructField("Is Fraud?", StringType(), True),
            StructField("timestamp", StringType(), True)  # Timestamp từ producer
        ])
    
    def read_from_kafka(self):
        """
        Đọc streaming data từ Kafka
        Spark đóng vai trò như CONSUMER ở đây
        """
        try:
            # Đọc stream từ Kafka
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", self.kafka_topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            logger.info(f"✅ Đã kết nối Kafka Consumer - Topic: {self.kafka_topic}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Không thể đọc từ Kafka: {e}")
            return None
    
    def process_stream(self, kafka_df):
        """
        Xử lý streaming data
        
        Args:
            kafka_df: DataFrame từ Kafka
        """
        # Parse JSON từ Kafka value
        schema = self.define_schema()
        
        parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")
        
        # Register các UDFs
        convert_to_vnd = self.register_exchange_rate_udf()
        create_datetime_hour_key = self.register_datetime_key_udf()
        get_day_of_week = self.register_day_of_week_udf()
        check_is_weekend = self.register_is_weekend_udf()
        
        # Xử lý và làm sạch dữ liệu
        processed_df = parsed_df \
            .withColumn("Amount_USD", 
                       regexp_replace(col("Amount"), "[$,]", "").cast("double")) \
            .withColumn("Amount_VND", convert_to_vnd(col("Amount_USD"))) \
            .withColumn("Exchange_Rate", lit(int(self.current_rate))) \
            .withColumn("transaction_date", 
                       to_timestamp(col("timestamp"))) \
            .withColumn("year", year(col("transaction_date"))) \
            .withColumn("month", month(col("transaction_date"))) \
            .withColumn("day", dayofmonth(col("transaction_date"))) \
            .withColumn("hour", hour(col("transaction_date"))) \
            .withColumn("minute", minute(col("transaction_date"))) \
            .withColumn("date_str", 
                       date_format(col("transaction_date"), "dd/MM/yyyy")) \
            .withColumn("time_str", 
                       date_format(col("transaction_date"), "HH:mm:ss")) \
            .withColumn("day_of_week_num", dayofweek(col("transaction_date"))) \
            .withColumn("Day_of_Week", get_day_of_week(col("day_of_week_num"))) \
            .withColumn("Is_Weekend", check_is_weekend(col("day_of_week_num"))) \
            .withColumn("DateTime_Hour_Key", 
                       create_datetime_hour_key(col("year"), col("month"), 
                                              col("day"), col("hour"))) \
            .withColumn("Use_Chip", col("Use Chip")) \
            .withColumn("Merchant_Name", col("Merchant Name")) \
            .withColumn("Merchant_City", col("Merchant City")) \
            .withColumn("Merchant_State", col("Merchant State")) \
            .withColumn("Errors", trim(col("Errors?"))) \
            .withColumn("Is_Fraud", trim(col("Is Fraud?"))) \
            .withColumn("Processed_Timestamp", 
                       date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("real_date_check", make_date(col("Year"), col("Month"), col("Day"))) \
            .withColumn("is_valid_date", col("real_date_check").isNotNull())
        
        # 1. Error Transactions: Cột Errors có nội dung (Bất kể Card/Year đúng hay sai)
        error_transactions = processed_df \
            .filter((col("Errors").isNotNull()) & (col("Errors") != ""))

        # 2. Fraud Transactions: Is Fraud = Yes (Và không phải là Error)
        fraud_transactions = processed_df \
            .filter((col("Errors").isNull()) | (col("Errors") == "")) \
            .filter(col("Is_Fraud") == "Yes")

        # 3. Valid Transactions: Không Error, Không Fraud, VÀ thỏa mãn điều kiện dữ liệu sạch
        valid_transactions = processed_df \
            .filter((col("Errors").isNull()) | (col("Errors") == "")) \
            .filter(col("Is_Fraud") == "No") \
            .filter(col("User").isNotNull()) \
            .filter(col("Card").isNotNull()) \
            .filter(length(col("Card")) >= 16) \
            .filter(col("Amount_USD").isNotNull() & (col("Amount_USD") > 0)) \
            .filter(col("is_valid_date") == True) 

        # 4. Invalid (Phần còn lại): Những cái không Error, không Fraud, nhưng dữ liệu rác
        invalid_df = processed_df \
            .filter((col("Errors").isNull()) | (col("Errors") == "")) \
            .filter(col("Is_Fraud") == "No") \
            .filter((col("Amount_USD").isNull()) 
                    | (col("Amount_USD") <= 0) 
                    | (length(col("Card")) < 16)
                    | (col("is_valid_date") == False)
                    ) \
            .withColumn("invalid_reason", 
                when(col("is_valid_date") == False, lit("Invalid Date"))
                .otherwise(lit("Data format invalid or missing")))

        return valid_transactions, fraud_transactions, error_transactions, invalid_df
    
    def write_to_console(self, df, output_mode="append", format_type="complete"):
        """
        Ghi dữ liệu ra console để debug
        
        Args:
            df: DataFrame để ghi
            output_mode: Mode ghi (append, complete, update)
            format_type: Loại format (complete, compact)
        """
        truncate_value = False if format_type == "complete" else True
        
        query = df \
            .writeStream \
            .outputMode(output_mode) \
            .format("console") \
            .option("truncate", truncate_value) \
            .trigger(processingTime='5 seconds') \
            .start()
        
        return query
    
    def write_to_hdfs(self, df, hdfs_path, checkpoint_path, coalesce_partitions=1):
        """
        Ghi dữ liệu vào HDFS ở định dạng Parquet
        
        Args:
            df: DataFrame để ghi
            hdfs_path: Đường dẫn HDFS
            checkpoint_path: Đường dẫn checkpoint
        """
        try:
            query = df \
                .coalesce(coalesce_partitions) \
                .writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", hdfs_path) \
                .option("checkpointLocation", checkpoint_path) \
                .partitionBy("year", "month", "day") \
                .trigger(processingTime='5 seconds') \
                .start()
            
            logger.info(f"✅ Đang ghi dữ liệu vào HDFS: {hdfs_path}")
            return query
            
        except Exception as e:
            logger.error(f"❌ Không thể ghi vào HDFS: {e}")
            return None
    
    def write_to_csv(self, df, output_path, checkpoint_path, coalesce_partitions=1):
        """
        Ghi dữ liệu ra CSV file
        
        Args:
            df: DataFrame để ghi
            output_path: Đường dẫn output
            checkpoint_path: Đường dẫn checkpoint
            coalesce_partitions: Số partitions sau khi coalesce (default: 1)
        """
        try:
            # ✅ Convert Path to string nếu cần
            output_path_str = str(output_path).replace("\\", "/")
            checkpoint_path_str = str(checkpoint_path).replace("\\", "/")
            
            query = df \
                .coalesce(coalesce_partitions) \
                .writeStream \
                .outputMode("append") \
                .format("csv") \
                .option("path", output_path_str) \
                .option("checkpointLocation", checkpoint_path_str) \
                .option("header", "true") \
                .trigger(processingTime='5 seconds') \
                .start()
            
            logger.info(f"✅ Đang ghi dữ liệu vào CSV: {output_path_str}")
            return query
            
        except Exception as e:
            logger.error(f"❌ Không thể ghi vào CSV: {e}")
            return None
    
    def write_validation_logs(self, invalid_df, output_path, checkpoint_path):
        """
        Ghi log các records bị drop
        """
        try:
            # ✅ Convert Path to string nếu cần
            output_path_str = str(output_path).replace("\\", "/")
            checkpoint_path_str = str(checkpoint_path).replace("\\", "/")
            
            query = invalid_df \
                .select("Card", "User", "Amount_USD", "invalid_reason", "timestamp") \
                .writeStream \
                .outputMode("append") \
                .format("csv") \
                .option("path", output_path_str) \
                .option("checkpointLocation", checkpoint_path_str) \
                .option("header", "true") \
                .trigger(processingTime='5 seconds') \
                .start()
            
            logger.info(f"✅ Đang ghi validation logs vào: {output_path_str}")
            return query
            
        except Exception as e:
            logger.error(f"❌ Không thể ghi validation logs: {e}")
            return None
    
    def start_streaming(self, output_type="console"):
        """
        Bắt đầu streaming application
        
        Args:
            output_type: Loại output (console, hdfs, csv, all)
        """
        if not self.create_spark_session():
            return
        
        # Đọc từ Kafka (CONSUMER)
        logger.info("🔍 Đang đọc data từ Kafka như một Consumer...")
        kafka_df = self.read_from_kafka()
        if kafka_df is None:
            return
        
        # Xử lý stream
        logger.info("⚙️  Đang xử lý streaming data...")
        valid_df, fraud_df, error_df, invalid_df = self.process_stream(kafka_df)
        
        # Chọn các cột để output
        output_columns = [
            "DateTime_Hour_Key",
            "User",
            "Card",
            "Year",
            "Month",
            "Day",
            "Hour",
            "Day_of_Week",
            "Is_Weekend",
            "Amount_USD",
            "Amount_VND",
            "Exchange_Rate",
            "Use_Chip",
            "Merchant_Name",
            "Merchant_City",
            "Merchant_State",
            "Zip",
            "MCC",
            "Errors",
            "Is_Fraud",
            "Processed_Timestamp"
        ]
        
        valid_output = valid_df.select(output_columns)
        
        queries = []
        
        # Console output
        if output_type in ["console", "all"]:
            logger.info("📺 Khởi động Console Output...")
            query1 = self.write_to_console(
                valid_output.select(
                    "DateTime_Hour_Key",
                    "Card", 
                    "Merchant_Name", 
                    "Amount_USD", 
                    "Amount_VND",      
                    "Exchange_Rate",   
                    "Day_of_Week", 
                    "Is_Weekend"
                ),
                output_mode="append",
                format_type="compact"
            )
            queries.append(query1)
        
        # CSV output
        if output_type in ["csv", "all"]:
            logger.info("📝 Khởi động Valid Transactions CSV Output...")
            query_valid = self.write_to_csv(
                valid_output,
                self.output_path / "valid_transactions",  # ✅ Sử dụng Path object
                self.checkpoint_path / "valid_transactions",
                coalesce_partitions=1
            )
            if query_valid:
                queries.append(query_valid)
            
        # HDFS output
        if output_type in ["hdfs", "all"]:
            logger.info("🗄️  Khởi động HDFS Output...")
            query_hdfs = self.write_to_hdfs(
                valid_output,
                f"{self.hdfs_base_path}/valid",
                f"{self.hdfs_checkpoint_path}/valid",
                coalesce_partitions=1
            )
            if query_hdfs:
                queries.append(query_hdfs)
        
        # Chờ tất cả queries
        try:
            logger.info("🚀 Spark Streaming Consumer đang chạy...")
            logger.info("   Nhấn Ctrl+C để dừng")
            logger.info("-" * 80)
            
            for query in queries:
                query.awaitTermination()
                
        except KeyboardInterrupt:
            logger.info("\n⏹️  Đang dừng Spark Streaming...")
            for query in queries:
                query.stop()
        finally:
            if self.spark:
                self.spark.stop()
            logger.info("✅ Spark Streaming Consumer đã dừng")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Spark Streaming Consumer - Đọc data từ Kafka'
    )
    parser.add_argument(
        '--broker',
        default='kafka-broker:9092',
        help='Kafka broker address (default: kafka-broker:9092)'
    )
    parser.add_argument(
        '--topic',
        default='credit-card-transactions',
        help='Kafka topic (default: credit-card-transactions)'
    )
    parser.add_argument(
        '--output',
        choices=['console', 'csv', 'hdfs', 'all'],
        default='console',
        help='Output type (default: console)'
    )
    
    args = parser.parse_args()
    
    # Tạo consumer
    consumer = SparkStreamingConsumer(
        kafka_bootstrap_servers=args.broker,
        kafka_topic=args.topic
    )
    
    # Bắt đầu streaming
    consumer.start_streaming(output_type=args.output)


if __name__ == "__main__":
    main()

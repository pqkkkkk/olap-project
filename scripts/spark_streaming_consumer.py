"""
Spark Streaming Consumer - Đọc và xử lý dữ liệu từ Kafka
Đóng vai trò như Consumer trong kiến trúc streaming
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp,
    when, date_format, year, month, dayofmonth, hour, minute,
    regexp_replace, trim, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType
)
import logging

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
        
        # Xử lý và làm sạch dữ liệu
        processed_df = parsed_df \
            .withColumn("Amount_USD", 
                       regexp_replace(col("Amount"), "[$,]", "").cast("double")) \
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
                       date_format(col("transaction_date"), "HH:mm:ss"))
        
        # Lọc giao dịch hợp lệ (không có lỗi và không phải fraud)
        valid_transactions = processed_df \
            .filter((col("Errors?").isNull()) | (trim(col("Errors?")) == "")) \
            .filter((col("Is Fraud?") != "Yes"))
        
        # Lọc giao dịch fraud
        fraud_transactions = processed_df \
            .filter(col("Is Fraud?") == "Yes")
        
        return valid_transactions, fraud_transactions
    
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
            .start()
        
        return query
    
    def write_to_hdfs(self, df, hdfs_path, checkpoint_path):
        """
        Ghi dữ liệu vào HDFS ở định dạng Parquet
        
        Args:
            df: DataFrame để ghi
            hdfs_path: Đường dẫn HDFS
            checkpoint_path: Đường dẫn checkpoint
        """
        try:
            query = df \
                .writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", hdfs_path) \
                .option("checkpointLocation", checkpoint_path) \
                .start()
            
            logger.info(f"✅ Đang ghi dữ liệu vào HDFS: {hdfs_path}")
            return query
            
        except Exception as e:
            logger.error(f"❌ Không thể ghi vào HDFS: {e}")
            return None
    
    def write_to_csv(self, df, output_path, checkpoint_path):
        """
        Ghi dữ liệu ra CSV file
        
        Args:
            df: DataFrame để ghi
            output_path: Đường dẫn output
            checkpoint_path: Đường dẫn checkpoint
        """
        try:
            query = df \
                .writeStream \
                .outputMode("append") \
                .format("csv") \
                .option("path", output_path) \
                .option("checkpointLocation", checkpoint_path) \
                .option("header", "true") \
                .start()
            
            logger.info(f"✅ Đang ghi dữ liệu vào CSV: {output_path}")
            return query
            
        except Exception as e:
            logger.error(f"❌ Không thể ghi vào CSV: {e}")
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
        valid_df, fraud_df = self.process_stream(kafka_df)
        
        # Chọn các cột để output
        output_columns = [
            "Card", "transaction_date", "date_str", "time_str",
            "Merchant Name", "Merchant City", "Merchant State",
            "Amount_USD", "User", "Is Fraud?"
        ]
        
        valid_output = valid_df.select(output_columns)
        fraud_output = fraud_df.select(output_columns)
        
        queries = []
        
        # Console output
        if output_type in ["console", "all"]:
            logger.info("📺 Khởi động Console Output...")
            query1 = self.write_to_console(
                valid_output.select("Card", "Merchant Name", "Amount_USD", "date_str", "time_str"),
                output_mode="append",
                format_type="compact"
            )
            queries.append(query1)
        
        # CSV output
        if output_type in ["csv", "all"]:
            logger.info("📁 Khởi động CSV Output...")
            query2 = self.write_to_csv(
                valid_output,
                "data/output/valid_transactions",
                "data/checkpoint/valid"
            )
            query3 = self.write_to_csv(
                fraud_output,
                "data/output/fraud_transactions",
                "data/checkpoint/fraud"
            )
            if query2:
                queries.append(query2)
            if query3:
                queries.append(query3)
        
        # HDFS output
        if output_type in ["hdfs", "all"]:
            logger.info("🗄️  Khởi động HDFS Output...")
            query4 = self.write_to_hdfs(
                valid_output,
                "hdfs://namenode:8020/transactions/valid",
                "hdfs://namenode:8020/checkpoint/valid"
            )
            query5 = self.write_to_hdfs(
                fraud_output,
                "hdfs://namenode:8020/transactions/fraud",
                "hdfs://namenode:8020/checkpoint/fraud"
            )
            if query4:
                queries.append(query4)
            if query5:
                queries.append(query5)
        
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

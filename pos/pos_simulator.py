"""
POS Simulator - Kafka Producer
Giả lập máy POS gửi giao dịch thẻ tín dụng đến Kafka
"""

import csv
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

RAW_TRANSACTIONS_PATH = '../sample_data/raw_transactions.csv'
KAKFA_TOPIC = 'credit-card-transactions'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9094'

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class POSSimulator:
    def __init__(self, 
                 bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                 topic=KAKFA_TOPIC,
                 csv_file=RAW_TRANSACTIONS_PATH):
        """
        Khởi tạo POS Simulator
        
        Args:
            bootstrap_servers: Địa chỉ Kafka broker
            topic: Topic để gửi transactions
            csv_file: File CSV chứa dữ liệu giao dịch
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.csv_file = csv_file
        self.producer = None
        
    def create_producer(self):
        """Tạo Kafka Producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Đảm bảo message được ghi vào tất cả replicas
                retries=3,   # Retry khi gửi thất bại
                max_in_flight_requests_per_connection=1  # Đảm bảo thứ tự messages
            )
            logger.info(f"✅ Kafka Producer đã kết nối đến {self.bootstrap_servers}")
            return True
        except Exception as e:
            logger.error(f"❌ Không thể kết nối Kafka Producer: {e}")
            return False
    
    def read_csv_data(self):
        """Đọc dữ liệu từ CSV file"""
        transactions = []
        try:
            with open(self.csv_file, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                for row in csv_reader:
                    transactions.append(row)
            logger.info(f"✅ Đã đọc {len(transactions)} giao dịch từ {self.csv_file}")
            return transactions
        except FileNotFoundError:
            logger.error(f"❌ Không tìm thấy file: {self.csv_file}")
            return []
        except Exception as e:
            logger.error(f"❌ Lỗi khi đọc CSV: {e}")
            return []
    
    def create_timestamp_from_transaction(self, transaction):
        """
        TẠO TIMESTAMP TỪ CSV DATA (Year, Month, Day, Time)
        
        Args:
            transaction: Dictionary chứa transaction data từ CSV
            
        Returns:
            timestamp: String ISO 8601 format (YYYY-MM-DDTHH:mm:ss)
        """
        try:
            # Lấy Year, Month, Day từ CSV
            year = str(transaction.get('Year', '')).strip()
            month = str(transaction.get('Month', '')).strip()
            day = str(transaction.get('Day', '')).strip()
            time_str = str(transaction.get('Time', '00:00:00')).strip()
            
            # Validate dữ liệu
            if not all([year, month, day]):
                logger.warning(f"⚠️  Thiếu Year/Month/Day, dùng timestamp hiện tại")
                return datetime.now().isoformat()
            
            # FORMAT: YYYY-MM-DDTHH:mm:ss
            timestamp = f"{year}-{int(month):02d}-{int(day):02d}T{time_str}"
            
            logger.debug(f"✅ Created timestamp: {timestamp} (Year={year}, Month={month}, Day={day}, Time={time_str})")
            return timestamp
            
        except Exception as e:
            logger.warning(f"⚠️  Lỗi tạo timestamp từ CSV: {e}")
            logger.warning(f"   Transaction: {transaction}")
            return datetime.now().isoformat()
    

    def send_transaction(self, transaction):
        """
        Gửi một giao dịch đến Kafka
        
        Args:
            transaction: Dictionary chứa thông tin giao dịch
        """
        try:
            # Tạo key từ Card number để partition theo card
            key = transaction.get('Card', '')

            # Chỉnh Year, Month, Day fields theo ngày hiện tại
            now = datetime.now()
            transaction['Year'] = now.year
            transaction['Month'] = now.month
            transaction['Day'] = now.day
            
            # Thêm timestamp
            transaction['timestamp'] = self.create_timestamp_from_transaction(transaction)
            
            # Gửi message
            future = self.producer.send(
                self.topic,
                key=key,
                value=transaction
            )
            
            # Chờ xác nhận
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"📤 Giao dịch đã gửi - "
                f"Card: {transaction.get('Card', 'N/A')[:8]}*** | "
                f"Amount: ${transaction.get('Amount', 'N/A')} | "
                f"Merchant: {transaction.get('Merchant Name', 'N/A')} | "
                f"Date: {transaction.get('Day')}/{transaction.get('Month')}/{transaction.get('Year')} | "
                f"Timestamp: {transaction.get('timestamp')} | "
                f"Partition: {record_metadata.partition} | "
                f"Offset: {record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            logger.error(f"❌ Lỗi Kafka khi gửi transaction: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Lỗi khi gửi transaction: {e}")
            return False
    
    def start_simulation(self, min_delay=1, max_delay=5, loop=False):
        """
        Bắt đầu giả lập gửi giao dịch
        
        Args:
            min_delay: Thời gian delay tối thiểu (giây)
            max_delay: Thời gian delay tối đa (giây)
            loop: Có lặp lại khi hết data không
        """
        if not self.create_producer():
            return
        
        transactions = self.read_csv_data()
        if not transactions:
            logger.error("❌ Không có dữ liệu để gửi")
            return
        
        logger.info(f"🚀 Bắt đầu giả lập POS - Topic: {self.topic}")
        logger.info(f"⏱️  Delay ngẫu nhiên: {min_delay}s - {max_delay}s")
        logger.info("-" * 80)
        
        try:
            count = 0
            while True:
                for transaction in transactions:
                    # Gửi transaction
                    if self.send_transaction(transaction):
                        count += 1
                    
                    # Random delay giữa các giao dịch (giả lập thời gian thực)
                    delay = random.uniform(min_delay, max_delay)
                    time.sleep(delay)
                
                # Nếu không loop thì dừng
                if not loop:
                    break
                
                logger.info(f"🔄 Đã gửi hết {count} giao dịch, bắt đầu lại từ đầu...")
                
        except KeyboardInterrupt:
            logger.info("\n⏹️  Dừng simulation...")
        finally:
            self.close()
            logger.info(f"✅ Đã gửi tổng cộng {count} giao dịch")
    
    def close(self):
        """Đóng Kafka Producer"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("🔒 Đã đóng Kafka Producer")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='POS Simulator - Kafka Producer')
    parser.add_argument(
        '--broker',
        default='localhost:9094',
        help='Kafka broker address (default: localhost:9094)'
    )
    parser.add_argument(
        '--topic',
        default='credit-card-transactions',
        help='Kafka topic (default: credit-card-transactions)'
    )
    parser.add_argument(
        '--csv',
        default=RAW_TRANSACTIONS_PATH,
        help='CSV file path (default: sample_data/raw_transactions.csv)'
    )
    parser.add_argument(
        '--min-delay',
        type=float,
        default=1.0,
        help='Minimum delay between transactions in seconds (default: 1.0)'
    )
    parser.add_argument(
        '--max-delay',
        type=float,
        default=5.0,
        help='Maximum delay between transactions in seconds (default: 5.0)'
    )
    parser.add_argument(
        '--loop',
        action='store_true',
        help='Loop through data continuously'
    )
    
    args = parser.parse_args()
    
    # Tạo simulator
    simulator = POSSimulator(
        bootstrap_servers=args.broker,
        topic=args.topic,
        csv_file=args.csv
    )
    
    # Bắt đầu simulation
    simulator.start_simulation(
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        loop=args.loop
    )


if __name__ == "__main__":
    main()

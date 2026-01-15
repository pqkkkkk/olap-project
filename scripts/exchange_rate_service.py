"""
Exchange Rate Service - Lấy tỉ giá USD/VND từ VCB
"""

import requests
from datetime import datetime
from bs4 import BeautifulSoup
import logging

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import time
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

logger = logging.getLogger(__name__)


class ExchangeRateService:
    """Service lấy tỉ giá từ VCB với fallback mechanism"""
    
    def __init__(self):
        self.cache = {}  # Cache tỉ giá theo ngày
        self.default_rate = 25057.0  # Tỉ giá mặc định
        
        # ✅ 1. Research VCB API endpoint
        self.vcb_api_url = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx"
        self.vcb_web_url = "https://www.vietcombank.com.vn/vi-VN/KHCN/Cong-cu-Tien-ich/Ty-gia"
    
    def get_exchange_rate(self, date_str=None):
        """
        Lấy tỉ giá USD/VND cho ngày cụ thể
        
        Args:
            date_str: Ngày (YYYY-MM-DD), None = hôm nay
            
        Returns:
            float: Tỉ giá USD/VND
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        # ✅ 5. Check cache
        if date_str in self.cache:
            logger.info(f"📦 Cache hit cho {date_str}: {self.cache[date_str]:,.0f}")
            return self.cache[date_str]
        
        # ✅ 2. Primary: VCB API
        rate = self._fetch_from_api()
        if rate:
            self.cache[date_str] = rate
            return rate
        
        # ✅ 3. Fallback: Web scraping (Selenium)
        rate = self._fetch_from_web()
        if rate:
            self.cache[date_str] = rate
            return rate
        
        # ✅ 4. Use default rate
        logger.warning(f"⚠️ Sử dụng tỉ giá mặc định: {self.default_rate:,.0f}")
        self.cache[date_str] = self.default_rate
        return self.default_rate
    
    def _fetch_from_api(self):
        """
        ✅ 2. Lấy tỉ giá từ VCB API (XML)
        """
        try:
            logger.info("🔍 Đang lấy tỉ giá từ VCB API...")
            
            response = requests.get(self.vcb_api_url, timeout=5)
            response.raise_for_status()
            
            # Parse XML
            from xml.etree import ElementTree as ET
            root = ET.fromstring(response.content)
            
            # Tìm USD rate
            for exrate in root.findall('.//Exrate'):
                if exrate.get('CurrencyCode') == 'USD':
                    # Lấy Transfer rate (tỉ giá chuyển khoản)
                    transfer = exrate.get('Transfer')
                    if transfer:
                        rate = float(transfer.replace(',', ''))
                        logger.info(f"✅ VCB API: {rate:,.0f} VND/USD")
                        return rate
            
            raise ValueError("USD rate not found in API response")
            
        except requests.Timeout:
            logger.warning("⏱️ VCB API timeout")
            return None
        except requests.RequestException as e:
            logger.warning(f"⚠️ VCB API error: {e}")
            return None
        except Exception as e:
            logger.warning(f"⚠️ Parse API error: {e}")
            return None
    
    def _fetch_from_web(self):
        """
        ✅ 3. Fallback: Web scraping từ VCB website (Selenium)
        Sử dụng Selenium vì website load dữ liệu bằng JavaScript
        """
        if not SELENIUM_AVAILABLE:
            logger.warning("⚠️ Selenium not available, skipping web scraping")
            return None
        
        try:
            logger.info("🌐 Đang scrape tỉ giá từ VCB website (Selenium)...")
            
            # Setup Chrome options
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-logging")
            chrome_options.add_argument("--log-level=3")
            chrome_options.add_argument(
                "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            
            # Initialize driver
            driver = webdriver.Chrome(options=chrome_options)
            
            try:
                # Navigate to page
                driver.get(self.vcb_web_url)
                
                # Wait for table to load (max 10 seconds)
                logger.info("⏳ Waiting for table to load...")
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.TAG_NAME, "table"))
                )
                
                time.sleep(2)  # Extra wait for JS to fully render
                
                # Get all tables
                tables = driver.find_elements(By.TAG_NAME, "table")
                logger.info(f"📊 Found {len(tables)} tables")
                
                # Search for USD in tables
                for table_idx, table in enumerate(tables):
                    rows = table.find_elements(By.TAG_NAME, "tr")
                    
                    for row_idx, row in enumerate(rows):
                        cells = row.find_elements(By.TAG_NAME, "td")
                        
                        # Skip header row
                        if len(cells) >= 4 and row_idx > 0:
                            try:
                                currency = cells[0].text.strip()
                                
                                if currency == "USD":
                                    # Extract rate from cell 3 (Mua chuyển khoản)
                                    transfer_text = cells[3].text.strip()
                                    cleaned = transfer_text.replace(",", "")
                                    rate = float(cleaned)
                                    
                                    # Validate
                                    if 20000 <= rate <= 30000:
                                        logger.info(f"✅ Web scraping: {rate:,.0f} VND/USD")
                                        return rate
                                    else:
                                        logger.warning(f"⚠️ Rate {rate:,.0f} out of range")
                                        return None
                            except (ValueError, IndexError) as e:
                                logger.warning(f"⚠️ Error parsing row {row_idx}: {e}")
                                continue
                
                logger.warning("⚠️ USD not found in any table")
                return None
                
            finally:
                driver.quit()
        
        except Exception as e:
            logger.warning(f"⚠️ Web scraping error: {e}")
            return None
    
    def get_cached_rates(self):
        """Trả về toàn bộ cache"""
        return self.cache.copy()
    
    def clear_cache(self):
        """Xóa cache"""
        self.cache.clear()
        logger.info("🗑️ Cache đã được xóa")


# Test function
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    service = ExchangeRateService()
    
    print("\n" + "="*60)
    print("TEST EXCHANGE RATE SERVICE")
    print("="*60)
    
    # Test 1: Lấy tỉ giá hôm nay
    rate = service.get_exchange_rate()
    print(f"\n✅ Tỉ giá hôm nay: {rate:,.0f} VND/USD")
    
    # Test 2: Lấy lại (từ cache)
    rate2 = service.get_exchange_rate()
    print(f"✅ Tỉ giá (cached): {rate2:,.0f} VND/USD")
    
    # Test 3: Convert 100 USD
    usd_amount = 100
    vnd_amount = usd_amount * rate
    print(f"\n💵 {usd_amount} USD = {vnd_amount:,.0f} VND")
    
    # Test 4: Hiển thị cache
    print(f"\n📦 Cache: {service.get_cached_rates()}")
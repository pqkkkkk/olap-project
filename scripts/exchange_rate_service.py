"""
Exchange Rate Service - Lấy tỉ giá USD/VND từ VCB
"""

import requests
from datetime import datetime
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)


class ExchangeRateService:
    """Service lấy tỉ giá từ VCB với fallback mechanism"""
    
    def __init__(self):
        self.cache = {}  # Cache tỉ giá theo ngày
        self.default_rate = 25057.0  # Tỉ giá mặc định
        
        # ✅ 1. Research VCB API endpoint
        self.vcb_api_url = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx"
        self.vcb_web_url = "https://portal.vietcombank.com.vn/Personal/TI-GIA/Pages/default.aspx"
    
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
        
        # ✅ 3. Fallback: Web scraping
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
        ✅ 3. Fallback: Web scraping từ VCB website
        """
        try:
            logger.info("🌐 Đang scrape tỉ giá từ VCB website...")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(self.vcb_web_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Tìm bảng tỉ giá
            # Structure có thể thay đổi - cần kiểm tra lại
            table = soup.find('table', {'id': 'ctl00_Content_ExrateView_GridView1'})
            if not table:
                table = soup.find('table', class_='table')
            
            if table:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        # Cột 0: Currency code
                        currency = cells[0].text.strip()
                        if currency == 'USD':
                            # Cột 3: Transfer rate
                            transfer_text = cells[3].text.strip()
                            rate = float(transfer_text.replace(',', ''))
                            logger.info(f"✅ Web scraping: {rate:,.0f} VND/USD")
                            return rate
            
            raise ValueError("USD rate not found in webpage")
            
        except requests.Timeout:
            logger.warning("⏱️ Web scraping timeout")
            return None
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
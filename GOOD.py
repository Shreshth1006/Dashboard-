import scrapy
import re
import json
from datetime import datetime
from scrapy.http import HtmlResponse
import hashlib

class FuelpricesRobustSpider(scrapy.Spider):
    """
    Robust scraper that:
    1. Tries direct scraping first (FREE)
    2. Falls back to ScrapFly if blocked (PAID)
    3. Pushes data even if some fuel types fail (sets null for failures)
    4. Continues scraping all fuel types even after failures
    """
    name = "fuelprices_robust"
    allowed_domains = ["www.goodreturns.in", "api.scrapfly.io"]
    
    custom_settings = {
        'CONCURRENT_REQUESTS': 2,
        'DOWNLOAD_TIMEOUT': 120,
        'DOWNLOAD_DELAY': 2,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 8,
        'RETRY_ENABLED': True,
        'RETRY_HTTP_CODES': [403, 429, 500, 502, 503, 504],
        'RETRY_TIMES': 1,
        'ROBOTSTXT_OBEY': False,
        'COOKIES_ENABLED': True,
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    }

    API_KEY = 'scp-live-c43aeceef9b4409886822b4f0e6e49c4'
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session_id = hashlib.md5(f"fuel_{datetime.now().date()}".encode()).hexdigest()[:16]
        self.proxy_used_count = 0
        self.direct_success_count = 0
        self.partial_data_count = 0
        self.complete_data_count = 0

    def start_requests(self):
        cities = [
            "Jaipur", "Lucknow", "Bhopal", "Patna", "Chandigarh", "Guwahati",
            "Bhubaneswar", "Raipur", "Hyderabad", "Chennai", "Bangalore",
            "Panjim", "Mumbai", "Kolkata", "Pune", "Ahmedabad", "Indore",
            "Surat", "Ghaziabad", "Noida", "Visakhapatnam", "Nainital",
            "Shimla", "Amritsar", "Dehradun", "Darjeeling", "Howrah",
            "Kullu", "Kanchipuram", "Faridabad", "Gurgaon", "Ambala",
            "Jalandhar", "Kottayam", "Nagpur", "Rajkot", "Varanasi",
            "Kozhikode", "Meerut", "Coimbatore", "Vadodara", "New Delhi"
        ]
        
        # Uncomment for testing
        # cities = ["Jaipur", "Ghaziabad", "Mumbai"]
        
        for city in cities:
            city_slug = city.replace(' ', '-').lower()
            
            # Initialize item with null values
            item = {
                'city': city,
                'date': None,
                'petrol': None,
                'diesel': None,
                'lpg_domestic': None,
                'lpg_commercial': None,
                'cng': None,
            }
            
            url = f"https://www.goodreturns.in/petrol-price-in-{city_slug}.html"
            
            # Try DIRECT request first
            yield scrapy.Request(
                url=url,
                callback=self.parse_petrol,
                cb_kwargs={'item': item, 'city': city, 'city_slug': city_slug, 'use_proxy': False},
                errback=self.handle_petrol_failure,
                meta={'item': item, 'city': city, 'city_slug': city_slug},
                dont_filter=True
            )

    def handle_petrol_failure(self, failure):
        """If petrol scrape fails, try with proxy then continue to diesel"""
        item = failure.request.meta.get('item')
        city = failure.request.meta.get('city')
        city_slug = failure.request.meta.get('city_slug')
        
        self.logger.warning(f"⚠ Petrol failed for {city}, trying proxy...")
        
        url = f"https://www.goodreturns.in/petrol-price-in-{city_slug}.html"
        
        yield scrapy.Request(
            url=self.get_scrapfly_url(url),
            callback=self.parse_petrol,
            cb_kwargs={'item': item, 'city': city, 'city_slug': city_slug, 'use_proxy': True},
            errback=self.skip_to_diesel,
            meta={'item': item, 'city': city, 'city_slug': city_slug},
            dont_filter=True
        )

    def skip_to_diesel(self, failure):
        """Petrol completely failed, skip to diesel with null petrol"""
        item = failure.request.meta.get('item')
        city = failure.request.meta.get('city')
        city_slug = failure.request.meta.get('city_slug')
        
        self.logger.error(f"✗ Petrol completely failed for {city}, continuing with null")
        item['petrol'] = None
        
        # Continue to diesel
        diesel_url = f"https://www.goodreturns.in/diesel-price-in-{city_slug}.html"
        yield scrapy.Request(
            url=diesel_url,
            callback=self.parse_diesel,
            cb_kwargs={'item': item, 'city': city, 'city_slug': city_slug, 'use_proxy': False},
            errback=self.handle_diesel_failure,
            meta={'item': item, 'city': city, 'city_slug': city_slug},
            dont_filter=True
        )

    def parse_petrol(self, response, item, city, city_slug, use_proxy):
        """Parse petrol price"""
        try:
            if use_proxy:
                self.proxy_used_count += 1
                data = json.loads(response.text)
                html = data['result']['content']
                response = HtmlResponse(url=response.url, body=html, encoding='utf-8')
            else:
                self.direct_success_count += 1
            
            # Extract date (only once from petrol page)
            if not item['date']:
                item['date'] = self.extract_date(response)
            
            item['petrol'] = self.extract_fuel_price(response)
            self.logger.info(f"✓ Petrol scraped for {city}: {item['petrol']}")
            
        except Exception as e:
            self.logger.error(f"✗ Error parsing petrol for {city}: {e}")
            item['petrol'] = None
        
        # Always continue to diesel
        diesel_url = f"https://www.goodreturns.in/diesel-price-in-{city_slug}.html"
        
        if use_proxy:
            yield scrapy.Request(
                url=self.get_scrapfly_url(diesel_url),
                callback=self.parse_diesel,
                cb_kwargs={'item': item, 'city': city, 'city_slug': city_slug, 'use_proxy': True},
                errback=self.handle_diesel_failure,
                meta={'item': item, 'city': city, 'city_slug': city_slug},
                dont_filter=True
            )
        else:
            yield scrapy.Request(
                url=diesel_url,
                callback=self.parse_diesel,
                cb_kwargs={'item': item, 'city': city, 'city_slug': city_slug, 'use_proxy': False},
                errback=self.handle_diesel_failure,
                meta={'item': item, 'city': city, 'city_slug': city_slug},
                dont_filter=True
            )

    def handle_diesel_failure(self, failure):
        """Try diesel with proxy, then skip to LPG"""
        item = failure.request.meta.get('item')
        city = failure.request.meta.get('city')
        city_slug = failure.request.meta.get('city_slug')
        
        self.logger.warning(f"⚠ Diesel failed for {city}, trying proxy...")
        
        url = f"https://www.goodreturns.in/diesel-price-in-{city_slug}.html"
        
        yield scrapy.Request(
            url=self.get_scrapfly_url(url),
            callback=self.parse_diesel,
            cb_kwargs={'item': item, 'city': city, 'city_slug': city_slug, 'use_proxy': True},
            errback=self.skip_to_lpg,
            meta={'item': item, 'city': city, 'city_slug': city_slug},
            dont_filter=True
        )

    def skip_to_lpg(self, failure):
        """Diesel failed, continue to LPG"""
        item = failure.request.meta.get('item')
        city = failure.request.meta.get('city')
        city_slug = failure.request.meta.get('city_slug')
        
        self.logger.error(f"✗ Diesel completely failed for {city}, continuing with null")
        item['diesel'] = None
        
        lpg_url = f"https://www.goodreturns.in/lpg-price-in-{city_slug}.html"
        yield scrapy.Request(
            url=lpg_url,
            callback=self.parse_lpg,
            cb_kwargs={'item': item, 'city': city, 'city_slug': city_slug, 'use_proxy': False},
            errback=self.handle_lpg_failure,
            meta={'item': item, 'city': city, 'city_slug': city_slug},
            dont_filter=True
        )

    def parse_diesel(self, response, item, city, city_slug, use_proxy):
        """Parse diesel price"""
        try:
            if use_proxy:
                self.proxy_used_count += 1
                data = json.loads(response.text)
                html = data['result']['content']
                response = HtmlResponse(url=response.url, body=html, encoding='utf-8')
            else:
                self.direct_success_count += 1
            
            item['diesel'] = self.extract_fuel_price(response)
            self.logger.info(f"✓ Diesel scraped for {city}: {item['diesel']}")
            
        except Exception as e:
            self.logger.error(f"✗ Error parsing diesel for {city}: {e}")
            item['diesel'] = None
        
        # Always continue to LPG
        lpg_url = f"https://www.goodreturns.in/lpg-price-in-{city_slug}.html"
        
        if use_proxy:
            yield scrapy.Request(
                url=self.get_scrapfly_url(lpg_url),
                callback=self.parse_lpg,
                cb_kwargs={'item': item, 'city': city, 'city_slug': city_slug, 'use_proxy': True},
                errback=self.handle_lpg_failure,
                meta={'item': item, 'city': city, 'city_slug': city_slug},
                dont_filter=True
            )
        else:
            yield scrapy.Request(
                url=lpg_url,
                callback=self.parse_lpg,
                cb_kwargs={'item': item, 'city': city, 'city_slug': city_slug, 'use_proxy': False},
                errback=self.handle_lpg_failure,
                meta={'item': item, 'city': city, 'city_slug': city_slug},
                dont_filter=True
            )

    def handle_lpg_failure(self, failure):
        """Try LPG with proxy, then skip to CNG"""
        item = failure.request.meta.get('item')
        city = failure.request.meta.get('city')
        city_slug = failure.request.meta.get('city_slug')
        
        self.logger.warning(f"⚠ LPG failed for {city}, trying proxy...")
        
        url = f"https://www.goodreturns.in/lpg-price-in-{city_slug}.html"
        
        yield scrapy.Request(
            url=self.get_scrapfly_url(url),
            callback=self.parse_lpg,
            cb_kwargs={'item': item, 'city': city, 'city_slug': city_slug, 'use_proxy': True},
            errback=self.skip_to_cng,
            meta={'item': item, 'city': city, 'city_slug': city_slug},
            dont_filter=True
        )

    def skip_to_cng(self, failure):
        """LPG failed, continue to CNG"""
        item = failure.request.meta.get('item')
        city = failure.request.meta.get('city')
        city_slug = failure.request.meta.get('city_slug')
        
        self.logger.error(f"✗ LPG completely failed for {city}, continuing with null")
        item['lpg_domestic'] = None
        item['lpg_commercial'] = None
        
        cng_url = f"https://www.goodreturns.in/cng-price-in-{city_slug}.html"
        yield scrapy.Request(
            url=cng_url,
            callback=self.parse_cng,
            cb_kwargs={'item': item, 'city': city, 'use_proxy': False},
            errback=self.handle_cng_failure,
            meta={'item': item, 'city': city, 'city_slug': city_slug},
            dont_filter=True
        )

    def parse_lpg(self, response, item, city, city_slug, use_proxy):
        """Parse LPG prices"""
        try:
            if use_proxy:
                self.proxy_used_count += 1
                data = json.loads(response.text)
                html = data['result']['content']
                response = HtmlResponse(url=response.url, body=html, encoding='utf-8')
            else:
                self.direct_success_count += 1
            
            prices = response.xpath(
                '//table[contains(@class,"gd-fuel-table-list")]//tr/td[2]/text()'
            ).getall()
            
            clean_prices = [
                float(p.replace('₹', '').replace(',', '').strip())
                for p in prices
                if p.strip() and re.search(r'\d+', p)
            ]
            
            if len(clean_prices) >= 2:
                item['lpg_domestic'] = self.format_price(clean_prices[0])
                item['lpg_commercial'] = self.format_price(clean_prices[1])
                self.logger.info(f"✓ LPG scraped for {city}: D={item['lpg_domestic']}, C={item['lpg_commercial']}")
            else:
                item['lpg_domestic'] = None
                item['lpg_commercial'] = None
                self.logger.warning(f"⚠ LPG prices not found for {city}")
            
        except Exception as e:
            self.logger.error(f"✗ Error parsing LPG for {city}: {e}")
            item['lpg_domestic'] = None
            item['lpg_commercial'] = None
        
        # Always continue to CNG
        cng_url = f"https://www.goodreturns.in/cng-price-in-{city_slug}.html"
        
        if use_proxy:
            yield scrapy.Request(
                url=self.get_scrapfly_url(cng_url),
                callback=self.parse_cng,
                cb_kwargs={'item': item, 'city': city, 'use_proxy': True},
                errback=self.handle_cng_failure,
                meta={'item': item, 'city': city},
                dont_filter=True
            )
        else:
            yield scrapy.Request(
                url=cng_url,
                callback=self.parse_cng,
                cb_kwargs={'item': item, 'city': city, 'use_proxy': False},
                errback=self.handle_cng_failure,
                meta={'item': item, 'city': city},
                dont_filter=True
            )

    def handle_cng_failure(self, failure):
        """Try CNG with proxy, then finalize"""
        item = failure.request.meta.get('item')
        city = failure.request.meta.get('city')
        city_slug = failure.request.meta.get('city_slug')
        
        self.logger.warning(f"⚠ CNG failed for {city}, trying proxy...")
        
        url = f"https://www.goodreturns.in/cng-price-in-{city_slug}.html"
        
        yield scrapy.Request(
            url=self.get_scrapfly_url(url),
            callback=self.parse_cng,
            cb_kwargs={'item': item, 'city': city, 'use_proxy': True},
            errback=self.finalize_and_push,
            meta={'item': item, 'city': city},
            dont_filter=True
        )

    def finalize_and_push(self, failure):
        """CNG failed, push data with null CNG"""
        item = failure.request.meta.get('item')
        city = failure.request.meta.get('city')
        
        self.logger.error(f"✗ CNG completely failed for {city}, pushing with null")
        item['cng'] = None
        
        # Push data even with nulls
        yield from self.push_data(item, city)

    def parse_cng(self, response, item, city, use_proxy):
        """Parse CNG price and finalize"""
        try:
            if use_proxy:
                self.proxy_used_count += 1
                data = json.loads(response.text)
                html = data['result']['content']
                response = HtmlResponse(url=response.url, body=html, encoding='utf-8')
            else:
                self.direct_success_count += 1
            
            item['cng'] = self.extract_fuel_price(response)
            self.logger.info(f"✓ CNG scraped for {city}: {item['cng']}")
            
        except Exception as e:
            self.logger.error(f"✗ Error parsing CNG for {city}: {e}")
            item['cng'] = None
        
        # Always push data (even with nulls)
        yield from self.push_data(item, city)

    def push_data(self, item, city):
        """Push data to APIs - even if some values are null"""
        
        # Count completeness
        null_count = sum(1 for v in item.values() if v is None)
        if null_count == 0:
            self.complete_data_count += 1
            self.logger.info(f"✓✓ {city} COMPLETE: {item}")
        else:
            self.partial_data_count += 1
            self.logger.warning(f"⚠ {city} PARTIAL ({null_count} nulls): {item}")
        
        # Post to production API
        headers = {'Content-Type': 'application/json'}
        prod_url = 'https://services.timesofindia.com/ufs-utility/utility/fuels/data/upload'
        
        yield scrapy.Request(
            prod_url,
            method='POST',
            body=json.dumps(item),
            headers=headers,
            callback=self.handle_prod_success,
            errback=self.handle_prod_error,
            cb_kwargs={'item': item, 'city': city},
            dont_filter=True
        )

    def handle_prod_success(self, response, item, city):
        """Production success - now post to staging"""
        self.logger.info(f"✓ Production API: {city}")
        
        headers = {'Content-Type': 'application/json'}
        staging_url = 'https://nprelease.indiatimes.com/ufs-utility/utility/fuels/data/upload'
        
        return scrapy.Request(
            staging_url,
            method='POST',
            body=json.dumps(item),
            headers=headers,
            callback=self.handle_staging_success,
            errback=self.handle_staging_error,
            cb_kwargs={'city': city},
            dont_filter=True
        )

    def handle_prod_error(self, failure, city):
        self.logger.error(f"✗ Production API failed for {city}: {failure}")

    def handle_staging_success(self, response, city):
        self.logger.info(f"✓ Staging API: {city}")

    def handle_staging_error(self, failure, city):
        self.logger.error(f"✗ Staging API failed for {city}: {failure}")

    def get_scrapfly_url(self, url):
        """Generate ScrapFly API URL"""
        import urllib.parse
        
        params = {
            'key': self.API_KEY,
            'url': url,
            'render_js': 'true',
            'session': self.session_id,
            'country': 'in',
            'asp': 'true',
            'cache': 'true',
            'cache_ttl': 7200,
        }
        
        return f"https://api.scrapfly.io/scrape?{urllib.parse.urlencode(params)}"

    def extract_date(self, response):
        """Extract and format date"""
        raw_date = response.xpath(
            '//div[contains(@class, "gd-fuel-updated-date")]/text()'
        ).get()
        
        if raw_date:
            get_date = raw_date.strip()
        else:
            today = datetime.now()
            day = today.day
            if 4 <= day <= 20 or 24 <= day <= 30:
                suffix = "th"
            else:
                suffix = ["st", "nd", "rd"][day % 10 - 1]
            get_date = today.strftime(f"%d{suffix} %b, %Y")
        
        cleaned_date = re.sub(r'(st|nd|rd|th)', '', get_date)
        
        try:
            return datetime.strptime(cleaned_date.strip(), '%d %b, %Y').strftime('%d-%m-%Y')
        except Exception as e:
            return datetime.now().strftime('%d-%m-%Y')

    def extract_fuel_price(self, response):
        """Extract fuel price"""
        price_data = response.xpath(
            '//div[contains(@class, "gd-fuel-priceblock")]//div[contains(@class, "gd-fuel-price")]/text()'
        ).getall()
        
        price_text = ''.join(price_data).strip()
        match = re.search(r'(\d+\.\d+)', price_text)
        
        if match:
            return self.format_price(float(match.group()))
        else:
            return None

    def format_price(self, amount):
        if amount is None:
            return None
        return f"{float(amount):.2f}"

    def closed(self, reason):
        """Print statistics when spider closes"""
        self.logger.info("=" * 70)
        self.logger.info(f"SCRAPING COMPLETE - Reason: {reason}")
        self.logger.info(f"Complete data (all fuels): {self.complete_data_count} cities")
        self.logger.info(f"Partial data (some nulls): {self.partial_data_count} cities")
        self.logger.info(f"Direct requests succeeded: {self.direct_success_count}")
        self.logger.info(f"Proxy requests used: {self.proxy_used_count}")
        self.logger.info(f"Estimated API cost: ${self.proxy_used_count * 0.0015:.4f}")
        self.logger.info("=" * 70)
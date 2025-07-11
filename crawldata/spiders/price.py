import scrapy, re , json, requests, hashlib
from datetime import datetime
from scrapy.selector import Selector
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs, urljoin


class CrawlerSpider(scrapy.Spider):
    name = 'price'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.5',
        'content-type': 'text/html; charset=EUC-JP'
    }
    base_url = 'https://www.jalan.net/'
    start_url = 'https://www.jalan.net/jalan/common/script/car_rental_master.js'
    params = {
        'roomCount': 1,
        'adultNum': 1,
        'distCd': '01',
        'child5Num': '',
        'mealType': '',
        'photo': 1,
        'child4Num': '',
        'roomCrack': 100000,
        'dateUndecided': 1,
        'child3Num': '',
        'afCd': '',
        'child1Num': '',
        'child2Num': '',
        'childPriceFlg': '0,0,0,0,0',
        'rootCd': '041'
    }
    def start_requests(self):
        yield scrapy.Request(self.start_url, callback=self.parse)
        
    def parse(self, response):
        ken_pattern = r'ken\[\d+\] = new KenData\("([^"]+)",\s*"(\d+)",\s*new Array\((.*?)\);\s*'
        matches = re.findall(ken_pattern, response.text, re.DOTALL)
        if matches:
            prefectures = {}
            for match in matches:
                prefecture_code = match[1]
                lrg_data = match[2]
                lrg_entries = self.extract_lrg_codes(lrg_data)
                prefectures[prefecture_code] = lrg_entries

            for index in range(1, 10):
                self.params['adultNum'] = index
                self.params['roomCrack'] = index * 100000

                for prefecture_code in prefectures:
                    for area in prefectures[prefecture_code]:
                        url = f"{self.base_url}{prefecture_code}/LRG_{area}/?{urlencode(self.params)}"
                        yield scrapy.Request(url, callback=self.parse_category)
        else:
            self.logger.error("No prefecture data found.")
    
    def parse_category(self, response):
        for href in response.xpath('//div[@id="jsiInnList"]//li//a[@class="jlnpc-yadoCassette__link"]/@href').getall():
            yield scrapy.Request(response.urljoin(href), callback=self.parse_hotel)
    
    def parse_hotel(self, response):
        item={}

        hotel_id = response.url.split('/')[-2]
        if hotel_id:
            item['hotel_id'] = hotel_id.replace('yad', '')
        
        cur_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        process_hash_id = hashlib.md5(cur_time.encode()).hexdigest()
        item['last_update_date'] = cur_time
        item['process_hash_id'] = process_hash_id

        accommodation_url = response.xpath('//div[@id="yado_header_tab_menu"]//li[@class="tab_02"]/a/@href').get()
        if accommodation_url:
            self.parse_rooms(response.urljoin(accommodation_url), item)
            yield item

    def parse_rooms(self, url, item):
        for adult_count in range(1, 6):
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            query_params['adultNum'] = [f'{adult_count}']
            query_params['roomCrack'] = [f'{100000 * adult_count}']
            new_query_string = urlencode(query_params, doseq=True)
            new_url = urlunparse(parsed_url._replace(query=new_query_string))
            response = requests.get(new_url, headers=self.headers)
            html = response.content.decode('shift_jis')
            selector = Selector(text=html)
            if 'plans' not in item:
                item['plans'] = []

            for plan in selector.xpath('//li[contains(@class, "p-planCassette")]'):
                plan_id = plan.xpath('./@data-plancode').get()

                meal_flag = plan.xpath('.//dd[contains(@class, "p-mealType__value")]/text()').get()
                non_smoking_flag = plan.xpath('.//li[contains(text(), "禁煙ルーム")]/text()').get()
                plan_name = plan.xpath('.//p[@class="p-searchResultItem__catchPhrase"]/text()').get()

                for room in plan.xpath('.//tr[contains(@class, "js-searchYadoRoomPlanCd")]'):
                    room_data = {}
                    room_data['plan_id'] = plan_id
                    
                    room_code = room.xpath('./@id').get()
                    if 'rc' in room_code:
                        room_data['room_code'] = room_code.split('rc')[-1]
                    
                    if plan_name:
                        room_data['plan_name'] = plan_name.strip()
        
                    room_name = room.xpath('.//a[contains(@class, "jsc-planDetailLink")]/text()').get()
                    if room_name:
                        room_data['room_name'] = room_name

                    if meal_flag:
                        room_data['meal_flag'] = meal_flag
                        
                    if non_smoking_flag:
                        room_data['non_smoking_flag'] = non_smoking_flag

                    room_data['adult_count'] = adult_count
                    room_data['meal_flag'] = meal_flag
                    room_data['non_smoking_flag'] = non_smoking_flag
                    item['plans'].append(room_data)
                    
                    room_url = room.xpath('.//a[contains(@class, "jsc-planDetailLink")]/@href').get()
                    room_data['url'] = urljoin(url, room_url)
                    if room_url:
                        self.parse_reservations(urljoin(url, room_url), room_data, item)

    def parse_reservations(self, url, room_data, item, year=None, month=None, month_count=0):
        response = requests.get(url, headers=self.headers)
        selector = Selector(text=response.text)

        plan_id = room_data.get('plan_id', None)
        room_code = room_data.get('room_code', None)
        adult_count = room_data.get('adult_count', None)

        price_data = []
        for room in selector.xpath('//a[contains(@href, "JavaScript:onLogin")]'):
            price_item = {}
            pattern = r"onLogin\('(\d{4})','(\d+)','(\d+)',"
            match = re.search(pattern, room.xpath('./@href').get())
            if match:
                price_item['date'] = f'{match.group(1)}-{match.group(2)}-{match.group(3)}'
            price = room.xpath('.//p[contains(@class, "jlnpc-price")]/text()').get()
            if price:
                price_item['price'] = int(price.replace(',', ''))
                price_data.append(price_item)

        if 'plans' in item:
            for plan in item['plans']:
                if plan['plan_id'] == plan_id and plan['room_code'] == room_code and plan['adult_count'] == adult_count:
                    if 'prices' not in plan:
                        plan['prices'] = []
                        plan['prices'] = price_data
                    else:
                        plan['prices'].extend(price_data)
        
        if month_count < 2:
            if month_count == 0:
                current_date = datetime.now()
                next_year = current_date.year
                next_month = current_date.month
            else:
                next_year = year
                next_month = month
            next_year, next_month, next_url = self.get_next_next_year_month(url, int(next_year), int(next_month))
            self.parse_reservations(next_url, room_data, item, next_year, next_month, month_count + 1)
        
    def get_next_next_year_month(self, url, year, month):
        new_month = month + 2
        new_year = year

        if new_month > 12:
            new_month -= 12
            new_year += 1

        base_url, *params = url.split('?')
        query_string = "&".join(params)
        params_dict = dict(param.split('=') for param in query_string.split('&') if '=' in param)
        params_dict['calYear'] = str(new_year)
        params_dict['calMonth'] = str(new_month)
        updated_query_string = '&'.join(f"{key}={value}" for key, value in params_dict.items())
        return new_year, new_month, f"{base_url}?{updated_query_string}"

    def extract_lrg_codes(self, lrg_data):
        lrg_entries = []
        lrg_lines = lrg_data.split('new LrgData(')[1:]
        for line in lrg_lines:
            lrg_info = re.search(r',\s*"(\d+)"\s*\)', line)
            if lrg_info:
                lrg_code = lrg_info.group(1)
                lrg_entries.append(lrg_code)

        return lrg_entries
import scrapy, re , json
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs

ITEMS_PER_COUNT = 30

class CrawlerSpider(scrapy.Spider):
    name = 'hotel'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.5',
        'content-type': 'text/html; charset=EUC-JP'
    }
    base_url = 'https://www.jalan.net/'
    start_url = 'https://www.jalan.net/js/quick/jalan_qs.js'
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
    hotel_list = []
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
        idx = response.meta.get('idx', 0)
        count = response.xpath('//span[@class="jlnpc-listInformation--count"]/text()').get()
        if count:
            count = int(count)
        hotel_urls = response.xpath('//div[@id="jsiInnList"]//li//a[@class="jlnpc-yadoCassette__link"]/@href').getall()
        if hotel_urls:
            for href in hotel_urls:
                hotel_url = response.urljoin(href.split('?')[0])
                if hotel_url not in self.hotel_list:
                    self.hotel_list.append(hotel_url)
                yield scrapy.Request(response.urljoin(href), callback=self.parse_hotel)

            idx += ITEMS_PER_COUNT
            if not response.xpath('//nav[@class="pagerLink"]'):
                return
            
            next_button = response.xpath('//span[@class="next"]')
            if next_button:
                return

            parsed_url = urlparse(response.url)
            query_params = parse_qs(parsed_url.query)
            query_params['idx'] = [f'{idx}']
            new_query_string = urlencode(query_params, doseq=True)
            new_url = urlunparse(parsed_url._replace(query=new_query_string))
            
            yield scrapy.Request(new_url, callback=self.parse_category, meta={'idx': idx})
    
    def parse_hotel(self, response):
        item={}

        hotel_id = response.url.split('/')[-2]
        if hotel_id:
            item['hotel_id'] = hotel_id.replace('yad', '')

        hotel_name = response.xpath('//div[@id="yado_header_hotel_name"]/a/text()').get()
        if hotel_name:
            item['hotel_name'] = hotel_name.strip()

        hotel_url = response.url.split('?')[0]
        if hotel_url:
            item['hotel_url'] = hotel_url
        
        address = response.xpath('//table[@class="jlnpc-shisetsu-accessparking-table"]/tr[1]/td/text()').get()
        if address:
            item['address'] = address.strip()
        
        detail_json = response.xpath('//script[@type="application/ld+json" and contains(text(), "postalCode")]/text()').get()
        if detail_json:
            detail_data = json.loads(detail_json)
            if 'address' in detail_data.keys():
                address_data = detail_data['address']
                if 'postalCode' in address_data.keys():
                    item['zip_code'] = address_data['postalCode']
        
        googlemap_url = response.xpath('//a[@class="jlnpc-mapImageBox__link"]/img/@src').get()
        if googlemap_url:
            parsed_url = urlparse(googlemap_url)
            query_params = parse_qs(parsed_url.query)
            center = query_params.get('center', [None])[0]
            if center:
                latitude, longitude = map(float, center.split(','))
                location = []
                location.append(longitude)
                location.append(latitude)
                item['location'] = location

        room_count = response.xpath('//table[@class="shisetsu-roomsetsubi_body" and contains(., "総部屋数")]//table[1]//tr[2]/td[5]/text()').get()
        if room_count:
            if '室' in room_count:
                item['room_count'] = int(room_count.replace('室', '').strip())
        
        amenities = response.xpath('//table[contains(@class, "shisetsu-amenityspec_body")]//td[contains(., "○")]/following-sibling::td[1]//text()').getall()
        if amenities:
            item['amenities'] = amenities
        
        rating = {}
        rating_average = response.xpath('//div[@class="jlnpc-yadoDetail__kuchikomi"]//span[@class="jlnpc-average-num"]//text()').get()
        if rating_average:
            rating['average'] = float(rating_average)
        review_count = response.xpath('//span[@class="jlnpc-voice-num"]//text()').get()
        if review_count:
            rating['review_count'] = int(review_count.replace(',', '')) 
        room = response.xpath('//table[contains(@class, "shisetsu-kuchikomi_spec_body")]//th[contains(., "部屋")]/following-sibling::td[1]//text()').get()
        if room and room.strip() != '-':
            rating['room'] = float(room)
        else:
            rating['room'] = 0
        bath = response.xpath('//table[contains(@class, "shisetsu-kuchikomi_spec_body")]//th[contains(., "風呂")]/following-sibling::td[1]//text()').get()
        if bath and bath.strip() != '-':
            rating['bath'] = float(bath)
        else:
            rating['bath'] = 0
        breakfast = response.xpath('//table[contains(@class, "shisetsu-kuchikomi_spec_body")]//th[contains(., "朝食")]/following-sibling::td[1]//text()').get()
        if breakfast and breakfast.strip() != '-':
            rating['breakfast'] = float(breakfast)
        else:
            rating['breakfast'] = 0
        dinner = response.xpath('//table[contains(@class, "shisetsu-kuchikomi_spec_body")]//th[contains(., "夕食")]/following-sibling::td[1]//text()').get()
        if dinner and dinner.strip() != '-':
            rating['dinner'] = float(dinner)
        else:
            rating['dinner'] = 0
        customer_service = response.xpath('//table[contains(@class, "shisetsu-kuchikomi_spec_body")]//th[contains(., "接客・サービス")]/following-sibling::td[1]//text()').get()
        if customer_service and customer_service.strip() != '-':
            rating['customer_service'] = float(customer_service)
        else:
            rating['customer_service'] = 0
        cleanliness = response.xpath('//table[contains(@class, "shisetsu-kuchikomi_spec_body")]//th[contains(., "清潔感")]/following-sibling::td[1]//text()').get()
        if cleanliness and cleanliness.strip() != '-':
            rating['cleanliness'] = float(cleanliness)
        else:
            rating['cleanliness'] = 0
        item['rating'] = rating
        yield item

    def extract_lrg_codes(self, lrg_data):
        lrg_entries = []
        lrg_lines = lrg_data.split('new LrgData(')[1:]
        for line in lrg_lines:
            lrg_info = re.search(r',\s*"(\d+)"\s*\)', line)
            if lrg_info:
                lrg_code = lrg_info.group(1)
                lrg_entries.append(lrg_code)

        return lrg_entries
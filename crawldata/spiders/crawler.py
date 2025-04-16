import scrapy,os,platform,re,json
from crawldata.functions import *
from datetime import datetime

class CrawlerSpider(scrapy.Spider):
    name = 'permin-jardin'
    DATE_CRAWL=datetime.now().strftime('%Y-%m-%d')
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8','Accept-Language': 'en-GB,en;q=0.5','Connection': 'keep-alive','Upgrade-Insecure-Requests': '1','Sec-Fetch-Dest': 'document','Sec-Fetch-Mode': 'navigate','Sec-Fetch-Site': 'none','Sec-Fetch-User': '?1','Priority': 'u=0, i'}
    headers_post = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0','Accept': '*/*','Accept-Language': 'en-GB,en;q=0.5','Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8','X-Requested-With': 'XMLHttpRequest','Origin': 'https://www.techniekwebshop.nl','Connection': 'keep-alive','Sec-Fetch-Dest': 'empty','Sec-Fetch-Mode': 'cors','Sec-Fetch-Site': 'same-origin','Priority': 'u=0'}
    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'
    domain='https://www.mecaservicesshop.fr/7057-kramp'
    page_urls=[]

    def start_requests(self):
        yield scrapy.Request(self.domain,callback=self.parse_list,dont_filter=True)

    def parse_categories(self,response):

        page_count = response.xpath('(//ul[contains(@class, "page-list")]//a[@class="js-search-link"])[last()]/text()').extract_first()
        if page_count:
            page_count = int(page_count)

        for i in range(page_count):
            link = response.url
            if i > 0:
                link = link + f"?page={i+1}"
            yield scrapy.Request(link, callback=self.parse_list, dont_filter=True)

    def parse_list(self, response):
        # print(response.url)
        links = response.xpath('//div[@class="product-image-container"]/a[contains(@class, "product-thumbnail")]/@href').getall()
        # link = 'https://www.mecaservicesshop.fr/reservoir-et-filtres/1036437-filter-20-mesh-screen-banjo.html'
        # yield scrapy.Request(link, callback=self.parse_data, dont_filter=True)
        for link in links:
            yield scrapy.Request(response.urljoin(link), callback=self.parse_data, dont_filter=True)

    def parse_data(self, response):
        item={}

        item['additional_images'] = []
        item['base_image'] = ''
        item['brand'] = ''
        item['breadcrumb'] = ''
        item['description'] = ''
        item['name'] = ''
        item['part_number'] = ''
        item['price'] = 0.00
        item['price_currency'] = ''
        item['qty'] = 0
        item['sku'] = ''
        item['thumbnail_image'] = ''
        item['discount_price'] = 0.00
        item['reviews'] = []
        item['review_number'] = 0
        item['tech_spec'] = {}

        base_image = response.xpath('//div[@class="product-cover"]/img/@src').extract_first()
        if base_image:
            item['base_image'] = response.urljoin(base_image)
            item['thumbnail_image'] = response.urljoin(base_image)
            item['small_image'] = response.urljoin(base_image)

        additional_image_links = response.xpath('//li[@class="thumb-container"]/img/@data-image-large-src').getall()
        for additional_image in additional_image_links:
            item['additional_images'].append(response.urljoin(additional_image))
        
        if item['base_image'] in item['additional_images']:
            item['additional_images'].remove(item['base_image'])
        
        brand = response.xpath('//div[@itemprop="brand"]/a//text()').extract_first()
        if not brand:
            brand = 'unbranded'

        item['brand'] = brand

        breadcrumb_all = response.xpath('//nav[contains(@class, "breadcrumb")]//li[position() > 1]/a/span/text()').getall()
        if breadcrumb_all:
            breadcrumb = "/".join(breadcrumb_all)
            item['breadcrumb'] = breadcrumb

        descriptions = response.xpath('//div[contains(@class, "desc") or contains(@class,"description")]//table').extract_first()
        if descriptions:
            item['description'] = descriptions
   
        name = response.xpath('//h1[@class="product_name"]/text()').extract_first().strip()
        if name:
            item['name'] = name

        item['original_page_url'] = response.url
        
        part_number = response.xpath('//div[contains(@class,"product-reference_top")][1]/span/text()').extract_first()
        if part_number:
            if '-' in part_number:
                part_number = part_number.split('-')[-1]
            item['part_number'] = part_number
        
        price = response.xpath('//meta[@property="product:price:amount"]/@content').extract_first()
        if price:
            item['price'] = float(price)

        price_currency = response.xpath('//meta[@property="product:price:currency"]/@content').extract_first()
        if price_currency:
            item['price_currency'] = price_currency
        
        qty = response.xpath('//input[@name="qty"]/@value').extract_first()
        if qty:
            item['qty'] = int(qty)

        if part_number and brand:
            part_number = re.sub(r'[^A-Za-z0-9]', '', part_number)
            sku = f"{item['brand']}-{part_number}"
            sku = sku.lower().replace(' - ', '-')
            item['sku'] = sku.replace(' ', '-')
        
        discount_price = response.xpath('//div[@id="product-details"]/@data-product').extract_first()
        if discount_price:
            product_data = json.loads(discount_price)
            if product_data['discount_amount']:
                discount_price = re.sub(r'[^\d,.-]', '', product_data['discount_amount'])
                discount_price = discount_price.replace(',', '.')
                item['discount_price'] = float(discount_price)
        
        tech_spec_keys = response.xpath('//section[@class="product-features"]/dl/dt//text()').getall()
        tech_spec_values = response.xpath('//section[@class="product-features"]/dl/dd//text()').getall()

        if tech_spec_keys:
            count = len(tech_spec_keys)
            for i in range(count):
                tech_spec_key = tech_spec_keys[i]
                tech_spec_value = tech_spec_values[i]
                item['tech_spec'][tech_spec_key] = tech_spec_value
        
        
        # review_number = response.xpath('//span[@itemprop="reviewCount"]//text()').extract_first()
        # if review_number:
        #     item['review_number'] = review_number

        # reviews_data = response.xpath('//li[@class="reviews__list-item"]')
        # for review_data in reviews_data:
        #     it = {}
        #     author = review_data.xpath('.//span[@class="reviews__review-author"]//text()').extract_first()
        #     score = review_data.xpath('.//span[@class="rating__color-stars"]/@data-rating').extract_first()
        #     text = review_data.xpath('.//span[@class="reviews__review-txt"]//text()').extract_first()
        #     date = review_data.xpath('.//span[@class="reviews__review-date"]//text()').extract_first()
            
        #     it['author'] = author
        #     it['score'] = score
        #     it['text'] = text
        #     it['date'] = date

        #     item['reviews'].append(it)
        

        yield item

    def get_db_data(self):
        with open('rolmax_dump.csv', 'r') as f:
            self.page_urls = {url.strip().strip('"') for url in f}

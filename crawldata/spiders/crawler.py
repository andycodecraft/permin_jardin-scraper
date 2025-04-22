import scrapy,os,platform,re,json,math
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
    domain='https://www.ferme-et-jardin.com/pieces-detachees'
    page_urls=[]

    def start_requests(self):
        # self.write_db_data('https://www.ferme-et-jardin.com/article/barri-res-10-pi-ces-1318411')
        # return
        # url = 'https://www.ferme-et-jardin.com/article/barri-res-10-pi-ces-1318411'
        # yield scrapy.Request(url,callback=self.parse_data,dont_filter=True)
        yield scrapy.Request(self.domain,callback=self.parse_categories,dont_filter=True)

    def parse_categories(self,response):
        links = response.xpath(
            '//a[@class="categoryLink"]/@href |'
            '//div[@class="dropdownList"]//a[@class="subCategoryLink"]/@href'
        ).getall()
        for link in links:
            yield scrapy.Request(
                response.urljoin(link),
                callback=self.parse_pages,
                dont_filter=True
            )

    def parse_pages(self, response):
        page_count = response.xpath('//span[@class="js-article-count"]/text()').extract_first()
        if page_count:
            page_count = int(page_count.strip())
            page_count = math.ceil(page_count / 20)

        for i in range(page_count):
            link = response.url
            if i > 0:
                link = link + f"?page={i+1}"
            yield scrapy.Request(response.urljoin(link), callback=self.parse_list, dont_filter=True)
        
    def parse_list(self, response):
        links = response.xpath('//a[@class="tileLink"]/@href').getall()
        for link in links:
            yield scrapy.Request(response.urljoin(link), callback=self.parse_data, dont_filter=True)
        # links = response.xpath('//div[@class="product-image-container"]/a[contains(@class, "product-thumbnail")]/@href').getall()
        # for link in links:
        #     yield scrapy.Request(response.urljoin(link), callback=self.parse_data, dont_filter=True)

    def fix_string(self, broken):
        replacement_map = {
            '�': 'é'
        }

        for broken_char, correct_char in replacement_map.items():
            broken = broken.replace(broken_char, correct_char)
        return broken

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
        item['weight'] = ''

        base_image = response.xpath('//div[contains(@class, "mainPhotoImg")]//img/@src').extract_first()
        if base_image:
            item['base_image'] = response.urljoin(base_image)
            item['thumbnail_image'] = response.urljoin(base_image)
            item['small_image'] = response.urljoin(base_image)
        
        additional_image_links = response.xpath('//li[@class="thumb-container"]/img/@data-image-large-src').getall()
        for additional_image in additional_image_links:
            item['additional_images'].append(response.urljoin(additional_image))
        
        if item['base_image'] in item['additional_images']:
            item['additional_images'].remove(item['base_image'])
        
        brand = response.xpath('//img[@class="actualBrand"]/@alt').extract_first()
        if not brand:
            brand = 'unbranded'

        item['brand'] = brand

        breadcrumb_all = response.xpath('//span[@class="breadcrumbItemTitle"]/text()').getall()
        if breadcrumb_all:
            breadcrumb = "/".join(item.strip() for item in breadcrumb_all)
            item['breadcrumb'] = self.fix_string(breadcrumb)

        description = response.xpath('//div[@itemprop="description"]/text()').extract_first()
        if description:
            item['description'] = description
        
        weights = response.xpath('//div[@class="caracValue"]//text()').getall()
        if weights:
            weight = ''.join(item.strip() for item in weights)
            item['weight'] = weight
   
        name = response.xpath('//h1[@class="articleTitle"]/text()').extract_first().strip()
        if name:
            item['name'] = self.fix_string(name)

        item['original_page_url'] = response.url
        
        part_number = response.xpath('//span[@itemprop="serialNumber"]/text()').extract_first()
        if part_number:
            item['part_number'] = part_number
        
        id = response.xpath('//div[contains(@class, "pricesBlock")]/@data-id').extract_first()
        if id:
            id = int(id)

        post_data = {
            "articles": [
                {
                    "id": id
                }
            ]
        }

        price_data = requests.post(
            'https://www.ferme-et-jardin.com/pricer',
            json=post_data)
        
        if price_data:
            price_data = json.loads(price_data.text)
            discount = price_data['prices'][0]['discount']
            if discount:
                item['discount_price'] = price_data['prices'][0]['priceWithVat']
            item['price'] = price_data['prices'][0]['totalStripedPriceWithVat']
            item['price_currency'] = 'EUR'
        
        qty = response.xpath('//div[contains(@class, "cartItem")]/@data-quantity').extract_first()
        if qty:
            item['qty'] = int(qty)
        
        availabilities = response.xpath('//span[@class="stockLabel" or contains(@class, "stockQuantity")]/text()').getall()
        if availabilities:
            availability = ''.join(item.strip() for item in availabilities)
            item['availability'] = availability

        if part_number and brand:
            part_number = re.sub(r'[^A-Za-z0-9]', '', part_number)
            sku = f"{item['brand']}-{part_number}"
            sku = sku.lower().replace(' - ', '-')
            item['sku'] = sku.replace(' ', '-')
        
        yield item

    def get_db_data(self):
        with open('permin_dump.csv', 'r') as f:
            self.page_urls = {url.strip().strip('"') for url in f}

    def write_db_data(self, url):
        filename='permin_dump.csv'
        with open(filename, 'a') as f:
            f.write(f'"{url}"\n')
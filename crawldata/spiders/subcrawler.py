import scrapy,json,os,platform
from crawldata.functions import *
from datetime import datetime

class CrawlerSpider(scrapy.Spider):
    name = 'subtechniekwebshop'
    DATE_CRAWL=datetime.now().strftime('%Y-%m-%d')
    #custom_settings={'LOG_FILE':'./log/'+name+'_'+DATE_CRAWL+'.log'}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8','Accept-Language': 'en-GB,en;q=0.5','Connection': 'keep-alive','Upgrade-Insecure-Requests': '1','Sec-Fetch-Dest': 'document','Sec-Fetch-Mode': 'navigate','Sec-Fetch-Site': 'none','Sec-Fetch-User': '?1','Priority': 'u=0, i'}
    headers_post = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0','Accept': '*/*','Accept-Language': 'en-GB,en;q=0.5','Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8','X-Requested-With': 'XMLHttpRequest','Origin': 'https://www.techniekwebshop.nl','Connection': 'keep-alive','Sec-Fetch-Dest': 'empty','Sec-Fetch-Mode': 'cors','Sec-Fetch-Site': 'same-origin','Priority': 'u=0'}
    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'
    domain='https://www.techniekwebshop.nl/filters-passend-voor-ag-chem-big-a-2700.html'
    proxy='http://brd-customer-hl_608de7a9-zone-eu_proxy:357jfq6mspo7@brd.superproxy.io:22225'
    def start_requests(self):
        print('start requests:')
        yield scrapy.Request(self.domain,callback=self.parse_data,dont_filter=True,meta={'id': '1', 'proxy':self.proxy})
    def parse_data(self,response):
        product_urls=response.xpath('//table[@id="super-product-table"]//tr/td[1]/a/@href').extract()
        if product_urls:
            for product_url in product_urls:
                yield scrapy.Request(response.urljoin(product_url), callback=self.parse_data,dont_filter=True,meta={'id': '1', 'proxy':self.proxy} )
        
        id=response.meta['id']
        cates=response.xpath('//div[@class="breadcrumbs"]//li[@class="crumb"]/a/span[@itemprop="title"]/text()').getall()
        if len(cates)>1:
            del cates[-1]
        title=response.xpath('//h1/text()').get()
        imgs=response.xpath('//ul[@class="product-image-thumbs"]/li//img/@src').getall()
        skus=response.xpath('//span[@class="title-sku"]/text()').get()
        skus_list=str(skus).split("|")
        options={}
        for ls in skus_list:
            if ':' in ls:
                lss=str(ls).split(':')
                options[str(lss[0]).strip().lower()]=str(lss[1]).strip()
        item={}
        if len(imgs)>0:
            item['base_image']=imgs[0]
            item['small_image']=str(item['base_image'])
            item['thumbnail_image']=item['base_image']
            if len(imgs)>1:
                del imgs[0]
                item['additional_images']=imgs
        try:
            item['brand']=str(response.xpath('//table[@id="product-attribute-specs-table-filters"]//a[contains(@href,"manufacturer")]/@href').get()).split("manufacturer=")[1]
            item['brand']=str(item['brand']).replace("%20"," ")
        except:
            pass
        item['breadcrumb']=('/'.join(cates))
        item['dimensions']={}
        it={}
        Data=response.xpath('//table[@id="product-attribute-specs-table-filters"]//tr')
        for row in Data:
            TITLE=row.xpath('./th[@class="label"]//text()').get()
            VAL=row.xpath('./td[@class="data"]//text()').get()
            if TITLE and VAL:
                TITLE=str(TITLE).lower().strip().replace(" ","-")
                VAL=str(VAL).strip().replace('"',' inch')
                if not TITLE in it:
                    it[TITLE]=[]
                it[TITLE].append(VAL)
        item['tech_spec']=it
        for k in it.keys():
            if 'categorie' in k or 'category':
                item['categories']=it[k]
            if 'diameter' in k:
                item['dimensions'][k]=it[k]
            if 'weight' in k:
                nbr=Get_Number(k)
                if str(nbr).isdigit() and not "." in nbr:
                    nbr=int(nbr)
                    val=Get_Number(it[k])
                    if str(val).isdigit():
                        val=float(val)
                        item['weight']=val/nbr
                elif str(it[k]).isdigit():
                    item['weight']=float(it[k])
        if 'ean' in options:
            item['ean']=options['ean']
        item['name']=title
        item['original_page_url']=response.url
        if 'artikelnummer' in options:
            item['part_number']=options['artikelnummer']
        if not 'part_number' in item and 'overzichtnummer' in options:
            item['part_number']=options['overzichtnummer']
        if not 'part_number' in item and 'snelcode' in options:
            item['part_number']=options['snelcode']
        try:
            item['sku']=item['brand']+'-'+item['part_number']
        except:
            try:
                item['sku']=item['part_number']
            except:
                pass
        price=response.xpath('//div[@class="product-shop"]//span[@class="old-price"]/span[@class="price"]/text()').get()
        if price:
            prices=str(price).strip().split()
            item['list_price']=Get_Number(prices[1])
            item['price_currency']=prices[0]
            if item['price_currency']=='€':
                item['price_currency']='EUR'
        item['qty']=response.xpath('//input[@data-qty]/@value').get()
        item['original_id']=key_MD5(item['breadcrumb'])+'_'+id
        warehouse_number=response.xpath('//td[contains(text(),"Voorraad extern magazijn")]/strong/text()').get()
        if warehouse_number:
            item['warehouse_number']=str(warehouse_number).strip().split()[0]
        free_shipping_limit=response.xpath('//div[contains(@class,"availability-new")]//span[contains(text(),"verzendkosten")]/../text()').get()
        if len(Get_Number(free_shipping_limit))>0:
            item['free_shipping_limit']=Get_Number(free_shipping_limit)
        price=response.xpath('//span[@id="product-price-'+id+'"]/span/text()').get()
        if price:
            item['price_in_vat']=Get_Number(price)
        price=response.xpath('//span[@id="price-excluding-tax-'+id+'"]/text()').get()
        if price:
            item['price_ex_vat']=Get_Number(price)
        if not 'price_currency' in item:
            if 'price_in_vat' in item:
                item['price_currency']=response.xpath('//span[@id="product-price-'+id+'"]/span/text()').get()[0]
                if item['price_currency']=='€':
                    item['price_currency']='EUR'
            if not 'price_currency' in item and 'price_ex_vat' in item:
                item['price_currency']=response.xpath('//span[@id="price-excluding-tax-'+id+'"]/text()').get()[0]
                if item['price_currency']=='€':
                    item['price_currency']='EUR'
        DESC=response.xpath('//div[@class="short-description"]/div//text()').getall()
        DES=[]
        for i in range(len(DES)):
            if str(DES[i]).strip()!='':
                DES.append(str(DES[i]).strip())
        if len(DES)>0:
            item['description']="\n".join(DES)
        trades=response.xpath('//tr[contains(@class,"trade")]/td/text()').getall()
        trade=[]
        for i in range(len(trades)):
            txt=str(trades[i]).strip()
            if txt!='':
                trade.append(txt)
        if len(trade)>0:
            item['equivalent_part_numbers']="; ".join(trade)
        Reviews=[]
        Data=response.xpath('//div[@id="customer-reviews"]/dl')
        if Data:
            TITLE=Data.xpath('./dt')
            VALS=Data.xpath('./dd')
            for i in range(len(TITLE)):
                title=TITLE[i].xpath('./span[@class="review-title-posted"]/text()').get()
                if title:
                    it={}
                    it['review_date']=str(title).strip().split()[-1]
                    it['review_content']=VALS[i].xpath('./text()').get()
                    it['rating']=0
                    rates=VALS[i].xpath('.//div[@class="ratings"]/i/@class').getall()
                    for rs in rates:
                        if str(rs).endswith('fa-star'):
                            it['rating']+=1
                        elif str(rs).endswith('fa-star-half-o'):
                            it['rating']+=0.5
                    Reviews.append(it)
        if len(Reviews)>0:
            item['ratings']=Reviews
        #yield(item)
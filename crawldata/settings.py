# Scrapy settings for crawldata project

BOT_NAME = 'crawldata'
SPIDER_MODULES = ['crawldata.spiders']
NEWSPIDER_MODULE = 'crawldata.spiders'
# DOWNLOADER_MIDDLEWARES = {
#     # 'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
#     'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
#     'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
# }

LOG_ENABLED = True
# LOG_FILE = 'scrapy_log.txt'
# ROTATING_PROXY_LIST_PATH = 'proxy.txt'
#REQUEST_FINGERPRINTER_IMPLEMENTATION="2.7"
URLLENGTH_LIMIT = 50000
#ROBOTSTXT_OBEY = False
HTTPERROR_ALLOW_ALL=True
CONCURRENT_REQUESTS=50
CONCURRENT_REQUESTS_PER_DOMAIN=99999
DOWNLOAD_DELAY = 1
DEFAULT_REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept': '*/*',
    'Accept-Language': 'en-GB,en;q=0.5',
    'content-type': 'text/plain',
    'Connection': 'keep-alive',
    'TE': 'trailers',
}
ITEM_PIPELINES = {
    'crawldata.pipelines.CrawldataPipeline': 300,
}
# settings.py




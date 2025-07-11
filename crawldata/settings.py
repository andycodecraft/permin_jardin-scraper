BOT_NAME = 'crawldata'
SPIDER_MODULES = ['crawldata.spiders']
NEWSPIDER_MODULE = 'crawldata.spiders'

LOG_ENABLED = True
URLLENGTH_LIMIT = 50000
HTTPERROR_ALLOW_ALL=True
CONCURRENT_REQUESTS=50
CONCURRENT_REQUESTS_PER_DOMAIN=99999
DOWNLOAD_DELAY = 0.5
# FEED_EXPORT_ENCODING = 'utf-8'
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




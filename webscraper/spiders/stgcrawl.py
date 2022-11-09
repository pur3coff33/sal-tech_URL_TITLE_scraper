import re
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from w3lib.url import url_query_cleaner

def process_links(links):
    for link in links:
        link.url = url_query_cleaner(link.url)
        yield link

class StgcrawlSpider(CrawlSpider):
    name = 'stgcrawl'
    allowed_domains = ['fallingrain.com']
    start_urls = ['http://www.fallingrain.com/world/DA/']
    base_url = 'fallingrain.com'

    custom_settings = {
        # in order to reduce the risk of getting blocked
        'DOWNLOADER_MIDDLEWARES': {'webscraper.middlewares.WebscraperSpiderMiddleware': 400, },
        'COOKIES_ENABLED': False,
        'CONCURRENT_REQUESTS': 10,
        'DOWNLOAD_DELAY': 0.5,
        'ROBOTSTXT_OBEY': False,

        # This settings are a must:
        
        # Duplicates pipeline
        'ITEM_PIPELINES': {'webscraper.pipelines.WebscraperPipeline': 300},

        # In order to create a CSV file:
        'FEEDS': {'csv_file.csv': {'format': 'csv'}}
    }

    rules = (
        Rule(
            LinkExtractor(allow_domains=allowed_domains),
            process_links=process_links,
            callback='parse_item',
            follow=True
        ),
    )

    def parse_item(self, response):

        self.base_url = response.url

        if 'http://www.fallingrain.com/world/DA/' not in response.url and 'html' in response.url:
            return

        table = response.css('table')
        result = {
            'url': response.url,
        }
    
        for tr in table.css('tr'):
            row_header = tr.css('th::text').get()
            row_value = tr.css('td::text').get()
            result[row_header] = row_value
          

        yield result
        

    def test(self, response):

        """""
        title = response.css('title::text').get()

        description = ""
        keywords = ""

        if response.xpath("//meta[@name='description']/@content") and response.xpath("//meta[@name='keywords']/@content"):
            description = response.xpath("//meta[@name='description']/@content")[0].extract()
            keywords = response.xpath("//meta[@name='keywords']/@content")[0].extract()

        yield {
            'url': response.url,
            'title': title,
            'description' : description,
            'keywords' : keywords
        }

        """""

        # see if you really need this loop (since you're parsing all the urls in the domain anyway, and you'll need
        # to filter all those duplicates):

        all_urls = response.css('a::attr(href)').getall()

        # In order to change from relative to absolute url in the pipeline:
        self.base_url = response.url

        title = ""
        for url in all_urls:

            """""
            #prevent duplicates
            prev_title = title
            title = response.css('title::text').get()

            if prev_title == title:
                continue
            # end 
            
            """
            description = ""
            keywords = ""

            if response.xpath("//meta[@name='description']/@content") and response.xpath("//meta[@name='keywords']/@content"):
                description = response.xpath("//meta[@name='description']/@content")[0].extract()
                keywords = response.xpath("//meta[@name='keywords']/@content")[0].extract()
                
            title = response.css('title::text').get()

            yield {
                'url': url,
                'title': title,
                'description' : description,
                'keywords' : keywords
            }

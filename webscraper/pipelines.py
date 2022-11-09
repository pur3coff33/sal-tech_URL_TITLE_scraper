# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class WebscraperPipeline:
    def __init__(self):
        self.scraped_urls = set()

    def process_item(self, item, spider):
        url = item['url'].strip()

        # if it's a relative url then convert to absolute url
        if 'http' not in url:
            url = spider.base_url + url
            item['url'] = url

        if url in self.scraped_urls:
            raise DropItem(f'Duplicate url: \"{url}\"')
        else:
            self.scraped_urls.add(url)
            return item
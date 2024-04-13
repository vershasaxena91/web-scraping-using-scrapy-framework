import scrapy
from amazon_product_scraping.items import AmazonProductScrapingItem


class AmazonProductDuplicateBSRSpider(scrapy.Spider):
    """
    A class for scrapy spider.

    Attributes
    ----------
    handle_httpstatus_all : Boolean
        True
    name : str
        spider name which identify the spider
    rotate_user_agent : Bollean
        True
    allowed_domains : list
        contains base-URLs for the allowed domains for the spider to crawl
    start_urls : list
        a list of URLs for the spider to start crawling from
    """

    handle_httpstatus_all = True
    name = "AmazonProductDuplicateBSRSpider"
    rotate_user_agent = True
    # allowed_domains = ["amazon.in"]
    start_urls = [
        "https://www.amazon.in/gp/bestsellers/beauty/1374334031/ref=zg_bs_nav_beauty_3_9851597031",
        "https://www.amazon.in/gp/bestsellers/beauty/1374334031/ref=zg_bs_pg_2?ie=UTF8&pg=2",
    ]

    def start_requests(self):
        """
        This class method must return an iterable with the first Requests to crawl for this spider.

        Set our proxy port http://scraperapi:API_KEY@proxy-server.scraperapi.com:8001 as the proxy in the meta parameter.
        """

        urls = self.start_urls

        for url in urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                # meta={
                #     "proxy": "http://scraperapi:1ee5ce80f3bbdbad4407afda1384b61e@proxy-server.scraperapi.com:8001"
                # },
            )

    def parse(self, response):
        """
        A class method used to parse the response for each request, extract scraped data as dicts.

        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        dicts
            extract the scraped data as dicts
        """

        print(response.url, response.status)
        items = AmazonProductScrapingItem()
        links_xpath = (
            response.xpath('//div[@id="zg-center-div"]')
            .xpath('//a[@class="a-link-normal"]/@href')
            .extract()
        )
        links = [i for i in links_xpath if "/dp/" in i]
        asin = [(i.split("/dp/")[1]).split("/")[0] for i in links]
        bsr_xpath = response.xpath(
            '//div[@id="zg-center-div"]//span//span//text()'
        ).extract()
        bsr = [i + " in Shampoos (Beauty)" for i in bsr_xpath if "#" in i]

        # items["links"] = links
        items["asin"] = asin
        items["bsr"] = bsr
        yield items

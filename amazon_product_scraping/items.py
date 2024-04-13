# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AmazonProductScrapingItem(scrapy.Item):
    """
    A class used to define the fields for the item.
    """

    product_name = scrapy.Field()
    product_brand = scrapy.Field()
    product_sale_price = scrapy.Field()
    product_offers = scrapy.Field()
    product_original_price = scrapy.Field()
    product_fullfilled = scrapy.Field()
    product_rating = scrapy.Field()
    product_total_reviews = scrapy.Field()
    product_availability = scrapy.Field()
    product_category = scrapy.Field()
    product_icons = scrapy.Field()
    product_best_seller_rank = scrapy.Field()
    product_details = scrapy.Field()
    product_asin = scrapy.Field()
    product_important_information = scrapy.Field()
    product_description = scrapy.Field()
    product_bought_together = scrapy.Field()
    product_subscription_discount = scrapy.Field()
    product_variations = scrapy.Field()
    links = scrapy.Field()
    asin = scrapy.Field()
    bsr = scrapy.Field()


class AmazonProductInfoItem(scrapy.Item):
    product_name = scrapy.Field()
    product_brand = scrapy.Field()
    # product_sale_price = scrapy.Field()
    product_offers = scrapy.Field()
    # product_original_price = scrapy.Field()
    # product_fullfilled = scrapy.Field()
    # product_rating = scrapy.Field()
    # product_total_reviews = scrapy.Field()
    # product_availability = scrapy.Field()
    product_category = scrapy.Field()
    product_icons = scrapy.Field()
    # product_best_seller_rank = scrapy.Field()
    product_details = scrapy.Field()
    product_bullets = scrapy.Field()
    product_asin = scrapy.Field()
    product_important_information = scrapy.Field()
    product_description = scrapy.Field()
    product_bought_together = scrapy.Field()
    # product_subscription_discount = scrapy.Field()
    product_variations = scrapy.Field()
    product_image_urls = scrapy.Field()


class AmazonProductDailyMovementItem(scrapy.Item):
    product_sale_price = scrapy.Field()
    product_original_price = scrapy.Field()
    product_fullfilled = scrapy.Field()
    product_rating = scrapy.Field()
    product_total_reviews = scrapy.Field()
    product_availability = scrapy.Field()
    product_asin = scrapy.Field()
    product_best_seller_rank = scrapy.Field()
    product_subscription_discount = scrapy.Field()
    product_total_questions = scrapy.Field()


class AmazonProductCommentsItem(scrapy.Item):
    product_asin = scrapy.Field()  # str of asin number
    product_comments = scrapy.Field()  # list(map(str, dynamic))


class AmazonSearchCount(scrapy.Item):
    count = scrapy.Field()


class AmazonSearchProductList(scrapy.Item):
    products = scrapy.Field()


class AmazonShareOfSearchItem(scrapy.Item):
    product_asin = scrapy.Field()
    product_name = scrapy.Field()
    product_brand = scrapy.Field()
    product_original_price = scrapy.Field()
    product_sale_price = scrapy.Field()
    product_fullfilled = scrapy.Field()
    product_offers = scrapy.Field()
    product_category = scrapy.Field()
    product_icons = scrapy.Field()
    product_details = scrapy.Field()
    product_important_information = scrapy.Field()
    product_description = scrapy.Field()
    product_bought_together = scrapy.Field()
    product_variations = scrapy.Field()
    product_rating = scrapy.Field()
    product_total_reviews = scrapy.Field()
    product_availability = scrapy.Field()
    product_best_seller_rank = scrapy.Field()
    product_subscription_discount = scrapy.Field()


class AmazonShareOfSearchRanksItem(scrapy.Item):
    product_ranks = scrapy.Field()


class AmazonQuestionsItem(scrapy.Item):
    product_asin = scrapy.Field()  # str of asin number
    product_QAs = scrapy.Field()  # list(map(str, dynamic))


class AmazonTotalQuestionsItem(scrapy.Item):
    product_asin = scrapy.Field()
    product_total_questions = scrapy.Field()


class FlipkartProductInfoItem(scrapy.Item):
    product_name = scrapy.Field()
    product_brand = scrapy.Field()
    product_original_price = scrapy.Field()
    product_category = scrapy.Field()
    product_highlights = scrapy.Field()
    product_services = scrapy.Field()
    product_details = scrapy.Field()
    product_pid = scrapy.Field()
    product_lid = scrapy.Field()
    marketplace = scrapy.Field()
    product_description = scrapy.Field()
    product_url = scrapy.Field()
    scraped_on = scrapy.Field()


class FlipkartProductDailyMovementItem(scrapy.Item):
    product_sale_price = scrapy.Field()
    product_original_price = scrapy.Field()
    product_rating = scrapy.Field()
    product_total_ratings = scrapy.Field()
    product_total_reviews = scrapy.Field()
    product_availability = scrapy.Field()
    product_assured = scrapy.Field()
    product_pid = scrapy.Field()
    product_lid = scrapy.Field()
    marketplace = scrapy.Field()
    product_url = scrapy.Field()


class FlipkartSearchProductList(scrapy.Item):
    products = scrapy.Field()


class FlipkartShareOfSearchItem(scrapy.Item):
    product_name = scrapy.Field()
    product_brand = scrapy.Field()
    product_original_price = scrapy.Field()
    product_category = scrapy.Field()
    product_highlights = scrapy.Field()
    product_services = scrapy.Field()
    product_details = scrapy.Field()
    product_pid = scrapy.Field()
    product_lid = scrapy.Field()
    marketplace = scrapy.Field()
    product_description = scrapy.Field()
    product_url = scrapy.Field()
    scraped_on = scrapy.Field()

    product_sale_price = scrapy.Field()
    product_rating = scrapy.Field()
    product_total_ratings = scrapy.Field()
    product_total_reviews = scrapy.Field()
    product_availability = scrapy.Field()
    product_assured = scrapy.Field()


class FlipkartRankItem(scrapy.Item):
    product_ranks = scrapy.Field()


class FlipkartProductCommentsItem(scrapy.Item):
    product_pid = scrapy.Field()  # str of asin number
    marketplace = scrapy.Field()
    scraped_on = scrapy.Field()
    product_comments = scrapy.Field()  # list(map(str, dynamic))


class FlipkartShareOfSearchRanksItem(scrapy.Item):
    product_ranks = scrapy.Field()


class ShopeeSearchProductList(scrapy.Item):
    products = scrapy.Field()


class ShopeeProductInfoItem(scrapy.Item):
    product_name = scrapy.Field()
    product_brand = scrapy.Field()
    product_original_price = scrapy.Field()
    product_categories = scrapy.Field()
    product_variations = scrapy.Field()
    product_images = scrapy.Field()
    product_videos = scrapy.Field()
    product_attributes = scrapy.Field()
    product_itemid = scrapy.Field()
    product_shopid = scrapy.Field()
    product_description = scrapy.Field()
    product_url = scrapy.Field()
    scraped_on = scrapy.Field()


class ShopeeProductDailyMovementItem(scrapy.Item):
    product_sale_price = scrapy.Field()
    product_rating = scrapy.Field()
    product_total_ratings = scrapy.Field()
    product_total_likes = scrapy.Field()
    product_stock = scrapy.Field()
    product_sold = scrapy.Field()
    product_historical_sold = scrapy.Field()
    product_itemid = scrapy.Field()
    product_shopid = scrapy.Field()
    product_original_price = scrapy.Field()
    product_discount = scrapy.Field()


class ShopeeProductCommentsItem(scrapy.Item):
    product_itemid = scrapy.Field()
    product_shopid = scrapy.Field()
    scraped_on = scrapy.Field()
    product_comments = scrapy.Field()  # list(map(str, dynamic))


class ShopeeShareOfSearchRanksItem(scrapy.Item):
    product_ranks = scrapy.Field()


class ShopeeShareOfSearchItem(scrapy.Item):
    product_name = scrapy.Field()
    product_brand = scrapy.Field()
    product_original_price = scrapy.Field()
    product_categories = scrapy.Field()
    product_variations = scrapy.Field()
    product_images = scrapy.Field()
    product_videos = scrapy.Field()
    product_attributes = scrapy.Field()
    product_itemid = scrapy.Field()
    product_shopid = scrapy.Field()
    product_description = scrapy.Field()
    product_url = scrapy.Field()
    scraped_on = scrapy.Field()

    product_sale_price = scrapy.Field()
    product_rating = scrapy.Field()
    product_total_ratings = scrapy.Field()
    product_total_likes = scrapy.Field()
    product_stock = scrapy.Field()
    product_sold = scrapy.Field()
    product_historical_sold = scrapy.Field()
    product_original_price = scrapy.Field()
    product_discount = scrapy.Field()


class NykaaSearchProductList(scrapy.Item):
    products = scrapy.Field()


class NykaaProductInfoItem(scrapy.Item):
    product_name = scrapy.Field()
    product_brand = scrapy.Field()
    # product_category = scrapy.Field()
    # product_image_urls = scrapy.Field()
    product_id = scrapy.Field()
    # product_description = scrapy.Field()


class NykaaProductDailyMovementItem(scrapy.Item):
    product_sale_price = scrapy.Field()
    product_original_price = scrapy.Field()
    product_rating = scrapy.Field()
    product_total_ratings = scrapy.Field()
    product_total_reviews = scrapy.Field()
    product_availability = scrapy.Field()
    product_discount = scrapy.Field()
    product_id = scrapy.Field()


class NykaaRankItem(scrapy.Item):
    product_ranks = scrapy.Field()


class NykaaProductCommentsItem(scrapy.Item):
    product_id = scrapy.Field()
    scraped_on = scrapy.Field()
    product_comments = scrapy.Field()


class NykaaProductQuestionsItem(scrapy.Item):
    product_id = scrapy.Field()
    scraped_on = scrapy.Field()
    product_questions = scrapy.Field()


class NykaaShareOfSearchItem(scrapy.Item):
    product_details = scrapy.Field()

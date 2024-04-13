from datetime import datetime
import re


def is_float(element: str) -> bool:
    try:
        float(element)
        return True
    except ValueError:
        return False


class NykaaScrapingHelper:
    """
    A class used to return attributes of an amazon product using xpath.
    """

    def get_title(self, response):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        str
            title of the amazon product
        """

        title_xpath_text = (
            response.xpath(
                '//*[@id="app"]/div/div/div[1]/div[2]/div/div[1]/h1/text()'
            ).extract()
            or "NA"
        )
        title = title_xpath_text[0]
        return title

    def get_brand(self, response):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        str
            brand of the amazon product
        """
        brand_xpath_text = (
            response.xpath('//*[@id="app"]/div/div/div[1]/div[2]/div/div[1]/h1/text()')
        ).extract()

        brand_text = brand_xpath_text[0]
        brand = "NA"
        if "Dove" in brand_text:
            brand = "Dove"
        elif "L'Oreal Professionnel" in brand_text:
            brand = "L'Oreal Professionnel"
        elif "L'Oreal Paris" in brand_text:
            brand = "L'Oreal Paris"
        elif "Biotique" in brand_text:
            brand = "Biotique"
        elif "Tresemme" in brand_text:
            brand = "Tresemme"
        elif "Head & Shoulders" in brand_text:
            brand = "Head & Shoulders"
        return brand

    def get_sale_price(self, response, current_time):
        sale_price_xpath_text = response.xpath(
            '//*[@id="app"]/div/div/div[1]/div[2]/div/div[1]/div[2]/div/span[2]/text()'
        ).extract()
        sale_price = sale_price_xpath_text[0].replace("\u20b9", "")
        sale_price_dict = {}
        sale_price_dict["time"] = current_time
        sale_price_dict["value"] = sale_price
        return sale_price_dict

    def get_original_price(self, response, current_time):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        str
            original price of the amazon product
        """
        original_price = "NA"
        original_price_xpath_text = response.xpath(
            '//*[@id="app"]/div/div/div[1]/div[2]/div/div[1]/div[2]/div/span[1]/span/text()'
        ).extract()
        if original_price_xpath_text:
            original_price = original_price_xpath_text[0].replace("\u20b9", "")
        else:
            original_price_xpath_text = response.xpath(
                '//*[@id="app"]/div/div/div[1]/div[2]/div/div[1]/div[2]/div/span[2]/text()'
            ).extract()
            original_price = original_price_xpath_text[0].replace("\u20b9", "")

        original_price_dict = {}
        original_price_dict["time"] = current_time
        original_price_dict["value"] = original_price
        return original_price_dict

    def get_total_ratings(self, response, current_time):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        dict
            fulfilled of the amazon product
        """
        total_ratings_xpath_text = response.xpath(
            '//*[@id="app"]/div/div/div[1]/div[2]/div/div[1]/div[1]/div[2]/div[1]/text()[1]'
        ).extract() or ["NA"]
        total_ratings = total_ratings_xpath_text[0]
        total_ratings_dict = {}
        total_ratings_dict["time"] = current_time
        total_ratings_dict["value"] = total_ratings
        return total_ratings_dict

    def get_rating(self, response, current_time):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        dict
            rating of the amazon product
        """

        rating_xpath_text = response.xpath(
            '//*[@id="app"]/div/div/div[1]/div[2]/div/div[1]/div[1]/div[1]/div[1]/text()'
        ).extract() or ["NA"]
        rating = rating_xpath_text[0]
        rating_dict = {}
        rating_dict["time"] = current_time
        rating_dict["value"] = rating
        return rating_dict

    def get_total_reviews(self, response, current_time):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        dict
            total reviews of the amazon product
        """

        total_reviews_xpath_text = response.xpath(
            '//*[@id="app"]/div/div/div[1]/div[2]/div/div[1]/div[1]/div[2]/div[3]/text()[1]'
        ).extract() or ["NA"]
        total_reviews = total_reviews_xpath_text[0]
        total_reviews_dict = {}
        total_reviews_dict["time"] = current_time
        total_reviews_dict["value"] = total_reviews
        return total_reviews_dict

    def get_availability(self, response, current_time):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        dict
            availability of the amazon product
        """
        sale_price_xpath_text = response.xpath(
            '//*[@id="app"]/div/div/div[1]/div[2]/div/div[1]/div[2]/div/span[2]/text()'
        ).extract()
        sale_price = sale_price_xpath_text[0].replace("\u20b9", "")
        availability = "false"
        if sale_price is not None:
            availability = "true"
        availability_dict = {}
        availability_dict["time"] = current_time
        availability_dict["value"] = availability
        return availability_dict

    # def get_category(self, response):
    #     """
    #     Parameters
    #     ----------
    #     response : object
    #         represents an HTTP response

    #     Returns
    #     -------
    #     array
    #         category of the amazon product
    #     """
    #     category_xpath_text = response.xpath(
    #         '//*[@id="app"]/li/a/text()'
    #     ).extract()
    #     # category = [i.strip() for i in category_xpath_text]
    #     return category_xpath_text

    def get_product_id(self, response):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        str
            asin of the amazon product
        """

        product_id = response.xpath("/html/head/link[2]/@href").extract() or "NA"
        return product_id

    # def get_product_description(self, response):
    #     """
    #     Parameters
    #     ----------
    #     response : object
    #         represents an HTTP response

    #     Returns
    #     -------
    #     str
    #         product description of the amazon product
    #     """
    #     product_description_xpath_text = (
    #         response.xpath('//*[@id="content-details"]/p[1]/text()').extract()
    #     )
    #     product_description = "".join(product_description_xpath_text).strip()
    #     return product_description

    def get_discount(self, response, current_time):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        str
            subscription discount of the amazon product
        """
        discount = "NA"
        discount_xpath_text = response.xpath(
            '//*[@id="app"]/div/div/div[1]/div[2]/div/div[1]/div[2]/div/span[3]/text()'
        ).extract()
        if discount_xpath_text:
            discount = discount_xpath_text[0].split(" ")[0]
        discount_dict = {}
        discount_dict["time"] = current_time
        discount_dict["value"] = discount
        return discount_dict

    # def get_images(self, response):
    #     images = response.xpath('//*[@id="app"]/div/div/div[1]/div[1]/div[2]/div[1]/div/div/div/img/@src').extract()
    #     prod_images = []
    #     for img in images:
    #         # if "https://m.media-amazon.com/images/I" in img:
    #         prod_images.append(img)

    #     return prod_images

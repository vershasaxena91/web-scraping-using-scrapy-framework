from datetime import datetime
import re


def is_float(element: str) -> bool:
    try:
        float(element)
        return True
    except ValueError:
        return False


class AmazonScrapingHelper:
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
                '//h1[@id="title"]//span[@id="productTitle"]/text()'
            ).extract_first()
            or "NA"
        )
        title = title_xpath_text.strip()
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

        # brand = (
        #     response.xpath(
        #         '//td[@class="a-span9"]//span[@class="a-size-base"]/text()'
        #     ).extract()
        #     or "NA"
        # )
        # //*[@id="product-specification-table"]/tbody
        brand_xpath_text = (
            response.xpath('//table[@class="a-normal a-spacing-micro"]//span/text()')
        ).extract() or (
            response.xpath('//table[@id="product-specification-table"]//th/text()')
        ).extract()
        if brand_xpath_text and "Brand" in brand_xpath_text:
            brand_index = brand_xpath_text.index("Brand")
            brand = brand_xpath_text[brand_index + 1]
            return brand
        elif brand_xpath_text and "Brand Name" in brand_xpath_text:
            brand_index = brand_xpath_text.index("Brand Name")
            brand_xpath_text_name = (
                response.xpath('//table[@id="product-specification-table"]//td/text()')
            ).extract()
            brand = brand_xpath_text_name[brand_index].strip()
            return brand
        else:
            return ""

    def get_bullet_details(self, response):
        keys = response.xpath(
            '//table[@class="a-normal a-spacing-micro"]//td[1]/span/text()'
        ).extract()
        table = dict()
        for i in range(len(keys)):
            value_i = " ".join(
                response.xpath(
                    '//table[@class="a-normal a-spacing-micro"]//tr[{}]/td[2]/span/text()'.format(
                        i + 1
                    )
                ).extract()
            )
            table[keys[i]] = value_i

        return table

    def get_sale_price(self, response, current_time):
        # Table Layout
        # //*[@id="corePrice_desktop"]/div/table/tbody/tr[2]/td[2]/span[1]/span[1]
        # //*[@id="corePrice_desktop"]/div/table/tbody/tr[2]/td[2]/span[1]/span[2]
        # //*[@id="corePriceDisplay_desktop_feature_div"]/div/span[1]/span[2]/span[2]
        # if 2 values are given then MRP & Price
# //*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]/span[2]/span[2]
        price_xpaths = response.xpath(
            '//*[@id="corePrice_desktop"]/div//tr/td[2]/span[1]/span[1]/text()'
        )
        if len(price_xpaths) == 2:
            sale_price_xpath_text = price_xpaths[1].extract()
        elif len(price_xpaths) == 1:
            sale_price_xpath_text = price_xpaths[0].extract()
        else:
            sale_price_xpath_text = []

        sale_price_strip = (
            ("".join(sale_price_xpath_text).strip())
            .replace("\xa0", "")
            .replace("\u20b9", "")
            .replace(",", "")
        )

        if len(sale_price_xpath_text) == 0 or not is_float(sale_price_strip):

            sale_price_xpath_text = response.xpath(
                '//table[@class="a-lineitem a-align-top"]//td/text()'
            ).extract()
            if sale_price_xpath_text and "Deal Price:" in sale_price_xpath_text:
                sale_price_index = sale_price_xpath_text.index("Deal Price:")
                sale_price_xpath_text_name = (
                    response.xpath(
                        '//table[@class="a-lineitem a-align-top"]//span/text()'
                    )
                ).extract()
                sale_price_strip = (
                    sale_price_xpath_text_name[(sale_price_index) * 2]
                    .replace("\u20b9", "")
                    .replace(",", "")
                )

            if (
                sale_price_xpath_text
                and "Price:" in sale_price_xpath_text
                and "Deal Price:" not in sale_price_xpath_text
            ):
                sale_price_index = sale_price_xpath_text.index("Price:")
                sale_price_xpath_text_name = (
                    response.xpath(
                        '//table[@class="a-lineitem a-align-top"]//span/text()'
                    )
                ).extract()
                sale_price_strip = (
                    sale_price_xpath_text_name[(sale_price_index) * 2]
                    .replace("\u20b9", "")
                    .replace(",", "")
                )
                if not is_float(sale_price_strip) and "-" in sale_price_xpath_text_name:
                    sale_price_strip_list = [sale_price_xpath_text_name[2].replace("\u20b9", "").replace(",", ""), sale_price_xpath_text_name[4], sale_price_xpath_text_name[6].replace("\u20b9", "").replace(",", "")]
                    sale_price_strip = "".join(sale_price_strip_list)

            if sale_price_xpath_text and "Deal of the Day:" in sale_price_xpath_text:
                sale_price_index = sale_price_xpath_text.index("Deal of the Day:")
                sale_price_xpath_text_name = (
                    response.xpath(
                        '//table[@class="a-lineitem a-align-top"]//span/text()'
                    )
                ).extract()
                sale_price_strip = (
                    sale_price_xpath_text_name[(sale_price_index) * 2]
                    .replace("\u20b9", "")
                    .replace(",", "")
                )


        # //*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]/span[1]
        if len(sale_price_xpath_text) == 0:
            # Up MRP & Down Row Price
            # //*[@id="corePriceDisplay_desktop_feature_div"]/div/span[1]/span[1]
            # //*[@id="corePriceDisplay_desktop_feature_div"]/div/span[1]/span[2]/span[2]/text()
            sale_price_xpath_text = response.xpath(
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div/span[1]/span[1]/text()'
            ).extract()
            sale_price_strip = (
                ("".join(sale_price_xpath_text).strip())
                .replace("\xa0", "")
                .replace("\u20b9", "")
                .replace(",", "")
            )
            if not is_float(sale_price_strip):
                sale_price_xpath_text = []

        if len(sale_price_xpath_text) == 0:
            # Row Layout
            # //*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]/span[2]/span[2]
            sale_price_xpath_text = response.xpath(
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]/span[2]/span[2]/text()'
            ).extract()
            sale_price_strip = (
                ("".join(sale_price_xpath_text).strip())
                .replace("\xa0", "")
                .replace("\u20b9", "")
                .replace(",", "")
            )

        if len(sale_price_xpath_text) == 0:
            # Up Row Price & Down MRP
            # //*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]/span[1]
            # //*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]/span[1]
            sale_price_xpath_text = response.xpath(
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]/span[1]/text()'
            ).extract()
            sale_price_strip = (
                ("".join(sale_price_xpath_text).strip())
                .replace("\xa0", "")
                .replace("\u20b9", "")
                .replace(",", "")
            )

        # if len(sale_price_xpath_text) == 0 or not is_float(sale_price_strip):
        #     sale_price_xpath_text = response.xpath(
        #         '//*[@id="corePrice_desktop"]/div/tr/td[2]/span[1]/span[1]/text()'
        #     ).extract()
        #     print("##########")
        #     print(sale_price_xpath_text)
        #     sale_price_strip = (
        #         ("".join(sale_price_xpath_text).strip())
        #         .replace("\xa0", "")
        #         .replace("\u20b9", "")
        #         .replace(",", "")
        #     )

        # sale_price = []
        # now = datetime.now()
        # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        sale_price_dict = {}
        sale_price_dict["time"] = current_time
        sale_price_dict["value"] = sale_price_strip
        # sale_price.append(sale_price_dict)
        # print(sale_price_dict)
        return sale_price_dict

    '''
    def get_sale_price(self, response):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        dict
            object with current time and sale price of the amazon product
        """
        try:
            print("PS1")
            sale_price_xpath_text = response.xpath('//span[contains(@id,"priceblock_dealprice") or contains(@id,"priceblock_ourprice")]/text()').extract()
        except:
            sale_price_xpath_text = []

        print("P1 {}, {}".format(sale_price_xpath_text, type(sale_price_xpath_text)))

        try:
            print("PS2", len(sale_price_xpath_text))
            if len(sale_price_xpath_text) == 0:
                # print(response.xpath('//*[@id="corePrice_desktop"]/div').extract())
                sale_price_xpath_text = response.xpath('//*[@id="corePrice_desktop"]/div//span[1]/span[2]/text()').extract()
                print(sale_price_xpath_text, len(sale_price_xpath_text))
                if len(sale_price_xpath_text) > 1:
                    sale_price_xpath_text = sale_price_xpath_text[1]
                print(sale_price_xpath_text)
        except:
            sale_price_xpath_text = []
            
        print("P2 {}, {}".format(sale_price_xpath_text, type(sale_price_xpath_text)))

        try:
            print("PS3", len(sale_price_xpath_text))
            if len(sale_price_xpath_text) == 0:
                print(response.xpath('//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]/span[1]').extract())
                sale_price_xpath_text = response.xpath('//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]/span[1]/text()').extract()
        except:
            sale_price_xpath_text = []

        print("P3 {}, {}".format(sale_price_xpath_text, type(sale_price_xpath_text)))

        # if len(sale_price_xpath_text) == 0:
        #     sale_price_xpath_text = "NA"
# //*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]/span[2]/span[2]
# //*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]/span[1]
# /html/body/div[2]/div[2]/div[5]/div[3]/div[4]/div[10]/div[1]/div[1]/span[2]/span[2]/span[2]
# //*[@id="corePrice_desktop"]/div/table/tbody/tr[2]/td[2]/span[1]/span[2]
# response.xpath('//*[@class="a-price aok-align-center a-text-bold priceSizeOverride priceToPay"]/span[1]/text()').extract()
# //*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]/span[1]
# response.xpath('//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]//span[@class="a-offscreen"]/text()').extract()
# //*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]/span[2]/span[2]
# //*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]/span[1]
# //*[@id="corePrice_desktop"]/div/table/tbody/tr[2]/td[2]/span[1]/span[1]

        sale_price_strip = (
            ("".join(sale_price_xpath_text).strip())
            .replace("\xa0", "")
            .replace("\u20b9", "")
        )
        print("NO ERROR")
        # sale_price = []
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        sale_price_dict = {}
        sale_price_dict["time"] = current_time
        sale_price_dict["value"] = sale_price_strip
        # sale_price.append(sale_price_dict)
        print(sale_price_dict)
        return sale_price_dict
    '''

    def get_offers(self, response):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        str
            offer count of the amazon product
        """

        offers_xpath_text = response.xpath(
            '//span[@class="saving-prompt"]/text()'
        ).extract()
        if offers_xpath_text:
            offers_strip = "".join(offers_xpath_text).strip()
            offers = str(int(re.search(r"\d+", offers_strip).group()))
            return offers
        else:
            return "NA"

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

        # original_price_xpath_text = response.xpath('//span[@class="priceBlockStrikePriceString a-text-strike"]/text()').extract()
        # if len(original_price_xpath_text) == 0:
        #     print("O1")
        #     original_price_xpath_text = response.xpath('//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]//span[@class="a-offscreen"]/text()').extract()
        #     if len(original_price_xpath_text) == 0:
        #         print("O2")
        #         original_price_xpath_text = response.xpath('//*[@id="corePrice_desktop"]/div//span[1]/span[2]/text()').extract()
        #         if len(original_price_xpath_text) > 1:
        #             print("O3")
        #             original_price_xpath_text = original_price_xpath_text[0]

        # //*[@id="corePriceDisplay_desktop_feature_div"]/span/span[2]/span/span[2]

        # Table Layout
        # //*[@id="corePrice_desktop"]/div/table/tbody/tr[1]/td[2]/span[1]/span[2]
        # //*[@id="corePrice_desktop"]/div/table/tbody/tr[1]/td[2]/span[1]/span[1]

        # //*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span/span[1]
        # //*[@id="corePriceDisplay_desktop_feature_div"]/div[2]/span/span[1]/span/span[2]
        # //*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]/span[1]
        # //*[@id="corePrice_desktop"]/div/table/tbody/tr[1]/td[2]/span[1]/span[2]
        original_price = "NA"
        original_price_xpath_text = response.xpath(
            '//*[@id="corePrice_desktop"]/div/tr[1]/td[2]/span[1]/span[2]/text()'
        ).extract()
        # print("$$$$$$$$")
        # print(original_price_xpath_text)
        if original_price_xpath_text:
            original_price = original_price_xpath_text[0].replace("\u20b9", "")
            # return original_price
        # original_price = (
        #     ("".join(original_price_xpath_text).strip())
        #     .replace("\xa0", "")
        #     .replace("\u20b9", "")
        #     .replace(",", "")
        # )

        if len(original_price_xpath_text) == 0:
            original_price_xpath_text = response.xpath(
                '//table[@class="a-lineitem a-align-top"]//td/text()'
            ).extract()

            if original_price_xpath_text and "Price:" == original_price_xpath_text[0]:
                # print("########")
                # print(original_price_xpath_text)
                # original_price_index = original_price_xpath_text.index("Price:")
                original_price_xpath_text_name = (
                    response.xpath(
                        '//table[@class="a-lineitem a-align-top"]//span/text()'
                    )
                ).extract()
                original_price = (
                    original_price_xpath_text_name[0]
                    .replace("\u20b9", "")
                    .replace(",", "")
                )
                if not is_float(original_price) and "-" in original_price_xpath_text_name:
                    original_price_list = [original_price_xpath_text_name[2].replace("\u20b9", "").replace(",", ""), original_price_xpath_text_name[4], original_price_xpath_text_name[6].replace("\u20b9", "").replace(",", "")]
                    original_price = "".join(original_price_list)

                # return original_price

            if original_price_xpath_text and "M.R.P.:" == original_price_xpath_text[0]:
                # print("########")
                # print(original_price_xpath_text)
                # original_price_index = original_price_xpath_text.index("M.R.P.:")
                original_price_xpath_text_name = (
                    response.xpath(
                        '//table[@class="a-lineitem a-align-top"]//span/text()'
                    )
                ).extract()
                original_price = (
                    original_price_xpath_text_name[0]
                    .replace("\u20b9", "")
                    .replace(",", "")
                )

            if (
                original_price_xpath_text
                and "Bundle List Price: " == original_price_xpath_text[0]
            ):
                # print("########")
                # print(original_price_xpath_text)
                # original_price_index = original_price_xpath_text.index("M.R.P.:")
                original_price_xpath_text_name = (
                    response.xpath(
                        '//table[@class="a-lineitem a-align-top"]//span/text()'
                    )
                ).extract()
                original_price = (
                    original_price_xpath_text_name[0]
                    .replace("\u20b9", "")
                    .replace(",", "")
                )

                # return original_price

        # if len(original_price_xpath_text) == 0:# or not is_float(original_price_xpath_text):
        #     # Up MRP & Down Row Price
        #     # //*[@id="corePriceDisplay_desktop_feature_div"]/span/span[1]/span/span[2]
        #     original_price_xpath_text = response.xpath(
        #         '//div[@id="corePriceDisplay_desktop_feature_div"]//div//span//text()'
        #     ).extract()
        #     print('########')
        #     print(original_price_xpath_text)
        #     original_price = (
        #         ("".join(original_price_xpath_text).strip())
        #         .replace("\xa0", "")
        #         .replace("\u20b9", "")
        #         .replace(",", "")
        #     )

        # if len(original_price_xpath_text) == 0:# or not is_float(original_price_xpath_text):
        #     # Row Layout
        #     # //*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]/span[1]/span/span[2]
        #     original_price_xpath_text = response.xpath(
        #         '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]/span[1]/span/span[2]/text()'
        #     ).extract()
        #     original_price = (
        #         ("".join(original_price_xpath_text).strip())
        #         .replace("\xa0", "")
        #         .replace("\u20b9", "")
        #         .replace(",", "")
        #     )
        #     return original_price

        if (
            len(original_price_xpath_text) == 0
        ):  # or not is_float(original_price_xpath_text):
            # Up Row Price & Down MRP
            # //*[@id="corePriceDisplay_desktop_feature_div"]/div[2]/span/span[1]/span/span[2]
            original_price_xpath_text = response.xpath(
                '//*[@id="corePriceDisplay_desktop_feature_div"]//div[2]//span//span[1]//span//span[2][@aria-hidden="true"]//text()'
            ).extract()
            # print("##########")
            # print(original_price_xpath_text)
            original_price = (
                ("".join(original_price_xpath_text).strip())
                .replace("\xa0", "")
                .replace("\u20b9", "")
                .replace(",", "")
            )
            # return original_price
        if len(original_price_xpath_text) == 0:
            # Up MRP & Down Row Price
            # //*[@id="corePriceDisplay_desktop_feature_div"]/div/span[1]/span[1]
            # //*[@id="corePriceDisplay_desktop_feature_div"]/div/span[1]/span[2]/span[2]/text()
            original_price_xpath_text = response.xpath(
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div/span[1]/span[1]/text()'
            ).extract()
            original_price = (
                ("".join(original_price_xpath_text).strip())
                .replace("\xa0", "")
                .replace("\u20b9", "")
                .replace(",", "")
            )

        original_price_dict = {}
        original_price_dict["time"] = current_time
        original_price_dict["value"] = original_price
        # sale_price.append(sale_price_dict)
        # print(sale_price_dict)
        return original_price_dict

        # if len(original_price_xpath_text) == 0:# or not is_float(original_price_xpath_text):
        #     # Only Price & No MRP
        #     # //*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]/span[1]
        #     original_price_xpath_text = response.xpath(
        #         '//span[@class="a-price a-text-price"]//text()'
        #     ).extract()
        #     original_price = (
        #         ("".join(original_price_xpath_text).strip())
        #         .replace("\xa0", "")
        #         .replace("\u20b9", "")
        #         .replace(",", "")
        #     )

        #     return original_price

    def get_fullfilled(self, response, current_time):
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
        if response.xpath('//span[@class="a-icon-text-fba"]/text()').extract_first():
            fullfilled_xpath_text = response.xpath(
                '//span[@class="a-icon-text-fba"]/text()'
            ).extract_first()
            # fullfilled = []
            # now = datetime.now()
            # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
            fullfilled_dict = {}
            fullfilled_dict["time"] = current_time
            fullfilled_dict["value"] = fullfilled_xpath_text
            # fullfilled.append(fullfilled_dict)
            return fullfilled_dict

        elif "Fulfilled by Amazon" in (
            response.xpath('//*[@id="merchant-info"]/a[2]/span/text()').extract()
            or "NA"
        ):
            # fullfilled = []
            # now = datetime.now()
            # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
            fullfilled_dict = {}
            fullfilled_dict["time"] = current_time
            fullfilled_dict["value"] = "Fulfilled"
            # fullfilled.append(fullfilled_dict)
            return fullfilled_dict

        elif "Fulfilled by Amazon" in (
            response.xpath('//div[@id="merchant-info"]//a/text()').extract() or "NA"
        ):
            # fullfilled = []
            # now = datetime.now()
            # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
            fullfilled_dict = {}
            fullfilled_dict["time"] = current_time
            fullfilled_dict["value"] = "Fulfilled"
            # fullfilled.append(fullfilled_dict)
            return fullfilled_dict

        elif "fulfilled" in (
            response.xpath('//div[@id="merchant-info"]/text()').extract_first() or "NA"
        ):
            # fullfilled = []
            # now = datetime.now()
            # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
            fullfilled_dict = {}
            fullfilled_dict["time"] = current_time
            fullfilled_dict["value"] = "Fulfilled"
            # fullfilled.append(fullfilled_dict)
            return fullfilled_dict

        else:
            # fullfilled = []
            # now = datetime.now()
            # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
            fullfilled_dict = {}
            fullfilled_dict["time"] = current_time
            fullfilled_dict["value"] = "NA"
            # fullfilled.append(fullfilled_dict)
            return fullfilled_dict

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

        rating = response.xpath('//*[@id="acrPopover"]/@title').extract_first() or "NA"
        # now = datetime.now()
        # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
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

        total_reviews = (
            response.xpath('//*[@id="acrCustomerReviewText"]/text()').extract_first()
            or "NA"
        )
        # now = datetime.now()
        # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
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

        availability_xpath_text = (
            response.xpath('//div[@id="availability"]//text()').extract() or "NA"
        )
        availability_strip = (
            ("".join(availability_xpath_text).strip()).replace("\n", "")
        ).split(".")[0]

        # availability = []
        # now = datetime.now()
        # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        availability_dict = {}
        availability_dict["time"] = current_time
        availability_dict["value"] = availability_strip
        # availability.append(availability_dict)
        return availability_dict

    def get_category(self, response):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        array
            category of the amazon product
        """

        category_xpath_text = response.xpath(
            '//a[@class="a-link-normal a-color-tertiary"]/text()'
        ).extract()
        category = [i.strip() for i in category_xpath_text]
        return category

    def get_icons(self, response):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        array
            icons of the amazon product
        """

        icons_xpath_text = response.xpath(
            '//a[@class="a-size-small a-link-normal a-text-normal"]/text()'
        ).extract()
        icons = []
        for i in icons_xpath_text:
            icons.append(i.strip())
        return icons

    def get_best_seller_rank_1(self, response, current_time):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        dict
            object with current time and best seller rank of the amazon product
        """

        product_details_xpath_text = response.xpath(
            '//div[@id="detailBullets_feature_div"]//span/text()'
        ).getall()
        if product_details_xpath_text:
            product_details_strip = [
                i.strip().replace("\n", "") for i in product_details_xpath_text
            ]
            product_details = [
                i.replace("\u200f", "").replace("\u200e", "")
                for i in product_details_strip
                if i != ""
            ]
            if "Best Sellers Rank:" in product_details:
                seller_rank_1_xpath_text = response.xpath(
                    '//div[@id="detailBullets_feature_div"]//span[@class="a-list-item"]/text()'
                ).getall()
                seller_rank_2_xpath_text = response.xpath(
                    '//div[@id="detailBullets_feature_div"]//span[@class="a-list-item"]//a/text()'
                ).getall()
                seller_rank_1_strip = [
                    i.strip().replace("\n", "").replace("(", "").replace(")", "")
                    for i in seller_rank_1_xpath_text
                ]
                seller_rank_1 = [i for i in seller_rank_1_strip if i != ""]
                seller_rank_2_strip = [
                    i.strip().replace("\n", "") for i in seller_rank_2_xpath_text
                ]
                seller_rank_2 = [i for i in seller_rank_2_strip if i != ""]
                first_element_seller_rank = ["(", seller_rank_2[0], ")"]
                seller_rank_2[0] = "".join(first_element_seller_rank)
                seller_rank_list = []
                for i, j in zip(seller_rank_1, seller_rank_2):
                    seller_rank_list.append(i)
                    seller_rank_list.append(j)
                seller_rank = " ".join(seller_rank_list)
                # best_seller_rank = []
                # now = datetime.now()
                # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                best_seller_rank_dict = {}
                best_seller_rank_dict["time"] = current_time
                best_seller_rank_dict["value"] = seller_rank
                # best_seller_rank.append(best_seller_rank_dict)
                return best_seller_rank_dict

            else:
                seller_rank_xpath_text = response.xpath(
                    '//*[@id="detailBulletsWrapper_feature_div"]/ul[1]/li/span/span/text()'
                ).extract()
                if seller_rank_xpath_text:
                    seller_rank_1_xpath_text = response.xpath(
                        '//*[@id="detailBulletsWrapper_feature_div"]/ul[1]/li/span/text()'
                    ).extract()
                    seller_rank_2_xpath_text = response.xpath(
                        '//*[@id="detailBulletsWrapper_feature_div"]/ul[1]/li/span/a/text()'
                    ).extract()
                    seller_rank_3_xpath_text = response.xpath(
                        '//*[@id="detailBulletsWrapper_feature_div"]/ul[1]/li/span/ul/li/span/text()'
                    ).extract()
                    seller_rank_4_xpath_text = response.xpath(
                        '//*[@id="detailBulletsWrapper_feature_div"]/ul[1]/li/span/ul/li/span/a/text()'
                    ).extract()
                    seller_rank_1_strip = [
                        i.strip().replace("\n", "").replace("(", "").replace(")", "")
                        for i in seller_rank_1_xpath_text
                    ]
                    seller_rank_1 = [i for i in seller_rank_1_strip if i != ""]
                    seller_rank_2_strip = [
                        i.strip().replace("\n", "") for i in seller_rank_2_xpath_text
                    ]
                    seller_rank_2 = [i for i in seller_rank_2_strip if i != ""]
                    first_element_seller_rank = ["(", seller_rank_2[0], ")"]
                    seller_rank_2[0] = "".join(first_element_seller_rank)
                    seller_rank_3_strip = [
                        i.strip().replace("\n", "").replace("(", "").replace(")", "")
                        for i in seller_rank_3_xpath_text
                    ]
                    seller_rank_3 = [i for i in seller_rank_3_strip if i != ""]
                    seller_rank_4_strip = [
                        i.strip().replace("\n", "") for i in seller_rank_4_xpath_text
                    ]
                    seller_rank_4 = [i for i in seller_rank_4_strip if i != ""]
                    second_element_seller_rank = seller_rank_4[0]
                    seller_rank_4[0] = "".join(second_element_seller_rank)
                    seller_rank_list = []
                    for i, j, k, l in zip(
                        seller_rank_1, seller_rank_2, seller_rank_3, seller_rank_4
                    ):
                        seller_rank_list.append(i)
                        seller_rank_list.append(j)
                        seller_rank_list.append(k)
                        seller_rank_list.append(l)
                    seller_rank = " ".join(seller_rank_list)
                    # best_seller_rank = []
                    # now = datetime.now()
                    # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    best_seller_rank_dict = {}
                    best_seller_rank_dict["time"] = current_time
                    best_seller_rank_dict["value"] = seller_rank
                    # best_seller_rank.append(best_seller_rank_dict)
                    return best_seller_rank_dict

                else:
                    seller_rank = "NA"
                    # best_seller_rank = []
                    # now = datetime.now()
                    # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                    best_seller_rank_dict = {}
                    best_seller_rank_dict["time"] = current_time
                    best_seller_rank_dict["value"] = seller_rank
                    # best_seller_rank.append(best_seller_rank_dict)
                    return best_seller_rank_dict
        else:
            seller_rank = "NA"
            # best_seller_rank = []
            # now = datetime.now()
            # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
            best_seller_rank_dict = {}
            best_seller_rank_dict["time"] = current_time
            best_seller_rank_dict["value"] = seller_rank
            # best_seller_rank.append(best_seller_rank_dict)
            return best_seller_rank_dict

    def get_product_details_1(self, response, current_time):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        object
            product details of the amazon product
        """

        product_details_xpath_text = response.xpath(
            '//div[@id="detailBullets_feature_div"]//span/text()'
        ).getall()
        if product_details_xpath_text:
            product_details_strip = [
                i.strip().replace("\n", "") for i in product_details_xpath_text
            ]
            product_details = [
                i.replace("\u200f", "").replace("\u200e", "")
                for i in product_details_strip
                if i != ""
            ]
            if "Best Sellers Rank:" in product_details:
                index_best_seller_rank = product_details.index("Best Sellers Rank:")
                product_details = product_details[0:index_best_seller_rank]
            else:
                if "Customer Reviews:" in product_details:
                    index_best_seller_rank = product_details.index("Customer Reviews:")
                    product_details = product_details[0:index_best_seller_rank]
            details = {}
            i = 0
            while i < len(product_details):
                details[product_details[i].replace(":", "")] = product_details[i + 1]
                i += 2
            if self.get_best_seller_rank_1(response, current_time)["value"] != "NA":
                details["Best Sellers Rank"] = self.get_best_seller_rank_1(
                    response, current_time
                )["value"]
            if (
                self.get_rating(response, current_time) != {}
                and self.get_total_reviews(response, current_time) != {}
            ):
                details["Customer Reviews"] = " ".join(
                    [
                        self.get_rating(response, current_time)["value"],
                        self.get_total_reviews(response, current_time)["value"],
                    ]
                )
            return details

        return {}

    def get_best_seller_rank_2(self, response, current_time):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        dict
            object with current time and best seller rank of the amazon product
        """

        additional_information_xpath_text = response.xpath(
            '//table[@id="productDetails_detailBullets_sections1"]//tr//th/text()'
        ).getall()
        if additional_information_xpath_text:
            additional_information = [
                i.strip().replace("\n", "") for i in additional_information_xpath_text
            ]
            if "Best Sellers Rank" in additional_information:
                seller_rank_1_xpath_text = response.xpath(
                    '//table[@id="productDetails_detailBullets_sections1"]//span/text()'
                ).getall()
                seller_rank_2_xpath_text = response.xpath(
                    '//table[@id="productDetails_detailBullets_sections1"]//span//a/text()'
                ).getall()
                seller_rank_1_strip = [
                    i.strip().replace("\n", "").replace("(", "").replace(")", "")
                    for i in seller_rank_1_xpath_text
                ]
                seller_rank_1_list = [i for i in seller_rank_1_strip if i != ""]
                seller_rank_1 = [i for i in seller_rank_1_list if "#" in i]
                seller_rank_2_strip = [
                    i.strip().replace("\n", "") for i in seller_rank_2_xpath_text
                ]
                seller_rank_2 = [i for i in seller_rank_2_strip if i != ""]
                first_element_seller_rank = ["(", seller_rank_2[0], ")"]
                seller_rank_2[0] = "".join(first_element_seller_rank)
                seller_rank_list = []
                for i, j in zip(seller_rank_1, seller_rank_2):
                    seller_rank_list.append(i)
                    seller_rank_list.append(j)
                seller_rank = " ".join(seller_rank_list)
                # best_seller_rank = []
                # now = datetime.now()
                # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                best_seller_rank_dict = {}
                best_seller_rank_dict["time"] = current_time
                best_seller_rank_dict["value"] = seller_rank
                # best_seller_rank.append(best_seller_rank_dict)
                return best_seller_rank_dict
            else:
                seller_rank = "NA"
                # best_seller_rank = []
                # now = datetime.now()
                # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                best_seller_rank_dict = {}
                best_seller_rank_dict["time"] = current_time
                best_seller_rank_dict["value"] = seller_rank
                # best_seller_rank.append(best_seller_rank_dict)
                return best_seller_rank_dict
        else:
            seller_rank = "NA"
            # best_seller_rank = []
            # now = datetime.now()
            # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
            best_seller_rank_dict = {}
            best_seller_rank_dict["time"] = current_time
            best_seller_rank_dict["value"] = seller_rank
            # best_seller_rank.append(best_seller_rank_dict)
            return best_seller_rank_dict

    def get_product_details_2(self, response, current_time):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        object
            product details of the amazon product
        """

        general_information_xpath_left_side = response.xpath(
            '//table[@id="productDetails_techSpec_section_1"]//tr//th/text()'
        ).getall()
        general_information_strip_left_side = [
            i.strip().replace("\n", "") for i in general_information_xpath_left_side
        ]
        general_information_xpath_right_side = response.xpath(
            '//table[@id="productDetails_techSpec_section_1"]//tr//td/text()'
        ).getall()
        general_information_strip_right_side = [
            i.strip().replace("\n", "").replace("\u200e", "")
            for i in general_information_xpath_right_side
        ]

        product_details = {}
        for i, j in zip(
            general_information_strip_left_side, general_information_strip_right_side
        ):
            product_details[i] = j

        additional_information_xpath_left_side = response.xpath(
            '//table[@id="productDetails_detailBullets_sections1"]//tr//th/text()'
        ).getall()
        additional_information_strip_left_side = [
            i.strip().replace("\n", "") for i in additional_information_xpath_left_side
        ]
        additional_information_left_side = [
            i
            for i in additional_information_strip_left_side
            if i != "Customer Reviews" and i != "Best Sellers Rank"
        ]
        additional_information_xpath_right_side = response.xpath(
            '//table[@id="productDetails_detailBullets_sections1"]//tr//td/text()'
        ).getall()
        additional_information_strip_right_side = [
            i.strip().replace("\n", "").replace("\u200e", "")
            for i in additional_information_xpath_right_side
        ]
        additional_information_right_side = [
            i
            for i in additional_information_strip_right_side
            if "out of" not in i and i != ""
        ]

        for i, j in zip(
            additional_information_left_side, additional_information_right_side
        ):
            product_details[i] = j

        if "Customer Reviews" in additional_information_strip_left_side:
            product_details["Customer Reviews"] = " ".join(
                [
                    self.get_rating(response, current_time)["value"],
                    self.get_total_reviews(response, current_time)["value"],
                ]
            )
        if "Best Sellers Rank" in additional_information_strip_left_side:
            product_details["Best Sellers Rank"] = self.get_best_seller_rank_2(
                response, current_time
            )["value"]

        return product_details

    def get_asin(self, response):
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

        asin = (
            response.xpath("//*[@data-asin]").xpath("@data-asin").extract_first()
            or "NA"
        )
        return asin

    def get_important_information(self, response):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        str
            important information of the amazon product
        """

        important_information_xpath_text = (
            response.xpath(
                '//div[@id="important-information"]//div[@class="a-section content"]//p/text()'
            ).extract()
            or "NA"
        )
        important_information = "".join(important_information_xpath_text)
        return important_information

    def get_product_description(self, response):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        str
            product description of the amazon product
        """
        # //*[@id="productDescription"]/p/span/text()
        product_description_xpath_text = (
            response.xpath('//div[@id="productDescription"]//p//text()').extract() or ""
        )
        product_description = "".join(product_description_xpath_text).strip()
        return product_description

    def get_bought_together(self, response):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        array
            bought together of the amazon product
        """

        bought_together_xpath_text = response.xpath(
            '//div[@aria-hidden="true"]/text()'
        ).extract()
        bought_together_strip = [
            i.strip().replace("\n", "") for i in bought_together_xpath_text
        ]
        bought_together = [i for i in bought_together_strip if i != ""]
        return bought_together

    def get_subscription_discount(self, response, current_time):
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

        subscription_discount_xpath_text = response.xpath(
            '//div[@id="corePrice_desktop"]//tr[3]//td[2]//text()'
        ).getall()
        # subscription_discount_xpath_text = response.xpath(
        #     '//div[contains(@id,"corePrice_desktop") or contains(@id,"corePriceDisplay_desktop_feature_div")]//td[2]//text()'
        # ).extract()
        if subscription_discount_xpath_text and "%" in " ".join(
            subscription_discount_xpath_text
        ):
            subscription_discount_strip = (
                (" ".join(subscription_discount_xpath_text)).split("(")[1]
            ).split(")")[0]
            # if len(subscription_discount_xpath_text.strip().split("(")) != 1:
            #     subscription_discount_strip = (
            #         subscription_discount_xpath_text.strip().split("(")[1]
            #     ).split(")")[0]

            # subscription_discount = []
            # now = datetime.now()
            # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
            subscription_discount_dict = {}
            subscription_discount_dict["time"] = current_time
            subscription_discount_dict["value"] = subscription_discount_strip
            # subscription_discount.append(subscription_discount_dict)
            return subscription_discount_dict

        if subscription_discount_xpath_text and "%" not in " ".join(
            subscription_discount_xpath_text
        ):
            subscription_discount_xpath_text = response.xpath(
                '//div[@id="corePrice_desktop"]//tr[4]//td[2]//text()'
            ).getall()
            if subscription_discount_xpath_text:
                subscription_discount_strip = (
                    (" ".join(subscription_discount_xpath_text)).split("(")[1]
                ).split(")")[0]
                # if len(subscription_discount_xpath_text.strip().split("(")) != 1:
                #     subscription_discount_strip = (
                #         subscription_discount_xpath_text.strip().split("(")[1]
                #     ).split(")")[0]

                # subscription_discount = []
                # now = datetime.now()
                # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                subscription_discount_dict = {}
                subscription_discount_dict["time"] = current_time
                subscription_discount_dict["value"] = subscription_discount_strip
                # subscription_discount.append(subscription_discount_dict)
                return subscription_discount_dict

        if not subscription_discount_xpath_text:
            subscription_discount_xpath_text = response.xpath(
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]//text()'
            ).extract()
            if subscription_discount_xpath_text and "%" in " ".join(
                subscription_discount_xpath_text
            ):
                subscription_discount_strip = (
                    " ".join(subscription_discount_xpath_text)
                ).split("-")[1]
                # if len(subscription_discount_xpath_text.strip().split("(")) != 1:
                #     subscription_discount_strip = (
                #         subscription_discount_xpath_text.strip().split("(")[1]
                #     ).split(")")[0]

                # subscription_discount = []
                # now = datetime.now()
                # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
                subscription_discount_dict = {}
                subscription_discount_dict["time"] = current_time
                subscription_discount_dict["value"] = subscription_discount_strip
                # subscription_discount.append(subscription_discount_dict)
                return subscription_discount_dict

        # subscription_discount = []
        # now = datetime.now()
        # current_time = now.strftime("%Y-%m-%d %H:%M:%S")

        subscription_discount_dict = {}
        subscription_discount_dict["time"] = current_time
        subscription_discount_dict["value"] = "NA"
        # subscription_discount.append(subscription_discount_dict)
        return subscription_discount_dict

    def get_variations(self, response):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        array
            variations of the amazon product
        """

        variations_xpath_text = response.xpath(
            '//*[@id="inline-twister-expander-content-pattern_name"]//img/@alt'
        ).getall()
        # variations = response.xpath('//div[@id="inline-twister-row-pattern_name"]//span[@id="inline-twister-expanded-dimension-text-pattern_name"]/text()').getall()
        # variations = (
        #     response.xpath(
        #         '//div[@id="variation_pattern_name"]//img[@class="imgSwatch"]'
        #     )
        #     .xpath("@alt")
        #     .getall()
        # )
        variations = [i for i in variations_xpath_text if len(i) != 0]
        return variations

    # def get_featurewise_rating(self, response):
    #     """
    #     Parameters
    #     ----------
    #     response : object
    #         represents an HTTP response

    #     Returns
    #     -------
    #     array
    #         variations of the amazon product
    #     """

    #     response.xpath('//*[@id="cr-summarization-attributes-list"]')
    #     # //*[@id="cr-summarization-attribute-attr-scent"]

    def get_total_questions(self, response, current_time):
        # if response.xpath('//*[@id="askATFLink"]/span/text()').extract_first():
        #     total_questions = (
        #         response.xpath('//*[@id="askATFLink"]/span/text()').extract_first()
        #     ) or "NA"
        # # now = datetime.now()
        # # current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        #     total_questions_dict = {}
        #     total_questions_dict["time"] = current_time
        #     total_questions_dict["value"] = total_questions
        #     return total_questions_dict

        # if "See more answered questions" in response.xpath('//*[@id="a-autoid-39-announce"]/text()').extract():
        #     total_questions = 5
        #     total_questions_dict = {}
        #     total_questions_dict["time"] = current_time
        #     total_questions_dict["value"] = total_questions
        #     return total_questions_dict

        total_questions_xpath_text = response.xpath(
            '//div[@class="a-section askPaginationHeaderMessage"]//span//text()'
        ).extract()
        if total_questions_xpath_text:
            total_questions = (
                total_questions_xpath_text[0]
                .split("of")[1]
                .split("questions")[0]
                .strip()
            )
            total_questions_dict = {}
            total_questions_dict["time"] = current_time
            total_questions_dict["value"] = total_questions
            return total_questions_dict
        else:
            total_questions = "NA"
            total_questions_dict = {}
            total_questions_dict["time"] = current_time
            total_questions_dict["value"] = total_questions
            return total_questions_dict

    def get_images(self, response):
        images = response.xpath('//*[@id="altImages"]/ul/li/span//img/@src').extract()
        prod_images = []
        for img in images:
            if "https://m.media-amazon.com/images/I" in img:
                prod_images.append(img)

        return prod_images


class AmazonCommentsScrapingHelper:
    def get_comments(self, response):
        """
        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        array
            List of comments with metadata.
        """

        # XPaths for Reference
        # root:
        # //div[@class="a-section review aok-relative"]

        # username:
        # root + //span[@class="a-profile-name"]/text()
        # root + div/div/div[1]/a/div[2]/span/text()

        # ratings:
        # root + div/div/div[2]/a[1]//span/text()

        # title:
        # root + div/div/div[2]/a[2]//span/text()

        # date:
        # root + div/div/span/text()

        # Design:
        # root + div/div/div[3]/a[1]/text()

        # verified:
        # root + div/div/div[3]/span//span/text()

        # description:
        # root + div/div/div[4]/span/span/text()

        # helpful:
        # root + div/div/div[7]/div/span[1]//span/text()

        comments = []
        comms = response.xpath('//div[@class="a-section review aok-relative"]')
        for com in comms:
            where = com.xpath("div/div/span/text()").extract_first()
            where = where.split() if isinstance(where, str) else []
            title = com.xpath("div/div/div[2]/a[2]//span/text()").extract_first()
            if title is None:
                title = com.xpath("div/div/div[2]/span//span/text()").extract_first()
            comments.append(
                {
                    "username": com.xpath(
                        "div/div/div[1]//div[2]/span/text()"
                    ).extract_first(),
                    "rating": com.xpath("div/div/div[2]//span/text()").extract_first(),
                    "title": title,
                    "country": " ".join(
                        where[where.index("in") + 1 : where.index("on")]
                    ),
                    "date": datetime.strptime(
                        " ".join(where[where.index("on") + 1 :]), "%d %B %Y"
                    ),
                    "design": com.xpath("div/div/div[3]/a[1]/text()").extract_first(),
                    "verified": com.xpath(
                        "div/div/div[3]/span//span/text()"
                    ).extract_first(),
                    "description": com.xpath(
                        "div/div/div[4]/span/span/text()"
                    ).extract_first(),
                    "helpful": com.xpath(
                        "div/div/div[7]/div/span[1]//span/text()"
                    ).extract_first(),
                }
            )
        # print(comments)
        return comments


class AmazonQAScrapingHelper:
    def get_QAs(self, response):
        # XPaths for Reference
        # root:
        # //*[@id="a-page"]/div[1]/div[6]/div/div/div[*]/div

        # votes:
        # //*[@id="a-page"]/div[1]/div[6]/div/div/div[*]/div/div[1]/ul/li[2]/span[1]/text()

        # question:
        # //*[@id="a-page"]/div[1]/div[6]/div/div/div[*]/div/div[2]/div/div/div[2]/a/span/text()

        # answer
        # //*[@id="a-page"]/div[1]/div[6]/div/div/div[*]/div/div[2]/div[2]/div/div[2]/span[1]/text()
        # //*[@id="a-page"]/div[1]/div[6]/div/div/div[5]/div/div[2]/div[2]/div/div[2]/span[1]
        # //*[@id="a-page"]/div[1]/div[6]/div/div/div[6]/div/div[2]/div[2]/div/div[2]/span[1]/span[2]

        # answerer
        # //*[@id="a-page"]/div[1]/div[6]/div/div/div[*]/div/div[2]/div[2]/div/div[2]/div[1]//div[2]/span/text()
        # //*[@id="a-page"]/div[1]/div[6]/div/div/div[1]/div/div[2]/div[2]/div/div[2]/div/div/div[2]/span

        # answered on
        # //*[@id="a-page"]/div[1]/div[6]/div/div/div[*]/div/div[2]/div[2]/div/div[2]/div[1]/span/text()

        QAs = []
        QA_xpath = response.xpath('//*[@id="a-page"]/div[1]/div[6]/div/div/div/div')
        print(len(QA_xpath))
        for qa in QA_xpath:
            # print(str(qa))
            # print(str(qa.xpath('div[2]/div/div/div[2]/a/span/text()').extract_first().strip()))
            # print(str(qa.xpath('div[2]/div[2]/div/div[2]/span[1]/text()').extract_first().strip()))
            # print(str(qa.xpath('div[2]/div[2]/div/div[2]/div[1]//div[2]/span/text()').extract_first().strip())),
            # print(str(qa.xpath('div[2]/div[2]/div/div[2]/div[1]/span/text()').extract()[-1].strip()))
            # //*[@id="a-page"]/div[1]/div[6]/div/div/div[10]/div/div[2]/div[2]/div/div[2]/span[1]/a/text()
            ans = (
                qa.xpath("div[2]/div[2]/div/div[2]/span[1]//text()")
                .extract_first()
                .strip()
            )
            if ans == "":
                ans_parts = qa.xpath(
                    "div[2]/div[2]/div/div[2]/span[1]/span[2]//text()"
                ).extract()
                ans = ""
                for p in ans_parts:
                    ans += p.strip()
            # //*[@id="a-page"]/div[1]/div[6]/div/div/div[10]/div/div[2]/div[2]/div/div[2]/div[2]/a/div[2]/span
            QAs.append(
                {
                    "question": qa.xpath("div[2]/div/div/div[2]/a/span/text()")
                    .extract_first()
                    .strip(),
                    "answer": ans,
                    "votes": qa.xpath("div[1]/ul/li[2]/span[1]/text()")
                    .extract_first()
                    .strip(),
                    "username": qa.xpath(
                        "div[2]/div[2]/div/div[2]/div//div[2]/span/text()"
                    )
                    .extract_first()
                    .strip(),
                    "date": datetime.strptime(
                        qa.xpath("div[2]/div[2]/div/div[2]/div/span/text()")
                        .extract()[-1]
                        .strip(),
                        "· %d %B, %Y",
                    ),
                }
            )
            # print("\n")

        return QAs

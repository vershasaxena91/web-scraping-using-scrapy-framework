from datetime import datetime
from dateutil.relativedelta import relativedelta
from pem import Key


class FlipkartInfoScrapingHelper:
    def get_title(self, response, marketplace):
        # //*[@id="container"]/div/div[2]/div[3]/div[2]/div[2]/div/div[1]/h1/span/text()[1]
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[1]/h1/span/text()
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[1]/div[1]/div/div[7]/div/div/p

        # //*[@id="container"]/div/div[2]/div[3]/div[2]/div[2]/div/div[1]/h1/span/text()[1]
        # print(response.xpath('//*[@id="container"]/div/div[2]/div[3]/div[2]/div[2]/div/div[1]/h1/span/text()').extract_first())
        # title_xpath_text = response.xpath('//*[@id="container"]/div/div/div/div[2]/div[2]/div/div[1]/h1/span/text()').extract_first()
        # title_xpath_text = response.xpath('//*[@id="container"]/div/div/div/div[2]/div[2]/div/div[1]/h1/span/text()').extract_first()
        #                                    //*[@id="container"]/div/div/div/div[2]/div[3]/div/div[1]/h1/span/text()[1]
        if marketplace == "FLIPKART" or marketplace == "GROCERY":
            title_xpath_text = response.xpath(
                '//*[@id="container"]/div/div/div/div/div/div/div[1]/h1/span/text()'
            ).extract_first()
        else:
            print("**DEBUG:**\n", "NO TITLE")
        title = title_xpath_text.strip()
        return title

    def get_brand(self, response, marketplace):
        # //*[@id="container"]/div/div[2]/div[3]/div[2]/div[1]/div[1]/div/div[6]/a/text()
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[1]/div[1]/div/div[6]/a/text()
        # brand_xpath_text = response.xpath('//*[@id="container"]/div/div[2]/div[3]/div[2]/div[1]/div[1]/div/div[6]/a/text()').extract_first()
        # brand_xpath_text = response.xpath('//*[@id="container"]/div/div[3]/div[1]/div[2]/div[1]/div[1]/div/div[6]/a/text()').extract_first()
        if marketplace == "FLIPKART" or marketplace == "GROCERY":
            brand_xpath_text = response.xpath(
                '//*[@id="container"]/div/div/div/div/div/div[1]/div/div[6]/a/text()'
            ).extract_first()
        else:
            print("**DEBUG:**\n", "NO BRAND")
        brand = brand_xpath_text.strip()

        return brand

    def get_sale_price(self, response, marketplace, current_time):
        # //*[@id="container"]/div/div[2]/div[3]/div[2]/div[2]/div/div[3]/div[1]/div/div[1]/text()
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[3]/div[1]/div/div[1]
        # //*[@id="container"]/div/div[2]/div[3]/div[2]/div[2]/div/div[2]/div[1]/div/div
        # response.xpath('//*[@id="container"]/div/div/div/div[2]/div[2]/div/div/div[1]/div/div[1]/text()').extract()
        # sale_price_xpath_text = response.xpath('//*[@id="container"]/div/div[2]/div[3]/div[2]/div[2]/div/div[3]/div[1]/div/div[1]/text()').extract_first()
        # sale_price_xpath_text = response.xpath('//*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[3]/div[1]/div/div[1]/text()').extract_first()
        if marketplace == "FLIPKART" or marketplace == "GROCERY":
            # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[3]/div/div[3]/div[1]/div/div
            sale_price_xpath_text = (
                response.xpath(
                    '//*[@id="container"]/div/div/div/div/div/div/div/div[1]/div/div[1]/text()'
                ).extract_first()
                or ""
            )
        else:
            print("**DEBUG:**\n", "NO SALE PRICE")
        sale_price_strip = (
            ("".join(sale_price_xpath_text).strip())
            .replace("\xa0", "")
            .replace("\u20b9", "")
        )

        sale_price_dict = {}
        sale_price_dict["time"] = current_time
        sale_price_dict["value"] = sale_price_strip

        return sale_price_dict

    def get_original_price(self, response, marketplace):
        # //*[@id="container"]/div/div[2]/div[3]/div[2]/div[2]/div/div[3]/div[1]/div/div[2]/text()[2]
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[4]/div[1]/div/div[2]/text()[2]
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[3]/div[1]/div/div[2]/text()[2]
        # original_price_xpath_text = response.xpath('//*[@id="container"]/div/div[2]/div[3]/div[2]/div[2]/div/div[3]/div[1]/div/div[2]/text()[2]').extract()
        # original_price_xpath_text = response.xpath('//*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[3]/div[1]/div/div[2]/text()[2]').extract()
        if marketplace == "FLIPKART" or marketplace == "GROCERY":
            original_price_xpath_text = response.xpath(
                '//*[@id="container"]/div/div/div/div/div/div/div/div[1]/div/div[2]/text()[2]'
            ).extract()
        else:
            print("**DEBUG:**\n", "NO ORIGINAL PRICE")
        original_price = (
            ("".join(original_price_xpath_text).strip())
            .replace("\xa0", "")
            .replace("\u20b9", "")
        )

        return original_price

    def get_highlights(self, response, marketplace):
        # //*[@id="container"]/div/div[2]/div[3]/div[2]/div[7]/div[1]/div/div[2]/ul/li/text()
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[5]/div[1]/div/div[2]/ul/li/text()
        # highlights_xpath_text = response.xpath('//*[@id="container"]/div/div[2]/div[3]/div[2]/div[7]/div[1]/div/div[2]/ul/li/text()').extract()
        # highlights_xpath_text = response.xpath('//*[@id="container"]/div/div[3]/div[1]/div[2]/div[5]/div[1]/div/div[2]/ul/li/text()').extract()
        if marketplace == "FLIPKART" or marketplace == "GROCERY":
            highlights_xpath_text = response.xpath(
                '//*[@id="container"]/div/div/div/div/div/div[1]/div/div[2]/ul/li/text()'
            ).extract()
        else:
            print("**DEBUG:**\n", "NO HIGHLIGHTS")
        return highlights_xpath_text

    def get_services(self, response):
        # //*[@id="container"]/div/div[2]/div[3]/div[2]/div[7]/div[2]/div/ul/li/div[2]/text()
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[5]/div[2]/div/ul/li/div[2]/text()
        # services_xpath_text = response.xpath('//*[@id="container"]/div/div[2]/div[3]/div[2]/div[7]/div[2]/div/ul/li/div[2]/text()').extract()
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[7]/div[2]/div/ul/li[1]/div[2]/text()
        services_xpath_text = response.xpath(
            '//*[@id="container"]/div/div/div/div/div/div[2]/div/ul/li/div[2]/text()'
        ).extract()

        return services_xpath_text

    def get_product_details(self, response):
        # //*[@id="container"]/div/div[2]/div[3]/div[2]/div[8]/div[3]/div/div[2]/div[1]/table/tbody/tr/td[1]/text()
        # //*[@id="container"]/div/div[2]/div[3]/div[2]/div[8]/div[3]/div/div[2]/div[1]/table/tbody/tr/td[2]/ul/li/text()
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[6]/div[3]/div/div[2]/div[2]/ul/li/div
        # product_details_xpath_keys = response.xpath('//*[@id="container"]/div/div[2]/div[3]/div[2]/div[8]/div[3]/div/div[2]/div[1]/table/tbody/tr/td[1]/text()').extract()
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[6]/div[4]/div/div[2]/div[1]/table/tbody/tr[1]/td[1]
        # //*[@id="container"]/div/div[2]/div[3]/div[2]/div[8]/div[4]/div/div[2]/div[1]/table/tbody/tr[1]/td[1]
        # response.xpath('//*[@id="container"]/div/div/div/div[2]/div/div/div/div[2]/div[1]/table/tbody/tr[1]/td[1]/text()')
        # product_details_xpath_keys = response.xpath('//*[@id="container"]/div/div[3]/div[1]/div[2]/div[6]/div[3]/div/div[2]/div[2]/ul/li/div/text()').extract()

        # product_details_xpath_keys = response.xpath('//*[@id="container"]/div/div/div/div[2]/div/div/div/div[2]/div[1]/ul/li/div/text()').extract()
        # //*[@id="container"]/div/div/div/div[2]/div/div/div/div/div/div[2]/div[1]/table/tbody/tr/td
        product_details_xpath_keys = response.xpath(
            '//*[@id="container"]/div/div/div/div[2]/div/div/div/div//div[1]/table/tbody/tr/td/text()'
        ).extract()
        details = {}
        for k in range(len(product_details_xpath_keys)):
            # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[6]/div[3]/div/div[2]/div[2]/ul/li/ul/li/text()
            product_details_xpath_values = response.xpath(
                '//*[@id="container"]/div/div/div/div[2]/div/div/div/div//div[1]/table/tbody/tr[{}]/td[2]/ul/li/text()'.format(
                    k + 1
                )
            ).extract()
            # product_details_xpath_values = response.xpath('//*[@id="container"]/div/div/div/div[2]/div/div/div/div[2]/div[2]/ul/li[{}]/ul/li/text()'.format(k+1)).extract()
            details[product_details_xpath_keys[k]] = product_details_xpath_values

        return details

    def get_product_description(self, response):
        # //*[@id="container"]/div/div[2]/div[3]/div[2]/div[8]/div[3]/div/div[2]/div[1]/p[1]/text()
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[6]/div[3]/div/div[2]/div[1]/text()
        # product_description_xpath_text = response.xpath('//*[@id="container"]/div/div[2]/div[3]/div[2]/div[8]/div[3]/div/div[2]/div[1]/p[1]/text()').extract()
        # //*[@id="container"]/div/div/div/div[2]/div[8]/div[4]/div/div[2]/div[1]
        # //*[@id="container"]/div/div/div/div[2]/div[8]/div[4]/div/div[2]/div[1]/p[1]/text()
        # product_description_xpath_text = response.xpath('//*[@id="container"]/div/div/div/div[2]/div/div/div/div[2]/div[1]//text()').extract()
        # //*[@id="container"]/div/div/div/div/div/div[3]/div/div[2]/div[1]
        # product_description_xpath_text = response.xpath('//*[@id="container"]/div/div/div/div/div/div[4]/div/div[2]/div[1]//text()').extract()
        # //*[@id="container"]/div/div/div/div/div/div[3]/div/div[1]
        # //*[@id="container"]/div/div/div/div/div/div[4]/div/div[1]
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[7]/div[3]/div/div[1]
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[6]/div[1]/div/div[1]
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[6]/div[2]/div/div
        if (
            response.xpath(
                '//*[@id="container"]/div/div/div/div/div/div[3]/div/div[1]//text()'
            ).extract_first()
            == "Description"
        ):
            product_description_xpath_text = response.xpath(
                '//*[@id="container"]/div/div/div/div/div/div[3]/div/div[2]/div[1]//text()'
            ).extract()
            for i in range(len(product_description_xpath_text)):
                product_description_xpath_text[i] = product_description_xpath_text[
                    i
                ].strip()
            product_description = " ".join(product_description_xpath_text).strip()
            return product_description
        else:
            return ""

    def get_category(self, response):
        # //*[@id="container"]/div/div[2]/div[3]/div[2]/div[1]/div[1]/div/div/a/text()
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[1]/div[1]/div/div/a/text()
        # category_xpath_text = response.xpath('//*[@id="container"]/div/div[2]/div[3]/div[2]/div[1]/div[1]/div/div/a/text()').extract()[1:]
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div[1]/div/div[5]/a
        category_xpath_text = response.xpath(
            '//*[@id="container"]/div/div[3]/div[1]/div[2]/div/div[1]/div/div/a/text()'
        ).extract()[1:]

        category = [i.strip() for i in category_xpath_text]
        return category

    def get_availability(self, response, current_time):
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[3]
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[3]
        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[3]/div[1]
        availability_xpath_text = response.xpath(
            '//*[@id="container"]/div/div/div/div[2]/div[3]/div[1]//text()'
        ).extract()[-1]

        availability_strip = (
            ("".join(availability_xpath_text).strip()).replace("\n", "")
        ).split(".")[0]

        availability_dict = {}
        availability_dict["time"] = current_time
        availability_dict["value"] = availability_strip
        return availability_dict

    def get_rating(self, response, marketplace, current_time):
        if marketplace == "FLIPKART":
            # //*[@id="container"]/div/div/div/div[2]/div[3]/div/div[2]/div/div
            rating = response.xpath(
                '//*[@id="container"]/div/div/div/div[2]/div/div/div[2]/div/div/span[1]//text()'
            ).extract_first()
        else:
            rating = "NA"

        rating_dict = {}
        rating_dict["time"] = current_time
        rating_dict["value"] = rating
        return rating_dict

    def get_total_reviews(self, response, marketplace, current_time):
        if marketplace == "FLIPKART":
            total_reviews_xpath_text = response.xpath(
                '//*[@id="container"]/div/div/div/div[2]/div/div/div[2]/div/div/span[2]/span/span[1]/text()'
            ).extract()
            total_reviews = (
                ("".join(total_reviews_xpath_text).strip())
                .replace("\xa0", "")
                .replace("\u20b9", "")
            )
        else:
            total_reviews = "NA"

        total_reviews_dict = {}
        total_reviews_dict["time"] = current_time
        total_reviews_dict["value"] = total_reviews
        return total_reviews_dict

    # //*[@id="container"]/div/div[2]/div[3]/div[2]/div[5]/div/div/div[2]/div/ul/div/div[1]/span
    # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[4]/div/div/div[2]/div[1]/ul/div/div[1]/span

    # //*[@id="container"]/div/div[3]/div[4]/div[2]/div[2]/div/div[2]/div[1]/div/div
    # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[3]/div[1]/div/div[1]


class FlipkartCommentsScrapingHelper:
    def get_comments(self, response, current_time: datetime):
        comments = []
        # //*[@id="container"]/div/div[3]/div/div[1]/div[2]/div[3]
        # //*[@id="container"]/div/div[3]/div/div[1]/div[2]/div[4]
        # //*[@id="container"]/div/div[3]/div/div[1]/div[2]/div[3]
        comms = response.xpath('//*[@id="container"]/div/div[3]/div/div[1]/div[2]/div')
        for com in comms:
            # //*[@id="container"]/div/div[3]/div/div/div[2]/div[4]/div/div/div/div[1]/div/text()
            # To remove: //*[@id="container"]/div/div[3]/div/div/div[2]/div[3]
            if (
                " ".join(com.xpath("div/div/div/div[1]/div/text()").extract()).strip()
                != ""
            ):
                d = " ".join(
                    com.xpath("div/div/div/div/div[1]/p[3]/text()").extract()
                ).strip()
                if "ago" in d or "ago" in d:
                    if "day" in d or "days" in d:
                        date = current_time - relativedelta(days=int(d.split()[0]))
                        date = date.replace(hour=0, minute=0, second=0)
                    elif "month" in d or "months" in d:
                        date = current_time - relativedelta(months=int(d.split()[0]))
                        date = date.replace(hour=0, minute=0, second=0)
                    elif "year" in d or "years" in d:
                        date = current_time - relativedelta(years=int(d.split()[0]))
                        date = date.replace(hour=0, minute=0, second=0)
                    else:
                        print("RELATIVE DATE", d)
                        date = current_time
                else:
                    date = (
                        datetime.strptime(d, "%b, %Y")
                        + relativedelta(months=1)
                        - relativedelta(days=1)
                    )

                comments.append(
                    {
                        "rating": " ".join(
                            com.xpath("div/div/div/div[1]/div/text()").extract()
                        ).strip(),
                        "title": " ".join(
                            com.xpath("div/div/div/div[1]/p/text()").extract()
                        ).strip(),
                        "description": " ".join(
                            com.xpath("div/div/div/div[2]/div/div/div/text()").extract()
                        ).strip(),
                        "username": " ".join(
                            com.xpath("div/div/div/div/div[1]/p[1]/text()").extract()
                        ).strip(),
                        "verified": " ".join(
                            com.xpath(
                                "div/div/div/div/div[1]/p[2]/span/text()"
                            ).extract()
                        ).strip(),
                        "date": date,
                        "likes": " ".join(
                            com.xpath(
                                "div/div/div/div/div[2]/div/div[1]/div[1]/span/text()"
                            ).extract()
                        ).strip(),
                        "dislikes": " ".join(
                            com.xpath(
                                "div/div/div/div/div[2]/div/div[1]/div[2]/span/text()"
                            ).extract()
                        ).strip(),
                    }
                )
        return comments


class FlipkartAllInfoHelper:
    """
    Structure:
    <div>                           //*[@id="container"]/div/div[3]/div[1]/div[2]/div[1]
        Categories                  //*[@id="container"]/div/div[3]/div[1]/div[2]/div[$]/div[1]/div
                                    //*[@id="container"]/div/div[3]/div[1]/div[2]/div[$]/div[1]/div/div[*]/a
    </div>
    <div>                           //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]
        Title                       //*[@id="container"]/div/div[3]/div[1]/div[2]/div[$]/div/div[1]/h1/span/text()[1]
        Rating & Review & Assured   //*[@id="container"]/div/div[3]/div[1]/div[2]/div[$]/div/div[2]
        Price                       //*[@id="container"]/div/div[3]/div[1]/div[2]/div[$]/div/div[3]/div[1]/div
    </div>
    ...
    <div>                           //*[@id="container"]/div/div[3]/div[1]/div[2]/div[6+]
        Highlights                  //*[@id="container"]/div/div[3]/div[1]/div[2]/div[$]/div[1]
        Services                    //*[@id="container"]/div/div[3]/div[1]/div[2]/div[$]/div[2]
    </div>
    ...
    <div>                           //*[@id="container"]/div/div[3]/div[1]/div[2]/div[7+]
        Important Note              //*[@id="container"]/div/div[3]/div[1]/div[2]/div[6]/div[1]
        ...
        Seller                      //*[@id="container"]/div/div[3]/div[1]/div[2]/div[$]/div[1+]
        ...
        Description                 //*[@id="container"]/div/div[3]/div[1]/div[2]/div[$]/div[3+]
        ...
        Specs                       //*[@id="container"]/div/div[3]/div[1]/div[2]/div[$]/div[4+]
    </div>
    //*[@id="container"]/div/div[3]/div[1]/div[2]/div[7]
    """

    def __init__(self, response, time) -> None:
        self.time = time
        self.response = response

    def getInfo(self):
        # TODO
        # / Title
        # / Brand
        # / Categories
        # / Specs
        # / Descriptions
        # / Highlights
        # / Original Price
        info = dict()
        body = self.response.xpath('//*[@id="container"]/div/div[3]/div[1]/div[2]')
        divs_1 = body.xpath("div")
        info["categories"] = None
        info["brand"] = None
        info["title"] = None
        info["original_price"] = None
        info["highlights"] = None
        info["services"] = None
        info["description"] = None
        info["specifications"] = None

        for i in range(len(divs_1)):
            if not info["categories"]:
                temp = divs_1[i].xpath("div[1]/div/div/a/text()").extract()
                if temp and len(temp) > 0:
                    info["categories"] = temp[1:]
                    info["brand"] = temp[-1]
            if not info["title"]:
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[1]/h1/span/text()[1]
                temp = divs_1[i].xpath("div/div[1]/h1/span/text()[1]").extract_first()
                if temp:
                    info["title"] = temp
            if not info["original_price"]:
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[3]
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[3]/div[1]/div/div[2]/text()[2]
                # div/div/div[1]/div/div[2]/text()[2]
                temp = (
                    divs_1[i].xpath("div/div/div[1]/div/div[2]/text()").extract_first()
                )
                if temp:
                    info["original_price"] = (
                        ("".join(temp).strip())
                        .replace("\xa0", "")
                        .replace("\u20b9", "")
                        .replace(",", "")
                    )
            if not info["highlights"]:
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[6]/div[1]/div/div[1]
                if (
                    divs_1[i].xpath("div[1]/div/div[1]/text()").extract_first()
                    == "Highlights"
                ):
                    # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[6]/div[1]/div/div[2]
                    temp = divs_1[i].xpath("div[1]/div//ul/li/text()").extract()
                    if temp and len(temp) > 0:
                        info["highlights"] = temp
            if not info["services"]:
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[6]/div[2]/div/div
                # //*[@id="container"]/div/div   /div   /div   /div   /div[2]/div/ul/li/div[2]/text()
                if (
                    divs_1[i].xpath("div[2]/div/div[1]/text()").extract_first()
                    == "Services"
                ):
                    temp = divs_1[i].xpath("div[2]/div//ul/li/div/text()").extract()
                    if temp and len(temp) > 0:
                        info["services"] = temp
            if (not info["description"]) and (not info["specifications"]):
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[7]/div[3]/div/div[1]
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[7]/div[3]/div/div[1]
                # //*[@id="container"]/div/div   /div   /div   /div   /div[3]/div/div[2]/div[1]//text()
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[6]/div[5]/div/div[2]/div[1]/p/text()
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[7]/div[4]/div/div[1]
                # //*[@id="container"]/div/div   /div   /div[2]/div   /div   /div/div//div[1]/table/tbody/tr/td/text()
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[8]/div[3]/div/div[2]/div[1]/div[2]/table/tbody/tr[*]/td[1]
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[8]/div[3]/div/div[2]/div[1]/div[*]/table/tbody/tr[*]/td[1]
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[7]/div[4]/div/div[2]/div[1]/div[*]/div
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[7]/div[4]/div/div[2]/div[1]/div[*]/div
                divs_2 = divs_1[i].xpath("div")
                # print(divs_2)
                for j in range(len(divs_2)):
                    if (
                        divs_2[j].xpath("div/div[1]/text()").extract_first()
                        == "Description"
                    ):
                        temp = divs_2[j].xpath("div/div[2]//text()").extract()
                        if temp and len(temp) > 0:
                            for i in range(len(temp)):
                                temp[i] = temp[i].strip()
                            info["description"] = " ".join(temp).strip()
                    elif (
                        divs_2[j].xpath("div/div[1]/text()").extract_first()
                        == "Specifications"
                    ):
                        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[7]/div[4]/div/div[1]
                        # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[7]/div[4]/div/div[2]/div[1]/div[2]/table/tbody/tr[1]/td[1]
                        temp = divs_2[j].xpath("div/div[2]/div[1]//table/tbody/tr")
                        info["specifications"] = dict()
                        # print(temp)
                        for pair in temp:
                            k_v = pair.xpath("td")
                            if k_v and len(k_v) == 2:
                                key = k_v[0].xpath("text()").extract_first()
                                value = k_v[-1].xpath(".//text()").extract()
                            elif k_v and len(k_v) == 1:
                                value = k_v[-1].xpath(".//text()").extract()
                                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[7]/div[4]/div/div[2]/div[1]/div[1]/div
                                key = (
                                    divs_2[j]
                                    .xpath("div/div[2]/div[1]/div//text()")
                                    .extract_first()
                                )
                            else:
                                print("Flipkart Specifications Error : Pair: ", k_v)
                                key = None
                                value = None
                            # print(pair, key, value)
                            value = ",".join(value)
                            if key and value:
                                info["specifications"][key] = value

        return info

    def getMovement(self):
        # TODO
        # / Price
        # / Rating
        # / Reviews
        # / Assured
        # / Availability
        move = dict()
        body = self.response.xpath('//*[@id="container"]/div/div[3]/div[1]/div[2]')
        divs_1 = body.xpath("div")

        move["sale_price"] = {"time": self.time, "value": None}
        move["original_price"] = {"time": self.time, "value": None}
        move["rating"] = {"time": self.time, "value": None}
        move["num_reviews"] = {"time": self.time, "value": None}
        move["num_ratings"] = {"time": self.time, "value": None}
        move["assured"] = {"time": self.time, "value": None}
        move["availability"] = {"time": self.time, "value": None}

        for i in range(len(divs_1)):
            if not move["rating"]["value"]:
                #
                temp = (
                    divs_1[i].xpath("div/div/div/div/span/div/text()").extract_first()
                )
                if temp:
                    move["rating"]["value"] = temp
            if (not move["num_reviews"]["value"]) and (
                not move["num_ratings"]["value"]
            ):
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[2]/div/div/span[2]/span/span[1] rating
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[2]/div/div/span[2]/span/span[3] review
                temp = (
                    divs_1[i]
                    .xpath("div/div/div/div/span[2]/span/span/text()")
                    .extract()
                )
                if temp and len(temp) == 3:
                    move["num_ratings"]["value"] = (
                        ("".join(temp[0]).strip())
                        .replace("\xa0", "")
                        .replace("\u20b9", "")
                        .replace(",", "")
                    )

                    move["num_reviews"]["value"] = (
                        ("".join(temp[-1]).strip())
                        .replace("\xa0", "")
                        .replace("\u20b9", "")
                        .replace(",", "")
                    )

            if move["assured"]["value"] is None:
                temp = divs_1[i].xpath("div/div[2]/span/img")
                if temp and len(temp) > 0:
                    move["assured"]["value"] = True

            if not move["sale_price"]["value"]:
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[3]/div[1]/div/div[1]
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[3]/div/div[4]/div[1]/div/div[1]
                temp = (
                    divs_1[i].xpath("div/div/div[1]/div/div[1]/text()").extract_first()
                )
                if temp:
                    move["sale_price"]["value"] = (
                        ("".join(temp).strip())
                        .replace("\xa0", "")
                        .replace("\u20b9", "")
                        .replace(",", "")
                    )
            if not move["original_price"]["value"]:
                # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[3]/div/div[4]/div[1]/div/div[2]/text()[2]
                temp = (
                    divs_1[i].xpath("div/div/div[1]/div/div[2]/text()").extract_first()
                )
                # temp = self.response.xpath('//*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[3]/div[1]/div/div[2]/text()').extract()
                if temp:
                    move["original_price"]["value"] = (
                        ("".join(temp).strip())
                        .replace("\xa0", "")
                        .replace("\u20b9", "")
                        .replace(",", "")
                    )
            # if move['availability']['value'] is None:
            #     # //*[@id="container"]/div/div   /div   /div[2]/div[3]/div[1]//text()
            #     # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[3]/div[1]/div/text()
            #     temp = divs_1[i].xpath('div[1]/div/text()').extract_first()
            #     # print(temp)
            #     if temp:
            #         move['availability']['value'] = temp
            # print("%%%%%%%%%")
            # print(move["sale_price"])
            # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[4]/div[1]/div/div[1]
            # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[4]/div[1]/div/div[2]/text()[2]
            if (
                move["original_price"]["value"] == None
                and move["sale_price"]["value"] == None
            ):
                move["availability"]["value"] = "unavailable"
            else:
                move["availability"]["value"] = "available"

        if move["assured"]["value"] is None:
            move["assured"]["value"] = False

        return move


"""
from amazon_product_scraping.utils.FlipkartScrapingHelper import FlipkartAllInfoHelper
from datetime import datetime
a = FlipkartAllInfoHelper(response, datetime.now())
i = a.getInfo()
m = a.getMovement()

"""
# from amazon_product_scraping.utils.FlipkartScrapingHelper import FlipkartAllInfoHelper

# >>> from datetime import datetime

# >>> a = FlipkartAllInfoHelper(response, datetime.now())

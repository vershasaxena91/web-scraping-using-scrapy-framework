import pandas as pd
import json
from tqdm import tqdm


class Converter_details:
    def __init__(self, filename):
        # with open(filename, encoding='utf-8') as f:
        #     self.data = json.loads(f.read())
        self.data = [json.loads(line) for line in open(filename, encoding="utf8")]
        self.prod_details = pd.DataFrame(columns=["ASIN", "Details Tag"])
        self.keys = set()

    def convert_doc(self, i):
        # print(self.data[i]['product_details'])
        prod_keys = set(self.data[i]["product_details"].keys())
        return list(
            map(
                lambda k: {
                    "ASIN": self.data[i]["product_pid"],
                    "Marketplace": self.data[i]["marketplace"],
                    "Details Tag": k,
                    "Present": k in prod_keys,
                },
                self.keys,
            )
        )

    def convert(self):
        for i in tqdm(range(len(self.data))):
            if "product_details" in self.data[i].keys() and isinstance(
                self.data[i]["product_details"], dict
            ):
                self.keys = self.keys.union(
                    set(map(str.strip, self.data[i]["product_details"].keys()))
                )
        done = set()
        for i in tqdm(range(len(self.data))):
            if (
                "product_details" in self.data[i].keys()
                and isinstance(self.data[i]["product_details"], dict)
                and self.data[i]["product_url"] not in done
            ):
                prod_details = self.convert_doc(i)
                done.add(self.data[i]["product_url"])
            # print(prod_details)
            # break
            if prod_details is not None:
                self.prod_details = self.prod_details.append(
                    prod_details, ignore_index=True
                )

    def save(self, details_file):
        self.prod_details.to_csv(details_file, index=False)


if __name__ == "__main__":
    # cvt = Converter_details('export_for_groupm/flipkart_amazon_clubbed/flipkart_product_data.json')
    # cvt.convert()
    # cvt.save('export_for_groupm/flipkart_amazon_clubbed/flipkart_prod_details.csv')

    # cvt = Converter_details('havells/flipkart_marketplace_scraping_havells_07_02_22/product_data.json')
    # cvt.convert()
    # cvt.save('havells/flipkart_prod_details.csv')

    # cvt = Converter_details('pureit/flipkart_marketplace_scraping_pureit_04_02_22/product_data.json')
    # cvt.convert()
    # cvt.save('pureit/flipkart_prod_details.csv')

    # cvt = Converter_details('shell/flipkart_marketplace_scraping_shell_01_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('shell/flipkart_prod_details.csv')

    # cvt = Converter_details('duracell/flipkart_marketplace_scraping_duracell_02_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('duracell/flipkart_prod_details.csv')

    # cvt = Converter_details('havells_fans/flipkart_marketplace_scraping_havells_fans_03_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('havells_fans/flipkart_prod_details.csv')

    # cvt = Converter_details('havells_ac/flipkart_marketplace_scraping_havells_ac_03_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('havells_ac/flipkart_prod_details.csv')

    # cvt = Converter_details('ghadi/flipkart_marketplace_scraping_ghadi_05_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('ghadi/flipkart_prod_details.csv')

    # cvt = Converter_details('kohler/flipkart_marketplace_scraping_kohler_11_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('kohler/flipkart_prod_details.csv')

    # cvt = Converter_details('park_avenue/flipkart_marketplace_scraping_park_avenue_19_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('park_avenue/flipkart_prod_details.csv')

    # cvt = Converter_details('../../data/InputData/lloyd_refrigerator/flipkart_marketplace_scraping_lloyd_refrigerator_11_07_22/product_data.json')
    # cvt.convert()
    # cvt.save('../../data/OutputData/lloyd_refrigerator/flipkart_marketplace_scraping_11_07_22/flipkart_prod_details.csv')

    # cvt = Converter_details('../../data/InputData/lloyd_ac/flipkart_marketplace_scraping_lloyd_ac_11_07_22/product_data.json')
    # cvt.convert()
    # cvt.save('../../data/OutputData/lloyd_ac/flipkart_marketplace_scraping_11_07_22/flipkart_prod_details.csv')

    # cvt = Converter_details('../../data/InputData/lloyd_washing_machine/flipkart_marketplace_scraping_lloyd_washing_machine_11_07_22/product_data.json')
    # cvt.convert()
    # cvt.save('../../data/OutputData/lloyd_washing_machine/flipkart_marketplace_scraping_11_07_22/flipkart_prod_details.csv')

    # cvt = Converter_details(
    #     "../../data/InputData/havells_light/flipkart_marketplace_scraping_havells_light_11_07_22/product_data.json"
    # )
    # cvt.convert()
    # cvt.save(
    #     "../../data/OutputData/havells_light/flipkart_marketplace_scraping_11_07_22/flipkart_prod_details.csv"
    # )

    cvt = Converter_details(
        "../../data/InputData/nycil_powder/flipkart_marketplace_scraping/product_data.json"
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_prod_details.csv"
    )

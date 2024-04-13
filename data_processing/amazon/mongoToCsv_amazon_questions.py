import pandas as pd
import json
from tqdm import tqdm
from datetime import datetime


class Converter_questions:
    def __init__(self, filename):
        # with open(filename, encoding='utf-8') as f:
        #     self.data = json.loads(f.read())
        self.data = [json.loads(line) for line in open(filename, encoding="utf8")]
        self.QAs = pd.DataFrame(columns=["ASIN", "Question", "Answer", "Votes", "Date"])

    def convert_doc(self, i):
        done = set()
        QAs = []
        for qa in self.data[i]["QAs"]:
            if str(qa) in done:
                continue
            done.add(str(qa))
            QAs.append(
                {
                    "ASIN": self.data[i]["product_asin"],
                    "Question": qa["question"],
                    "Answer": qa["answer"],
                    "Votes": qa["votes"],
                    "Date": datetime.strptime(qa["date"], "%Y-%m-%d %H:%M:%S").strftime(
                        "%d-%m-%Y"
                    ),
                }
            )
        return QAs

    def convert(self):
        for i in tqdm(range(len(self.data))):
            QAs = self.convert_doc(i)
            self.QAs = self.QAs.append(QAs, ignore_index=True)

    def save(self, details_file):
        self.QAs.to_csv(details_file, index=False)


if __name__ == "__main__":
    # cvt = Converter_questions('havells/amazon_marketplace_scraping_havells_03_02_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('havells/amazon_QAs.csv')

    # cvt = Converter_questions('sanitary_napkins/amazon_marketplace_scraping_sanitary_napkins_24_01_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('sanitary_napkins/amazon_QAs.csv')

    # cvt = Converter_questions('pureit/amazon_marketplace_scraping_pureit_04_02_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('pureit/amazon_QAs.csv')

    # cvt = Converter_questions('shell/amazon_marketplace_scraping_shell_14_02_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('shell/amazon_QAs.csv')

    # cvt = Converter_questions('duracell/amazon_marketplace_scraping_duracell_16_02_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('duracell/amazon_QAs.csv')

    # cvt = Converter_questions('hafele/amazon_marketplace_scraping_hafele_20_02_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('hafele/amazon_QAs.csv')

    # cvt = Converter_questions('ferrero/amazon_marketplace_scraping_ferrero_28_02_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('ferrero/amazon_QAs.csv')

    # cvt = Converter_questions('kohler/amazon_marketplace_scraping_kohler_02_03_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('kohler/amazon_QAs.csv')

    # cvt = Converter_questions('havells_fans/amazon_marketplace_scraping_havells_fans_04_03_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('havells_fans/amazon_QAs.csv')

    # cvt = Converter_questions('havells_ac/amazon_marketplace_scraping_havells_ac_04_03_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('havells_ac/amazon_QAs.csv')

    # cvt = Converter_questions('ghadi/amazon_marketplace_scraping_ghadi_05_03_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('ghadi/amazon_QAs.csv')

    # cvt = Converter_questions('nestle/amazon_marketplace_scraping_nestle_06_03_22/product_QAs_nescafe.json')
    # cvt.convert()
    # cvt.save('nestle/amazon_QAs_nescafe.csv')

    # cvt = Converter_questions('park_avenue/amazon_marketplace_scraping_park_avenue_19_03_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('park_avenue/amazon_QAs.csv')

    # cvt = Converter_questions('bajaj_mixer/amazon_marketplace_scraping_bajaj_mixer_25_03_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('bajaj_mixer/amazon_QAs.csv')

    # cvt = Converter_questions('blue_heaven/amazon_marketplace_scraping_blue_heaven_05_04_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('blue_heaven/amazon_QAs.csv')

    # cvt = Converter_questions('nature_essence/amazon_marketplace_scraping_nature_essence_05_04_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('nature_essence/amazon_QAs.csv')

    # cvt = Converter_questions('eno/amazon_marketplace_scraping_eno_12_04_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('eno/amazon_QAs.csv')

    # cvt = Converter_questions('nescafe_coffee_maker/amazon_marketplace_scraping_nescafe_coffee_maker_12_04_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('nescafe_coffee_maker/amazon_QAs.csv')

    # cvt = Converter_questions('nescafe_gold/amazon_marketplace_scraping_nescafe_gold_12_04_22/product_QAs.json')
    # cvt.convert()
    # cvt.save('nescafe_gold/amazon_QAs.csv')

    cvt = Converter_questions(
        "../../data/InputData/manyavar/amazon_marketplace_scraping/product_QAs.json"
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/manyavar/amazon_marketplace_scraping/amazon_QAs.csv"
    )

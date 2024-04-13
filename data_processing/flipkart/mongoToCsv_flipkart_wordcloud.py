import pandas as pd
import json
from tqdm import tqdm
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer


class Converter_word_cloud:
    def __init__(self, filename):
        self.STOPWORDS = set(stopwords.words("english"))
        self.lemmatizer = WordNetLemmatizer()
        # with open(filename, encoding='utf-8') as f:
        #     self.data = json.loads(f.read())
        self.data = [json.loads(line) for line in open(filename, encoding="utf8")]
        self.word_freqs = pd.DataFrame(
            columns=["ASIN", "String", "Rating", "Frequency"]
        )
        self.keys = set()

    def convert_doc(self, i, max_len):
        word_count = dict()
        for comm in self.data[i]["comments"]:
            wordlist = list(map(str.lower, comm["title"].split()))
            wordlist = [
                self.lemmatizer.lemmatize(w)
                for w in wordlist
                if not w in self.STOPWORDS
            ]
            for st in range(len(wordlist)):
                for length in range(max_len):
                    string = " ".join(wordlist[st : st + length + 1])
                    word_count[comm["rating"]] = word_count.get(comm["rating"], {})
                    word_count[comm["rating"]][string] = (
                        word_count[comm["rating"]].get(string, 0) + 1
                    )

            wordlist = list(map(str.lower, comm["description"].split()))
            wordlist = [
                self.lemmatizer.lemmatize(w)
                for w in wordlist
                if not w in self.STOPWORDS
            ]
            for st in range(len(wordlist)):
                for length in range(max_len):
                    string = " ".join(wordlist[st : st + length + 1])
                    word_count[comm["rating"]] = word_count.get(comm["rating"], {})
                    word_count[comm["rating"]][string] = (
                        word_count[comm["rating"]].get(string, 0) + 1
                    )

        word_freq = []
        for r, wc in word_count.items():
            for w, c in wc.items():
                word_freq.append(
                    {
                        "ASIN": self.data[i]["product_asin"],
                        "Marketplace": self.data[i]["marketplace"],
                        "String": w,
                        "Frequency": c,
                        "Rating": r,
                    }
                )
        return word_freq

    def convert(self):
        for i in tqdm(range(len(self.data))):
            word_freq = self.convert_doc(i, 2)
            if word_freq != []:
                self.word_freqs = self.word_freqs.append(word_freq, ignore_index=True)

    def save(self, details_file):
        self.word_freqs.to_csv(details_file, index=False)


if __name__ == "__main__":
    # cvt = Converter_word_cloud('export_for_groupm/flipkart/dash7/product_comments.json')
    # cvt.convert()
    # cvt.save('export_for_groupm/flipkart/dash7/word_freq.csv')

    # cvt = Converter_word_cloud('havells/flipkart_marketplace_scraping_havells_28_01_22/product_comments.json')
    # cvt.convert()
    # cvt.save('havells/flipkart_word_freq.csv')

    # cvt = Converter_word_cloud('pureit/flipkart_marketplace_scraping_pureit_04_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('pureit/flipkart_word_freq.csv')

    # cvt = Converter_word_cloud('shell/flipkart_marketplace_scraping_shell_14_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('shell/flipkart_word_freq.csv')

    # cvt = Converter_word_cloud('duracell/flipkart_marketplace_scraping_duracell_16_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('duracell/flipkart_word_freq.csv')

    # cvt = Converter_word_cloud('havells_fans/flipkart_marketplace_scraping_havells_fans_03_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('havells_fans/flipkart_word_freq.csv')

    # cvt = Converter_word_cloud('havells_ac/flipkart_marketplace_scraping_havells_ac_04_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('havells_ac/flipkart_word_freq.csv')

    # cvt = Converter_word_cloud('ghadi/flipkart_marketplace_scraping_ghadi_05_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('ghadi/flipkart_word_freq.csv')

    # cvt = Converter_word_cloud('kohler/flipkart_marketplace_scraping_kohler_11_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('kohler/flipkart_word_freq.csv')

    # cvt = Converter_word_cloud('park_avenue/flipkart_marketplace_scraping_park_avenue_19_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('park_avenue/flipkart_word_freq.csv')

    # cvt = Converter_word_cloud('../../data/InputData/lloyd_refrigerator/flipkart_marketplace_scraping_lloyd_refrigerator_11_07_22/product_comments.json')
    # cvt.convert()
    # cvt.save('../../data/OutputData/lloyd_refrigerator/flipkart_marketplace_scraping_11_07_22/flipkart_word_freq.csv')

    # cvt = Converter_word_cloud('../../data/InputData/lloyd_ac/flipkart_marketplace_scraping_lloyd_ac_11_07_22/product_comments.json')
    # cvt.convert()
    # cvt.save('../../data/OutputData/lloyd_ac/flipkart_marketplace_scraping_11_07_22/flipkart_word_freq.csv')

    # cvt = Converter_word_cloud('../../data/InputData/lloyd_washing_machine/flipkart_marketplace_scraping_lloyd_washing_machine_11_07_22/product_comments.json')
    # cvt.convert()
    # cvt.save('../../data/OutputData/lloyd_washing_machine/flipkart_marketplace_scraping_11_07_22/flipkart_word_freq.csv')

    # cvt = Converter_word_cloud(
    #     "../../data/InputData/havells_light/flipkart_marketplace_scraping_havells_light_11_07_22/product_comments.json"
    # )
    # cvt.convert()
    # cvt.save(
    #     "../../data/OutputData/havells_light/flipkart_marketplace_scraping_11_07_22/flipkart_word_freq.csv"
    # )

    cvt = Converter_word_cloud(
        "../../data/InputData/nycil_powder/flipkart_marketplace_scraping/product_comments.json"
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_word_freq.csv"
    )

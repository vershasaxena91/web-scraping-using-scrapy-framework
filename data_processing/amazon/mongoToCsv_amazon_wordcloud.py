import pandas as pd
import json
from tqdm import tqdm
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

# import nltk
# nltk.download('omw-1.4')


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
        # self.keys = set()

    def convert_doc(self, i, max_len):
        word_count = dict()
        done = set()
        for comm in self.data[i]["comments"]:
            if str(comm) in done:
                continue
            done.add(str(comm))
            if comm["title"]:
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

            if comm["description"]:
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
                        "String": w,
                        "Frequency": c,
                        "Rating": float(r.split(" out ")[0]) if r else None,
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
    # cvt = Converter_word_cloud('havells/amazon_marketplace_scraping_havells_18_01_22/product_comments.json')
    # cvt.convert()
    # cvt.save('havells/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('sanitary_napkins/amazon_marketplace_scraping_sanitary_napkins_24_01_22/product_comments.json')
    # cvt.convert()
    # cvt.save('sanitary_napkins/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('pureit/amazon_marketplace_scraping_pureit_04_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('pureit/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('shell/amazon_marketplace_scraping_shell_14_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('shell/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('duracell/amazon_marketplace_scraping_duracell_16_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('duracell/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('hafele/amazon_marketplace_scraping_hafele_21_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('hafele/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('ferrero/amazon_marketplace_scraping_ferrero_28_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('ferrero/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('kohler/amazon_marketplace_scraping_kohler_02_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('kohler/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('havells_fans/amazon_marketplace_scraping_havells_fans_03_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('havells_fans/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('havells_ac/amazon_marketplace_scraping_havells_ac_04_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('havells_ac/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('ghadi/amazon_marketplace_scraping_ghadi_05_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('ghadi/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('nestle/amazon_marketplace_scraping_nestle_06_03_22/product_comments_cerelac.json')
    # cvt.convert()
    # cvt.save('nestle/amazon_word_freq_cerelac.csv')

    # cvt = Converter_word_cloud('park_avenue/amazon_marketplace_scraping_park_avenue_19_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('park_avenue/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('bajaj_mixer/amazon_marketplace_scraping_bajaj_mixer_25_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('bajaj_mixer/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('blue_heaven/amazon_marketplace_scraping_blue_heaven_05_04_22/product_comments.json')
    # cvt.convert()
    # cvt.save('blue_heaven/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('nature_essence/amazon_marketplace_scraping_nature_essence_05_04_22/product_comments.json')
    # cvt.convert()
    # cvt.save('nature_essence/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('eno/amazon_marketplace_scraping_eno_12_04_22/product_comments.json')
    # cvt.convert()
    # cvt.save('eno/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('nescafe_coffee_maker/amazon_marketplace_scraping_nescafe_coffee_maker_12_04_22/product_comments.json')
    # cvt.convert()
    # cvt.save('nescafe_coffee_maker/amazon_word_freq.csv')

    # cvt = Converter_word_cloud('nescafe_gold/amazon_marketplace_scraping_nescafe_gold_12_04_22/product_comments.json')
    # cvt.convert()
    # cvt.save('nescafe_gold/amazon_word_freq.csv')

    cvt = Converter_word_cloud(
        "../../data/InputData/manyavar/amazon_marketplace_scraping/product_comments.json"
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/manyavar/amazon_marketplace_scraping/amazon_word_freq.csv"
    )

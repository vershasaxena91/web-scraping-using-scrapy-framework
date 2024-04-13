import pandas as pd
import json
from tqdm import tqdm
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer


class Converter_word_cloud:
    def __init__(self, filename):
        self.STOPWORDS = set(stopwords.words("english"))
        self.lemmatizer = WordNetLemmatizer()

        self.data = [json.loads(line) for line in open(filename, encoding="utf8")]
        self.word_freqs = pd.DataFrame(
            columns=["ASIN", "String", "Rating", "Frequency"]
        )

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
                        "ASIN": self.data[i]["product_id"],
                        "String": w,
                        "Frequency": c,
                        "Rating": float(r) if r else None,
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

    cvt = Converter_word_cloud(
        "../../data/InputData/loreal_shampoo/nykaa_marketplace_scraping/product_comments.json"
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/loreal_shampoo/nykaa_marketplace_scraping/nykaa_word_freq.csv"
    )

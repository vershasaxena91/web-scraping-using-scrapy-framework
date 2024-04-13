import pandas as pd
import json
from tqdm import tqdm
from transformers import pipeline


class Converter_senti_score:
    def __init__(self, filename):

        self.data = [json.loads(line) for line in open(filename, encoding="utf8")]
        self.sentiment_score = pd.DataFrame(columns=["ASIN", "Sentiment Score"])
        self.classifier = pipeline(
            "sentiment-analysis",
            model="nlptown/bert-base-multilingual-uncased-sentiment",
        )
        self.sentiment_types = {
            "1 star": 1,
            "2 stars": 2,
            "3 stars": 3,
            "4 stars": 4,
            "5 stars": 5,
        }

    def get_title_des(self, i):
        comms_list = []
        done = set()
        for comm in self.data[i]["comments"]:
            if str(comm) in done:
                continue
            done.add(str(comm))
            if comm["description"]:
                if comm["title"]:
                    comm["title"] = comm["title"].replace("\\n", "")
                    comm["title"] = comm["title"].replace("\n", "")
                    comm["title"] = comm["title"].strip()
                    comm["title"] = comm["title"].lower()

                comm["description"] = comm["description"].replace("\\n", "")
                comm["description"] = comm["description"].replace("\n", "")
                comm["description"] = comm["description"].strip()
                comm["description"] = comm["description"].lower()
                if len(comm["description"].split()) >= 5:
                    comms_list.append(
                        {
                            "ASIN": self.data[i]["product_id"],
                            "title": comm["title"],
                            "description": comm["description"],
                        }
                    )

        return comms_list

    def convert(self):
        comms_all = pd.DataFrame(columns=["ASIN", "title", "description"])
        for i in tqdm(range(len(self.data))):
            comms_i = self.get_title_des(i)
            if comms_i != []:
                comms_all = comms_all.append(comms_i, ignore_index=True)

        sentiments = self.classifier(comms_all["description"].tolist())
        y_preds = []
        for s in sentiments:
            y_preds.append(s["label"])

        print(len(y_preds), len(comms_all))
        comms_all["Sentiment"] = y_preds

        sentiment_score_dict = dict()
        for i in tqdm(range(len(comms_all))):
            sentiment_score_dict[comms_all.loc[i, "ASIN"]] = sentiment_score_dict.get(
                comms_all.loc[i, "ASIN"], {"count": 0, "sum": 0}
            )
            sentiment_score_dict[comms_all.loc[i, "ASIN"]]["count"] += 1
            sentiment_score_dict[comms_all.loc[i, "ASIN"]][
                "sum"
            ] += self.sentiment_types[comms_all.loc[i, "Sentiment"]]

        senti_list = []
        for asin, score in sentiment_score_dict.items():
            senti_list.append(
                {
                    "ASIN": asin,
                    "Sentiment Score": score["sum"] / score["count"]
                    if score["count"] > 0
                    else 0,
                }
            )
        self.sentiment_score = self.sentiment_score.append(
            senti_list, ignore_index=True
        )

    def save(self, senti_score_file):
        self.sentiment_score.to_csv(senti_score_file, index=False)

    def check(self, senti_score_file_in, data_file, senti_score_file_out):
        senti_scores = pd.read_csv(senti_score_file_in)
        data = pd.read_csv(data_file)
        asins = data["ASIN"].tolist()
        print(asins)
        filtered_senti_scores = senti_scores[senti_scores["ASIN"].isin(asins)]
        filtered_senti_scores.to_csv(senti_score_file_out, index=False)


if __name__ == "__main__":

    cvt = Converter_senti_score(
        "../../data/InputData/loreal_shampoo/nykaa_marketplace_scraping/product_comments.json"
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/loreal_shampoo/nykaa_marketplace_scraping/nykaa_sentiment_scores.csv"
    )

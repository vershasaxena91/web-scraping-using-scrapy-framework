"""# 
import json
import pandas as pd
from tqdm import tqdm

with open('park_avenue/flipkart_marketplace_scraping_park_avenue_19_03_22/product_comments.json', encoding='utf-8') as f:
    prods = json.load(f)

print(len(prods))
# with open('park_avenue/flipkart_marketplace_scraping_park_avenue_16_02_22/product_data.json', encoding='utf-8') as f:
#     data = json.load(f)
# needed = set()
# for p in data:
#     if p['product_brand'] == "DURACELL Batteries":
#         needed.add(p['product_pid'])
# print(needed)
# print(len(needed))


comms_df = pd.DataFrame(columns=['ASIN', 'Marketplace', 'username', 'rating', 'title', 'date', 'likes', 'dislikes', 'verified', 'description'])
counts = dict()
for prod in tqdm(prods):
    comms_list = []
    done = set()
    # if prod['product_asin'] not in needed:
    #     continue
    for comm in prod['comments']:
        if str(comm) in done:
            continue
        done.add(str(comm))
        # print(comm['description'])
        if comm['description']:
            comm['description'] = comm['description'].replace('\\n', '')
            comm['description'] = comm['description'].replace('\n', '')
            if len(comm['description'].split()) >= 5:
                comms_list.append({
                    'ASIN': prod['product_asin'],
                    'Marketplace': prod['marketplace'],
                    'username': comm['username'], 
                    'rating': comm['rating'], 
                    'title': comm['title'],
                    'date': comm['date'],  
                    'likes': comm['likes'],
                    'dislikes': comm['dislikes'],
                    'verified': comm['verified'],
                    'description': comm['description']
                })
    if len(comms_list) > 0:
        comms_df = comms_df.append(comms_list)
        # print(comms_df.columns)
    else:
        # print(prod)
        pass
counts = comms_df['ASIN'].value_counts().to_dict()
print(counts)
print(comms_df.shape)
comms_df = comms_df.drop_duplicates()
comms_df['Counts'] = comms_df['ASIN'].apply(lambda x: counts[x])
comms_df = comms_df.sort_values(['Counts', 'ASIN'], ascending=False)
# print(comms_df)
comms_df.to_csv('park_avenue/flipkart_comments_for_ABSA.csv', index=False)

"""
import pandas as pd
import json
from tqdm import tqdm
from transformers import pipeline


class Converter_senti_score:
    def __init__(self, filename):
        # with open(filename, encoding='utf-8') as f:
        #     self.data = json.loads(f.read())
        self.data = [json.loads(line) for line in open(filename, encoding="utf8")]
        self.sentiment_score = pd.DataFrame(
            columns=["ASIN", "Marketplace", "Sentiment Score"]
        )
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

    # def convert_doc(self, i):
    #     # comms_df = pd.DataFrame(columns=['username', 'rating', 'title', 'country', 'date', 'description', 'helpful'])
    #     comms_list = []
    #     for comm in self.data[i]['comments']:
    #         # print(comm['description'])
    #         if comm['description']:
    #             comm['title'] = comm['title'].replace('\\n', '')
    #             comm['title'] = comm['title'].replace('\n', '')
    #             comm['title'] = comm['title'].strip()
    #             comm['title'] = comm['title'].lower()

    #             comm['description'] = comm['description'].replace('\\n', '')
    #             comm['description'] = comm['description'].replace('\n', '')
    #             comm['description'] = comm['description'].strip()
    #             comm['description'] = comm['description'].lower()
    #             if len(comm['description'].split()) >= 5:
    #                 comms_list.append(comm['title'])
    #                 comms_list.append(comm['description'])
    #             # if len(comm['description'].split()) >= 5:
    #             #     comms_list.append({
    #             #         'ASIN': self.data[i]['product_pid'],
    #             #         'Marketplace': self.data[i]['marketplace'],
    #             #         # 'username': comm['username'],
    #             #         # 'rating': comm['rating'],
    #             #         'title': comm['title'],
    #             #         # 'date': comm['date'],
    #             #         # 'likes': comm['likes'],
    #             #         # 'dislikes': comm['dislikes'],
    #             #         # 'verified': comm['verified'],
    #             #         'description': comm['description']
    #             #     })
    #     # comms_df = comms_df.append(comms_list)
    #     # comms_df['description'] = comms_df['description'].apply(lambda x: x.replace('\n', ' '))
    #     # comms_df['description'] = comms_df['description'].apply(lambda x: x.strip())
    #     # comms_df['description'] = comms_df['description'].apply(lambda x: x.lower())
    #     sentiments = self.classifier(comms_list)
    #     y_preds = []
    #     for r in sentiments:
    #         y_preds.append(r['label'])

    #     print(len(y_preds), len(comms_list), len(y_preds) == len(comms_list))
    #     print(y_preds)
    #     # comms_df['Sentiment'] = y_preds
    #     assert len(y_preds) == len(comms_list), "ERROR"
    #     sentiment_score = 0
    #     if len(y_preds) > 0:
    #         for y_pred in y_preds:
    #             sentiment_score += self.sentiment_types[y_pred]
    #         sentiment_score /= len(y_preds)
    #     else:
    #         return {}

    #     return {
    #         'ASIN': self.data[i]['product_pid'],
    #         'Marketplace': self.data[i]['marketplace'],
    #         'Sentiment Score': sentiment_score
    #     }

    def get_title_des(self, i):
        comms_list = []
        for comm in self.data[i]["comments"]:
            # print(comm['description'])
            if comm["description"]:
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
                            "ASIN": self.data[i]["product_asin"],
                            "Marketplace": self.data[i]["marketplace"],
                            "title": comm["title"],
                            "description": comm["description"],
                        }
                    )

        return comms_list

    def convert(self):
        comms_all = pd.DataFrame(
            columns=["ASIN", "Marketplace", "title", "description"]
        )
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
            sentiment_score_dict[
                comms_all.loc[i, "Marketplace"]
            ] = sentiment_score_dict.get(comms_all.loc[i, "Marketplace"], {})
            sentiment_score_dict[comms_all.loc[i, "Marketplace"]][
                comms_all.loc[i, "ASIN"]
            ] = sentiment_score_dict[comms_all.loc[i, "Marketplace"]].get(
                comms_all.loc[i, "ASIN"], {"count": 0, "sum": 0}
            )
            sentiment_score_dict[comms_all.loc[i, "Marketplace"]][
                comms_all.loc[i, "ASIN"]
            ]["count"] += 1
            sentiment_score_dict[comms_all.loc[i, "Marketplace"]][
                comms_all.loc[i, "ASIN"]
            ]["sum"] += self.sentiment_types[comms_all.loc[i, "Sentiment"]]

        senti_list = []
        for m, sc in sentiment_score_dict.items():
            for asin, score in sc.items():
                senti_list.append(
                    {
                        "ASIN": asin,
                        "Marketplace": m,
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
        filtered_senti_scores = senti_scores[senti_scores["ASIN"].isin(asins)]
        filtered_senti_scores.to_csv(senti_score_file_out, index=False)


if __name__ == "__main__":
    # cvt = Converter_senti_score('export_for_groupm/flipkart_amazon_clubbed/flipkart_product_comments.json')
    # cvt.convert()
    # cvt.save('export_for_groupm/flipkart_amazon_clubbed/flipkart_sentiment_scores.csv')

    # cvt = Converter_senti_score('havells/flipkart_marketplace_scraping_havells_28_01_22/product_comments.json')
    # cvt.convert()
    # cvt.save('havells/flipkart_sentiment_scores.csv')

    # cvt = Converter_senti_score('pureit/flipkart_marketplace_scraping_pureit_04_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('pureit/flipkart_sentiment_scores.csv')

    # cvt = Converter_senti_score('shell/flipkart_marketplace_scraping_shell_14_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('shell/flipkart_sentiment_scores.csv')

    # cvt = Converter_senti_score('duracell/flipkart_marketplace_scraping_duracell_16_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('duracell/flipkart_sentiment_scores.csv')

    # cvt = Converter_senti_score('havells_fans/flipkart_marketplace_scraping_havells_fans_03_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('havells_fans/flipkart_sentiment_scores.csv')
    # cvt.check('havells_fans/flipkart_sentiment_scores_old.csv', 'havells_fans/flipkart_prod_info.csv', 'havells_fans/flipkart_sentiment_scores.csv')

    # cvt = Converter_senti_score('havells_ac/flipkart_marketplace_scraping_havells_ac_04_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('havells_ac/flipkart_sentiment_scores.csv')
    # cvt.check('havells_ac/flipkart_sentiment_scores_old.csv', 'havells_ac/flipkart_prod_info.csv', 'havells_ac/flipkart_sentiment_scores.csv')

    # cvt = Converter_senti_score('ghadi/flipkart_marketplace_scraping_ghadi_05_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('ghadi/flipkart_sentiment_scores.csv')
    # cvt.check('ghadi/flipkart_sentiment_scores_old.csv', 'ghadi/flipkart_prod_info.csv', 'ghadi/flipkart_sentiment_scores.csv')

    # cvt = Converter_senti_score('kohler/flipkart_marketplace_scraping_kohler_11_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('kohler/flipkart_sentiment_scores.csv')
    # cvt.check('kohler/flipkart_sentiment_scores_old.csv', 'kohler/flipkart_prod_info.csv', 'kohler/flipkart_sentiment_scores.csv')

    # cvt = Converter_senti_score('park_avenue/flipkart_marketplace_scraping_park_avenue_19_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('park_avenue/flipkart_sentiment_scores.csv')
    # cvt.check('park_avenue/flipkart_sentiment_scores.csv', 'park_avenue/flipkart_prod_info.csv', 'park_avenue/flipkart_sentiment_scores.csv')

    # cvt = Converter_senti_score('../../data/InputData/lloyd_refrigerator/flipkart_marketplace_scraping_lloyd_refrigerator_11_07_22/product_comments.json')
    # cvt.convert()
    # cvt.save('../../data/OutputData/lloyd_refrigerator/flipkart_marketplace_scraping_11_07_22/flipkart_sentiment_scores.csv')
    # cvt.check('../../data/OutputData/lloyd_refrigerator/flipkart_marketplace_scraping_11_07_22/flipkart_sentiment_scores.csv', '../../data/OutputData/lloyd_refrigerator/flipkart_marketplace_scraping_11_07_22/flipkart_prod_info.csv', '../../data/OutputData/lloyd_refrigerator/flipkart_marketplace_scraping_11_07_22/flipkart_sentiment_scores.csv')

    # cvt = Converter_senti_score('../../data/InputData/lloyd_ac/flipkart_marketplace_scraping_lloyd_ac_11_07_22/product_comments.json')
    # cvt.convert()
    # cvt.save('../../data/OutputData/lloyd_ac/flipkart_marketplace_scraping_11_07_22/flipkart_sentiment_scores.csv')
    # cvt.check('../../data/OutputData/lloyd_ac/flipkart_marketplace_scraping_11_07_22/flipkart_sentiment_scores.csv', '../../data/OutputData/lloyd_ac/flipkart_marketplace_scraping_11_07_22/flipkart_prod_info.csv', '../../data/OutputData/lloyd_ac/flipkart_marketplace_scraping_11_07_22/flipkart_sentiment_scores.csv')

    # cvt = Converter_senti_score('../../data/InputData/lloyd_washing_machine/flipkart_marketplace_scraping_lloyd_washing_machine_11_07_22/product_comments.json')
    # cvt.convert()
    # cvt.save('../../data/OutputData/lloyd_washing_machine/flipkart_marketplace_scraping_11_07_22/flipkart_sentiment_scores.csv')
    # cvt.check('../../data/OutputData/lloyd_washing_machine/flipkart_marketplace_scraping_11_07_22/flipkart_sentiment_scores.csv', '../../data/OutputData/lloyd_washing_machine/flipkart_marketplace_scraping_11_07_22/flipkart_prod_info.csv', '../../data/OutputData/lloyd_washing_machine/flipkart_marketplace_scraping_11_07_22/flipkart_sentiment_scores.csv')

    # cvt = Converter_senti_score(
    #     "../../data/InputData/havells_light/flipkart_marketplace_scraping_havells_light_11_07_22/product_comments.json"
    # )
    # cvt.convert()
    # cvt.save(
    #     "../../data/OutputData/havells_light/flipkart_marketplace_scraping_11_07_22/flipkart_sentiment_scores.csv"
    # )
    # cvt.check(
    #     "../../data/OutputData/havells_light/flipkart_marketplace_scraping_11_07_22/flipkart_sentiment_scores.csv",
    #     "../../data/OutputData/havells_light/flipkart_marketplace_scraping_11_07_22/flipkart_prod_info.csv",
    #     "../../data/OutputData/havells_light/flipkart_marketplace_scraping_11_07_22/flipkart_sentiment_scores.csv",
    # )

    cvt = Converter_senti_score(
        "../../data/InputData/nycil_powder/flipkart_marketplace_scraping/product_comments.json"
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_sentiment_scores.csv"
    )
    cvt.check(
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_sentiment_scores.csv",
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_prod_info.csv",
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_sentiment_scores.csv",
    )

"""# 
import json
import pandas as pd
from tqdm import tqdm

with open('ceregrow/amazon_marketplace_scraping_ceregrow_13_04_22/product_comments.json', encoding='utf-8') as f:
    prods = json.load(f)

print(len(prods))
# with open('ceregrow/amazon_marketplace_scraping_ceregrow_02_03_22/product_data.json', encoding='utf-8') as f:
#     data = json.load(f)
# needed = set()
# for p in data:
#     if p['product_brand'].lower() == "ceregrow" or p['product_name'].split()[0].lower() == "ceregrow":
#         needed.add(p['product_asin'])
# print(needed)
# print(len(needed))


comms_df = pd.DataFrame(columns=['ASIN', 'username', 'rating', 'title', 'date', 'country', 'helpful', 'description'])
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
                    'username': comm['username'], 
                    'rating': comm['rating'], 
                    'title': comm['title'],
                    'date': comm['date'],
                    'country': comm['country'],
                    'helpful': comm['helpful'],
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
comms_df.to_csv('ceregrow/amazon_comments_for_ABSA.csv', index=False)

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
    #             #         'ASIN': self.data[i]['product_asin'],
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
    #         'ASIN': self.data[i]['product_asin'],
    #         'Sentiment Score': sentiment_score
    #     }

    def get_title_des(self, i):
        comms_list = []
        done = set()
        for comm in self.data[i]["comments"]:
            if str(comm) in done:
                continue
            done.add(str(comm))
            # print(comm['description'])
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
                            "ASIN": self.data[i]["product_asin"],
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
                # comms_all = pd.concat([comms_all, comms_i])
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
    # cvt = Converter_senti_score('havells/amazon_marketplace_scraping_havells_18_01_22/product_comments.json')
    # cvt.convert()
    # cvt.save('havells/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('sanitary_napkins/amazon_marketplace_scraping_sanitary_napkins_24_01_22/product_comments.json')
    # cvt.convert()
    # cvt.save('sanitary_napkins/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('pureit/amazon_marketplace_scraping_pureit_04_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('pureit/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('shell/amazon_marketplace_scraping_shell_14_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('shell/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('duracell/amazon_marketplace_scraping_duracell_16_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('duracell/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('hafele/amazon_marketplace_scraping_hafele_20_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('hafele/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('ferrero/amazon_marketplace_scraping_ferrero_28_02_22/product_comments.json')
    # cvt.convert()
    # cvt.save('ferrero/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('kohler/amazon_marketplace_scraping_kohler_02_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('kohler/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('havells_fans/amazon_marketplace_scraping_havells_fans_03_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('havells_fans/amazon_sentiment_scores.csv')
    # cvt.check('havells_fans/amazon_sentiment_scores_old.csv', 'havells_fans/amazon_prod_info.csv', 'havells_fans/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('havells_ac/amazon_marketplace_scraping_havells_ac_04_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('havells_ac/amazon_sentiment_scores.csv')
    # cvt.check('havells_ac/amazon_sentiment_scores_old.csv', 'havells_ac/amazon_prod_info.csv', 'havells_ac/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('ghadi/amazon_marketplace_scraping_ghadi_05_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('ghadi/amazon_sentiment_scores.csv')
    # cvt.check('ghadi/amazon_sentiment_scores_old.csv', 'ghadi/amazon_prod_info.csv', 'ghadi/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('nestle/amazon_marketplace_scraping_nestle_06_03_22/product_comments_nescafe.json')
    # cvt.convert()
    # cvt.save('nestle/amazon_sentiment_scores_nescafe.csv')
    # cvt.check('nestle/amazon_sentiment_scores.csv', 'nestle/amazon_prod_info.csv', 'nestle/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('park_avenue/amazon_marketplace_scraping_park_avenue_19_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('park_avenue/amazon_sentiment_scores.csv')
    # cvt.check('park_avenue/amazon_sentiment_scores.csv', 'park_avenue/amazon_prod_info.csv', 'park_avenue/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('bajaj_mixer/amazon_marketplace_scraping_bajaj_mixer_25_03_22/product_comments.json')
    # cvt.convert()
    # cvt.save('bajaj_mixer/amazon_sentiment_scores.csv')
    # cvt.check('bajaj_mixer/amazon_sentiment_scores.csv', 'bajaj_mixer/amazon_prod_info.csv', 'bajaj_mixer/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('nature_essence/amazon_marketplace_scraping_nature_essence_05_04_22/product_comments.json')
    # cvt.convert()
    # cvt.save('nature_essence/amazon_sentiment_scores.csv')
    # cvt.check('nature_essence/amazon_sentiment_scores.csv', 'nature_essence/amazon_prod_info.csv', 'nature_essence/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('blue_heaven/amazon_marketplace_scraping_blue_heaven_05_04_22/product_comments.json')
    # cvt.convert()
    # cvt.save('blue_heaven/amazon_sentiment_scores.csv')
    # cvt.check('blue_heaven/amazon_sentiment_scores.csv', 'blue_heaven/amazon_prod_info.csv', 'blue_heaven/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('eno/amazon_marketplace_scraping_eno_12_04_22/product_comments.json')
    # cvt.convert()
    # cvt.save('eno/amazon_sentiment_scores.csv')
    # # cvt.check('eno/amazon_sentiment_scores.csv', 'eno/amazon_prod_info.csv', 'eno/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('nescafe_coffee_maker/amazon_marketplace_scraping_nescafe_coffee_maker_12_04_22/product_comments.json')
    # cvt.convert()
    # cvt.save('nescafe_coffee_maker/amazon_sentiment_scores.csv')
    # # cvt.check('nescafe_coffee_maker/amazon_sentiment_scores.csv', 'nescafe_coffee_maker/amazon_prod_info.csv', 'nescafe_coffee_maker/amazon_sentiment_scores.csv')

    # cvt = Converter_senti_score('nescafe_gold/amazon_marketplace_scraping_nescafe_gold_12_04_22/product_comments.json')
    # cvt.convert()
    # cvt.save('nescafe_gold/amazon_sentiment_scores.csv')
    # # cvt.check('nescafe_gold/amazon_sentiment_scores.csv', 'nescafe_gold/amazon_prod_info.csv', 'nescafe_gold/amazon_sentiment_scores.csv')

    cvt = Converter_senti_score(
        "../../data/InputData/manyavar/amazon_marketplace_scraping/product_comments.json"
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/manyavar/amazon_marketplace_scraping/amazon_sentiment_scores.csv"
    )
    # cvt.check('ceregrow/amazon_sentiment_scores.csv', 'ceregrow/amazon_prod_info.csv', 'ceregrow/amazon_sentiment_scores.csv')

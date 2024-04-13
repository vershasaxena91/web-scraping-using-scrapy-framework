from matplotlib.pyplot import title
import pandas as pd
import json
from tqdm import tqdm
from word2number import w2n


def is_float(w):
    try:
        w = float(w)
        return True
    except:
        return False


class Converter_details:
    def __init__(self, filename):
        # with open(filename, encoding='utf-8') as f:
        #     self.data = json.loads(f.read())
        self.data = [json.loads(line) for line in open(filename, encoding="utf8")]
        self.prod_details = pd.DataFrame(columns=["ASIN", "Details Tag"])
        self.prod_scores = pd.DataFrame(columns=["ASIN", "Title Score"])
        # self.prod_scores = []
        # self.details_suggestions = []
        self.details_add = pd.DataFrame(columns=["ASIN", "Add Details", "Percentage"])
        self.details_remove = pd.DataFrame(
            columns=["ASIN", "Remove Details", "Percentage"]
        )
        self.bullets_add = pd.DataFrame(columns=["ASIN", "Add Bullets", "Percentage"])
        self.bullets_remove = pd.DataFrame(
            columns=["ASIN", "Remove Bullets", "Percentage"]
        )
        self.avg_images = 0
        self.max_images = 0
        self.keys = set()
        self.bullets = set()
        self.keys_present_count = dict()
        self.bullets_present_count = dict()
        for i in tqdm(range(len(self.data))):
            self.avg_images += len(self.data[i]["product_image_urls"])
            self.max_images = max(
                self.max_images, len(self.data[i]["product_image_urls"])
            )
            self.keys = self.keys.union(
                set(
                    map(
                        lambda x: x.strip().lower().title(),
                        self.data[i]["product_details"].keys(),
                    )
                )
            )
            for k in self.data[i]["product_details"].keys():
                k = k.strip().lower().title()
                self.keys_present_count[k] = self.keys_present_count.get(k, 0) + 1

            # TODO self.bullets = self.bullets.union(set(map(lambda x: x.strip().lower().title(), self.data[i]['product_bullets'].keys())))
            # for k in self.data[i]['product_bullets'].keys():
            #     self.bullets_present_count[k] = self.bullets_present_count.get(k, 0) + 1
        self.avg_images /= len(self.data)
        for k in self.keys_present_count.keys():
            self.keys_present_count[k] /= len(self.data)

        # TODO for k in self.bullets_present_count.keys():
        #     self.bullets_present_count[k] /= len(self.data)

    def title_score(self, i: int):
        title = self.data[i]["product_name"]
        score = 100
        # suggestion = {
        #     "long_text": False,
        #     "all_caps/small": False,
        #     "not_camel_case": False,
        #     "not_ascii": False,
        #     "decoration": False
        # }
        suggestion = []
        if not isinstance(title, str):
            return None
        # if greater than 80
        if len(title) > 80:
            suggestion.append(
                "The Title is long. Reduce it to <80 chars."
            )  # ['long_text'] = True
            score -= 30
        # if all upper or all lower
        if title.isupper():
            suggestion.append(
                "The Title formating is wrong. The Title should not be ALL CAPS"
            )  # ['all_caps/small'] = True
            score -= 15
        elif title.islower():
            suggestion.append(
                "The Title formating is wrong. The Title should be all lowercase."
            )  # ['all_caps/small'] = True
            score -= 15
        # not capital & small
        mistakes = 0
        for w in title.split():
            if not (
                w.istitle()
                or is_float(w)
                or len(w) < 2
                or w in self.data[i]["product_brand"]
                or "".join(filter(lambda x: x.isalpha(), w)).lower()
                in [
                    "in",
                    "on",
                    "over",
                    "with",
                    "and",
                    "or",
                    "for",
                    "the",
                    "a",
                    "an",
                    "g",
                    "gm",
                    "ml",
                    "l",
                    "litre",
                    "liter",
                ]
            ):
                mistakes += 1
        if mistakes > 0:
            suggestion.append(
                "The Title formating is wrong. The Title should be CamelCase. e.g. {}".format(
                    title.title()
                )
            )  # ['not_camel_case'] = True
            score -= mistakes * 15 / len(title.split())
        # non ascii
        if len(title) != len(title.encode()):
            score -= 20
            suggestion.append(
                "The Title contains non-ascii chars. Remove any non-ascii chars."
            )  # ['not_ascii'] = True

        # Decoration
        for letter in title:
            if letter in "`~!@#$%^*()_=+[{]};:'\"<>?":
                score -= 20
                suggestion.append(
                    "The Title contains decorative chars. Remove any `~!@#$%^*()_=+[{]};:'\"<>?"
                )  # ['decoration'] = True
                break

        # return {"ASIN": self.data[i]['product_asin'], "Title Score": score, "Suggestions": suggestion}
        if len(suggestion) == 0:
            suggestion.append("")
        return [
            {
                "ASIN": self.data[i]["product_asin"],
                # "Title": title, "Brand": self.data[i]['product_brand'],
                "Title Score": score,
                "Suggestions": s,
            }
            for s in suggestion
        ]

    def suggest_details(self, i):
        prod_keys = set(
            map(
                lambda x: x.strip().lower().title(),
                self.data[i]["product_details"].keys(),
            )
        )
        keys_add = []  # Need this detail as most of the prods have this
        keys_remove = (
            []
        )  # Most of the products do not have this. Probably added erreneously.
        # print(prod_keys)
        for k, v in self.keys_present_count.items():
            if v >= 0.6 and k not in prod_keys:
                # print(k)
                keys_add.append(
                    {
                        "ASIN": self.data[i]["product_asin"],
                        "Add Details": k,
                        "Percentage": v,
                    }
                )
                # keys_add.append({"Add Details": k, "Percentage": v})
            if v < 0.05 and k in prod_keys:
                keys_remove.append(
                    {
                        "ASIN": self.data[i]["product_asin"],
                        "Remove Details": k,
                        "Percentage": v,
                    }
                )
                # keys_remove.append({"Remove Details": k, "Percentage": v})

        # prod_bullets = set(map(lambda x: x.strip().lower().title(), self.data[i]['product_bullets'].keys()))
        bullets_add = []  # Need this detail as most of the prods have this
        bullets_remove = (
            []
        )  # Most of the products do not have this. Probably added erreneously.
        # for k, v in self.bullets_present_count.items():
        #     if v >= 0.6 and k not in prod_bullets:
        #         bullets_add.append({"ASIN": self.data[i]['product_asin'], "Add Bullets": k, "Percentage": v})
        #         bullets_add.append({"Add Bullets": k, "Percentage": v})
        #     if v < 0.05 and k in prod_bullets:
        #         bullets_remove.append({"ASIN": self.data[i]['product_asin'], "Remove Bullets": k, "Percentage": v})
        #         bullets_remove.append({"Remove Bullets": k, "Percentage": v})

        return keys_add, keys_remove, bullets_add, bullets_remove
        # return {"ASIN": self.data[i]['product_asin'], "Add Details": keys_add, "Remove Details": keys_remove, "Add Bullets": bullets_add, "Remove Bullets": bullets_remove}

    def convert_doc(self, i):
        # print(self.data[i]['product_details'])
        # TODO prod_bullets = set(map(lambda x: x.strip().lower().title(), self.data[i]['product_bullets'].keys()))
        prod_keys = set(
            map(
                lambda x: x.strip().lower().title(),
                self.data[i]["product_details"].keys(),
            )
        )
        # print(prod_keys)
        return list(
            map(
                lambda k: {
                    "ASIN": self.data[i]["product_asin"],
                    "Details Tag": k,
                    "Present": k in prod_keys,
                },
                self.keys,
            )
        )

    def convert(self):
        done = set()
        print(len(self.keys), self.keys)
        for i in tqdm(range(len(self.data))):
            if self.data[i]["product_asin"] not in done:
                prod_details = self.convert_doc(i)
                title_score = self.title_score(i)
                (
                    details_add,
                    details_remove,
                    bullets_add,
                    bullets_remove,
                ) = self.suggest_details(i)
                print(
                    len(details_add),
                    len(details_remove),
                    len(bullets_add),
                    len(bullets_remove),
                )
                # details_suggestions = self.suggest_details(i)
                done.add(self.data[i]["product_asin"])
            # print(prod_details)
            # break
            else:
                print(self.data[i]["product_asin"])
            if prod_details is not None:
                self.prod_details = self.prod_details.append(
                    prod_details, ignore_index=True
                )
            if title_score is not None:
                self.prod_scores = self.prod_scores.append(title_score)
            if len(details_add) != 0:
                self.details_add = self.details_add.append(
                    details_add, ignore_index=True
                )
            if len(details_remove) != 0:
                self.details_remove = self.details_remove.append(
                    details_remove, ignore_index=True
                )
            if len(bullets_add) != 0:
                self.bullets_add = self.bullets_add.append(
                    bullets_add, ignore_index=True
                )
            if len(bullets_remove) != 0:
                self.bullets_remove = self.bullets_remove.append(
                    bullets_remove, ignore_index=True
                )

            # if title_score is not None:
            #     self.prod_scores.append(title_score)
            # if details_suggestions:
            #     self.details_suggestions.append(details_suggestions)

    def save(self, details_file):
        print(self.details_add)
        self.prod_details.to_csv(details_file, index=False)

    # def save(self, details_file, scores_file, details_add_file, details_remove_file, bullets_add_file, bullets_remove_file):
    #     print(self.details_add)
    #     self.prod_details.to_csv(details_file, index=False)
    #     self.prod_scores.to_csv(scores_file, index=False)
    #     self.details_add.to_csv(details_add_file, index=False)
    #     self.details_remove.to_csv(details_remove_file, index=False)
    #     self.bullets_add.to_csv(bullets_add_file, index=False)
    #     self.bullets_remove.to_csv(bullets_remove_file, index=False)

    # with open(scores_file, 'w') as f:
    #     f.write(json.dumps(self.prod_scores))
    # with open(details_sugg_file, 'w') as f:
    #     f.write(json.dumps(self.details_suggestions))


if __name__ == "__main__":
    # cvt = Converter_details('havells/amazon_marketplace_scraping_havells_07_02_22/product_data.json')
    # cvt.convert()
    # cvt.save('havells/amazon_prod_details.csv')

    # cvt = Converter_details('sanitary_napkins/amazon_marketplace_scraping_sanitary_napkins_24_01_22/product_data.json')
    # cvt.convert()
    # cvt.save('sanitary_napkins/amazon_prod_details.csv')

    # cvt = Converter_details('pureit/amazon_marketplace_scraping_pureit_04_02_22/product_data.json')
    # cvt.convert()
    # cvt.save('pureit/amazon_prod_details.csv')

    # cvt = Converter_details('shell/amazon_marketplace_scraping_shell_01_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('shell/amazon_prod_details.csv')

    # cvt = Converter_details('duracell/amazon_marketplace_scraping_duracell_02_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('duracell/amazon_prod_details.csv')

    # cvt = Converter_details('hafele/amazon_marketplace_scraping_hafele_20_02_22/product_data.json')
    # cvt.convert()
    # cvt.save('hafele/amazon_prod_details.csv')

    # cvt = Converter_details('ferrero/amazon_marketplace_scraping_ferrero_28_02_22/product_data.json')
    # cvt.convert()
    # cvt.save('ferrero/amazon_prod_details.csv')

    # cvt = Converter_details('kohler/amazon_marketplace_scraping_kohler_02_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('kohler/amazon_prod_details.csv')

    # cvt = Converter_details('havells_fans/amazon_marketplace_scraping_havells_fans_03_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('havells_fans/amazon_prod_details.csv')

    # cvt = Converter_details('havells_ac/amazon_marketplace_scraping_havells_ac_03_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('havells_ac/amazon_prod_details.csv')

    # cvt = Converter_details('ghadi/amazon_marketplace_scraping_ghadi_05_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('ghadi/amazon_prod_details.csv')

    # cvt = Converter_details('nestle/amazon_marketplace_scraping_nestle_06_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('nestle/amazon_prod_details2.csv', 'nestle/amazon_prod_scores.csv', 'nestle/amazon_prod_add_details.csv', 'nestle/amazon_prod_remove_details.csv', 'nestle/amazon_prod_add_bullets.csv', 'nestle/amazon_prod_remove_bullets.csv')

    # cvt = Converter_details('park_avenue/amazon_marketplace_scraping_park_avenue_19_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('park_avenue/amazon_prod_details2.csv', 'park_avenue/amazon_prod_scores.csv', 'park_avenue/amazon_prod_add_details.csv', 'park_avenue/amazon_prod_remove_details.csv', 'park_avenue/amazon_prod_add_bullets.csv', 'park_avenue/amazon_prod_remove_bullets.csv')

    # cvt = Converter_details('bajaj_mixer/amazon_marketplace_scraping_bajaj_mixer_25_03_22/product_data.json')
    # cvt.convert()
    # cvt.save('bajaj_mixer/amazon_prod_details2.csv', 'bajaj_mixer/amazon_prod_scores.csv', 'bajaj_mixer/amazon_prod_add_details.csv', 'bajaj_mixer/amazon_prod_remove_details.csv', 'bajaj_mixer/amazon_prod_add_bullets.csv', 'bajaj_mixer/amazon_prod_remove_bullets.csv')

    # cvt = Converter_details('blue_heaven/amazon_marketplace_scraping_blue_heaven_05_04_22/product_data.json')
    # cvt.convert()
    # cvt.save('blue_heaven/amazon_prod_details2.csv', 'blue_heaven/amazon_prod_scores.csv', 'blue_heaven/amazon_prod_add_details.csv', 'blue_heaven/amazon_prod_remove_details.csv', 'blue_heaven/amazon_prod_add_bullets.csv', 'blue_heaven/amazon_prod_remove_bullets.csv')

    # cvt = Converter_details('nature_essence/amazon_marketplace_scraping_nature_essence_05_04_22/product_data.json')
    # cvt.convert()
    # cvt.save('nature_essence/amazon_prod_details2.csv', 'nature_essence/amazon_prod_scores.csv', 'nature_essence/amazon_prod_add_details.csv', 'nature_essence/amazon_prod_remove_details.csv', 'nature_essence/amazon_prod_add_bullets.csv', 'nature_essence/amazon_prod_remove_bullets.csv')

    # cvt = Converter_details('eno/amazon_marketplace_scraping_eno_12_04_22/product_data.json')
    # cvt.convert()
    # cvt.save('eno/amazon_prod_details.csv', 'eno/amazon_prod_scores.csv', 'eno/amazon_prod_add_details.csv', 'eno/amazon_prod_remove_details.csv', 'eno/amazon_prod_add_bullets.csv', 'eno/amazon_prod_remove_bullets.csv')

    # cvt = Converter_details('nescafe_coffee_maker/amazon_marketplace_scraping_nescafe_coffee_maker_12_04_22/product_data.json')
    # cvt.convert()
    # cvt.save('nescafe_coffee_maker/amazon_prod_details.csv', 'nescafe_coffee_maker/amazon_prod_scores.csv', 'nescafe_coffee_maker/amazon_prod_add_details.csv', 'nescafe_coffee_maker/amazon_prod_remove_details.csv', 'nescafe_coffee_maker/amazon_prod_add_bullets.csv', 'nescafe_coffee_maker/amazon_prod_remove_bullets.csv')

    # cvt = Converter_details('nescafe_gold/amazon_marketplace_scraping_nescafe_gold_12_04_22/product_data.json')
    # cvt.convert()
    # cvt.save('nescafe_gold/amazon_prod_details.csv', 'nescafe_gold/amazon_prod_scores.csv', 'nescafe_gold/amazon_prod_add_details.csv', 'nescafe_gold/amazon_prod_remove_details.csv', 'nescafe_gold/amazon_prod_add_bullets.csv', 'nescafe_gold/amazon_prod_remove_bullets.csv')

    cvt = Converter_details(
        "../../data/InputData/manyavar/amazon_marketplace_scraping/product_data.json"
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/manyavar/amazon_marketplace_scraping/amazon_prod_details.csv"
    )

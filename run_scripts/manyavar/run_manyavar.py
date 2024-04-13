import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Manyavar-Keywords.csv", header=None)
    keywords = keywords[0].to_list()
    print(keywords)
    run(
        bsr_100=False,
        search_list=False,
        brand_search=False,
        force_info_scrape=False,
        price_bsr_move=True,
        total_questions_move=True,
        comments=False,
        questionAnswers=False,
        sos=False,
        mongo_db="amazon_marketplace_scraping_manayar",
        run_summary_file="run_summary_manyavar",
        company_client="Manayar",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?bbn=3723382031&rh=n%3A3723382031%2Cp_89%3AManyavar&dc&qid=1679991485&rnid=3837712031&ref=lp_3723382031_nr_p_89_4",  # Manayar for Men's Kurta Sets # 63
            "https://www.amazon.in/s?i=apparel&bbn=3723382031&rh=n%3A3723382031%2Cp_89%3AVASTRAMAY&dc&qid=1679991671&rnid=3837712031&ref=sr_nr_p_89_1&ds=v1%3Ah1J7nDjf%2FE8LNDH6McknU6jQsUe1WHUnQYYUkpsF9Js",  # VASTRAMAY for Men's Kurta Sets # 58
            "https://www.amazon.in/s?i=apparel&bbn=1968248031&rh=n%3A1571271031%2Cn%3A1968024031%2Cn%3A1968248031%2Cn%3A3723382031%2Cp_89%3AEthnix+by+Raymond&dc&ds=v1%3ALaPZrwIL2fA6d%2Fi2Y2qTEpMFcE7OVY63BmLKSbLMRkw&qid=1679996616&rnid=1968248031&ref=sr_nr_n_2",  # Ethnix by Raymond for Men's Kurta Sets # 8
            "https://www.amazon.in/s?i=apparel&bbn=1968248031&rh=n%3A1571271031%2Cn%3A1968024031%2Cn%3A1968248031%2Cn%3A1968250031%2Cp_89%3AManyavar&dc&ds=v1%3AI9UqmRUogqoXpOYKQN6Duolbf4nYKNAAXni6Lk07NPE&qid=1679996067&rnid=1968248031&ref=sr_nr_n_1",  # Manayar for Men's Kurtas # 52
            "https://www.amazon.in/s?i=apparel&bbn=1968250031&rh=n%3A1571271031%2Cn%3A1968024031%2Cn%3A1968248031%2Cn%3A1968250031%2Cp_89%3AVASTRAMAY&dc&qid=1679996408&rnid=3837712031&ref=sr_nr_p_89_2&ds=v1%3AeOCF%2Fm%2FDGZBUnvYhGZR0UwImvOd1Pg4waSxBJZ9IHWA",  # VASTRAMAY for Men's Kurtas # 51
            "https://www.amazon.in/s?i=apparel&bbn=1968248031&rh=n%3A1571271031%2Cn%3A1968024031%2Cn%3A1968248031%2Cn%3A1968250031%2Cp_89%3AEthnix+by+Raymond&dc&ds=v1%3A8xEz842VJWrMZCf5vhdXGOMaQYFzIzkT250E1bTrFUg&qid=1679996548&rnid=1968248031&ref=sr_nr_n_1",  # Ethnix by Raymond for Men's Kurtas # 48
            "https://www.amazon.in/s?bbn=5229862031&rh=n%3A5229862031%2Cp_89%3AManyavar&dc&qid=1679993470&rnid=3837712031&ref=lp_5229862031_nr_p_89_5",  # Manayar for Men's Nehru Jackets & Vests # 51
            "https://www.amazon.in/s?i=apparel&bbn=5229862031&rh=n%3A5229862031%2Cp_89%3AVASTRAMAY&dc&qid=1679993645&rnid=3837712031&ref=sr_nr_p_89_1&ds=v1%3A3o7gu2kPRNeQw5TH3qrqId2l7rj%2BBLYPXNHJ2d7ZSYY",  # VASTRAMAY for Men's Nehru Jackets & Vests # 25
            "https://www.amazon.in/s?i=apparel&bbn=5229862031&rh=n%3A5229862031%2Cp_89%3AEthnix+by+Raymond&dc&qid=1679993757&rnid=3837712031&ref=sr_nr_p_89_1&ds=v1%3AtRt%2FnWMgBH3E3ZIZTq5C4n9elZiJU8072NNDW9wVrJo",  # Ethnix by Raymond for Men's Nehru Jackets & Vests # 49
            "https://www.amazon.in/s?bbn=1968251031&rh=n%3A1968251031%2Cp_89%3AManyavar&dc&qid=1679995951&rnid=3837712031&ref=lp_1968251031_nr_p_89_24",  # Manayar for Men's Sherwani # 3
            "https://www.amazon.in/s?bbn=1968251031&rh=n%3A1968251031%2Cp_89%3AVASTRAMAY&dc&qid=1679993905&rnid=3837712031&ref=lp_1968251031_nr_p_89_2",  # VASTRAMAY for Men's Sherwani # 46
            "https://www.amazon.in/s?i=apparel&bbn=1968251031&rh=n%3A1968251031%2Cp_89%3AEthnix+by+Raymond&dc&qid=1679995807&rnid=3837712031&ref=sr_nr_p_89_1&ds=v1%3AdLeKZwq5MMJtkaVqBmjDpfE5DhfWnRdEZv7kemD5fEc",  # Ethnix by Raymond for Men's Sherwani # 6
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

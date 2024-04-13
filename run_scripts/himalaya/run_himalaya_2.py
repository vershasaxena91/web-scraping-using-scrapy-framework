import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run

if __name__ == "__main__":
    run(
        bsr_100=False,
        search_list=True,
        brand_search=False,
        price_bsr_move=True,
        total_questions_move=True,
        comments=False,
        sos=False,
        mongo_db="amazon_marketplace_scraping_himalaya",
        run_summary_file="run_summary_himalaya",
        company_client="Himalaya",
        search_list_links=[
            "https://tinyurl.com/ycku6d56",  # https://www.amazon.in/s?k=shampoo&i=beauty&rh=n%3A1355016031%2Cp_89%3AHead+%26+Shoulders&dc&crid=1W2684GTTJCSQ&qid=1639980182&rnid=3837712031&sprefix=shampoo%2Cbeauty%2C210&ref=sr_nr_p_89_2
            "https://tinyurl.com/2p9cyy28",  # https://www.amazon.in/s?k=anti+dandruff+shampoo&i=beauty&rh=n%3A1355016031%2Cp_89%3AClear%7CDove%7CHimalaya&dc&crid=BDB7S0I8MF92&qid=1639980267&rnid=3837712031&sprefix=anti+dandruff+shampoo%2Caps%2C211&ref=sr_nr_p_89_10
            "https://tinyurl.com/2p8bfv5z",  # https://www.amazon.in/s?k=meera+anti+dandruff+shampoo&rh=n%3A1355016031%2Cp_89%3AMeera&dc&qid=1639980354&rnid=3837712031&sprefix=meera+%2Caps%2C213&ref=sr_nr_p_89_1
        ],
        sos_keywords=[
            "Anti dandruff shampoo",
            "Dandruff Shampoo",
            "Himalaya Anti Dandruff Shampoo",
            "Dove Anti Dandruff Shampoo",
            "Meera Anti Dandruff Shampoo",
            "Head & Shoulders Anti Dandruff Shampoo",
            "Clear Anti Dandruff Shampoo",
            "Anti Dandruff Shampoo for men",
            "Anti Dandruff Shampoo for women",
        ],
    )
    reactor.run()

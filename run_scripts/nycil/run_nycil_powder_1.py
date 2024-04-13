import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Nycil_Powder-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_nycil_powder",
        run_summary_file="run_summary_nycil_powder",
        company_client="Nycil_Powder",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?k=NYCIL&i=beauty&bbn=9530413031&rh=n%3A9530413031&dc&qid=1667827719&rnid=3837712031&ref=sr_nr_p_89_1&ds=v1%3Ab7jexh70eYD4F5ue2O4Nh3juPYrC8MU0CIsTrgWEcAQ",  # Nycil # 129
            "https://www.amazon.in/s?i=beauty&bbn=9530413031&rh=n%3A9530413031%2Cp_89%3ACandid+Prickly&dc&ds=v1%3AL1mcWmY64d8BJ4XNRUy0lpJWuJPKAu%2FhXUnhM3u5L6M&qid=1667822803&rnid=3837712031&ref=sr_nr_p_89_1",  # Candid Prickly # 1
            "https://www.amazon.in/s?i=beauty&bbn=1374408031&rh=n%3A1355016031%2Cn%3A1374407031%2Cn%3A1374408031%2Cn%3A9530413031%2Cp_89%3ANavratna&dc&ds=v1%3A0Zh3oMf68%2FG%2Bu6AU%2FK9ywTPb%2FZNN2MiDGaL%2BN%2FI9rdc&qid=1667822266&rnid=1374408031&ref=sr_nr_n_2",  # Navratna # 8
            "https://www.amazon.in/s?k=Dermicool&rh=n%3A9530413031&dc&ds=v1%3AAPPg9ON9G8SHivB%2BO11zZL3Gu0tvNL8bgI8UO0JA8yg&qid=1667828276&rnid=3576079031&ref=sr_nr_n_1",  # Dermi Cool # 92
            "https://www.amazon.in/s?i=beauty&bbn=9530413031&rh=n%3A1355016031%2Cn%3A1374407031%2Cn%3A1374408031%2Cn%3A9530413031%2Cp_89%3AShower+To+Shower&dc&ds=v1%3AL1mcWmY64d8BJ4XNRUy0lpJWuJPKAu%2FhXUnhM3u5L6M&qid=1667822803&rnid=3837712031&ref=sr_nr_p_89_1",  # Shower to Shower # 1
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

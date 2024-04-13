import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Havells_Fans-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_havells_fans",
        run_summary_file="run_summary_havells_fans",
        company_client="Havells_Fans",
        bsr_100_links=[],
        search_list_links=[
            "https://tinyurl.com/ymn6dare",  # https://www.amazon.in/s?k=havells+fans&i=kitchen&rh=n%3A976442031%2Cp_89%3AHavells&dc&crid=1UBP78NJ4VK5C&qid=1646209251&rnid=3837712031&sprefix=havells+fan%2Caps%2C244&ref=sr_pg_1
            "https://tinyurl.com/2k9snuve",  # https://www.amazon.in/s?k=bajaj+fans&rh=n%3A976442031%2Cp_89%3ABajaj&dc&crid=1M29PNJYH8OPR&qid=1646209291&rnid=3837712031&sprefix=bajaj+fan%2Caps%2C232&ref=sr_nr_p_89_1
            "https://tinyurl.com/6yzey3w8",  # https://www.amazon.in/s?k=fans&rh=n%3A976442031%2Cp_89%3AOrient+Electric&dc&crid=3UDH0YV4NX0BP&qid=1645611300&rnid=3837712031&sprefix=fan%2Caps%2C214&ref=sr_nr_p_89_6
            "https://tinyurl.com/4f7276v4",  # https://www.amazon.in/s?k=fans&i=kitchen&rh=n%3A976442031%2Cp_89%3ACrompton&dc&crid=3UDH0YV4NX0BP&qid=1645611371&rnid=3837712031&sprefix=fan%2Caps%2C214&ref=sr_nr_p_89_2
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

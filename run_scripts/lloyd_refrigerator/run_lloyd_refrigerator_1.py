import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Lloyd_Refrigerator-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_lloyd_refrigerator",
        run_summary_file="run_summary_lloyd_refrigerator",
        company_client="Lloyd_refrigerator",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?bbn=1380365031&rh=n%3A1380365031%2Cp_89%3AHaier&dc&qid=1656417857&rnid=3837712031&ref=lp_1380365031_nr_p_89_1",  # Haier
            "https://www.amazon.in/s?bbn=1380365031&rh=n%3A1380365031%2Cp_89%3ASamsung&dc&qid=1656417968&rnid=3837712031&ref=lp_1380365031_nr_p_89_0",  # Samsung
            "https://www.amazon.in/s?bbn=1380365031&rh=n%3A1380365031%2Cp_89%3ALG&dc&qid=1656418021&rnid=3837712031&ref=lp_1380365031_nr_p_89_2",  # LG
            "https://www.amazon.in/s?i=kitchen&bbn=1380365031&rh=n%3A1380365031%2Cp_89%3AWhirlpool&dc&ds=v1%3AIV0clHEa1uQO48mfmcANEX8po6dQ5jlDukDLMZyK9dc&qid=1656418096&rnid=3837712031&ref=sr_nr_p_89_1",  # Whirlpool
            "https://www.amazon.in/s?i=kitchen&bbn=1380365031&rh=n%3A1380365031%2Cp_90%3A6741118031%2Cp_89%3ALloyd&dc&qid=1656418241&rnid=3837712031&ref=sr_nr_p_89_1&ds=v1%3AP4f6Qxc%2BLt%2BxFOrrobsV2qYMQmDX2AQpqAqZ5v%2F3aGE",  # Lloyd
            "https://www.amazon.in/s?k=Liebherr&rh=n%3A1380365031%2Cp_89%3ALiebherr&dc&ds=v1%3ABnEZE0LLdcOIJNCU%2F6%2B8Ksvq3SQxhQ%2F%2FbgP7hoVg1%2Fg&qid=1656418413&rnid=3837712031&ref=sr_nr_p_89_1",  # Liebherr
            "https://www.amazon.in/s?k=Voltas+Beko&rh=n%3A1380263031%2Cn%3A1380365031&dc&ds=v1%3AEGVFYW6bxVunNzLpnlsvMi2jUgDYUOcdbB8G6aoxBiw&qid=1656418517&rnid=3576079031&ref=sr_nr_n_4",  # Voltas Beko
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

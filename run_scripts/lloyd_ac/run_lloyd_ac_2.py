import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Lloyd_AC-Keywords.csv", header=None)
    keywords = keywords[0].to_list()
    print(keywords)
    run(
        bsr_100=False,
        search_list=False,
        brand_search=False,
        force_info_scrape=False,
        price_bsr_move=False,
        total_questions_move=False,
        comments=True,
        questionAnswers=True,
        sos=False,
        mongo_db="amazon_marketplace_scraping_lloyd_ac",
        run_summary_file="run_summary_lloyd_ac",
        company_client="Lloyd_AC",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?k=ac&rh=n%3A3474656031%2Cp_89%3ALloyd&dc&ds=v1%3ApavEG5GSFQ6YejO4pxdVRv%2F9Z6NrAw3Dx8JIqjtyrqA&crid=3U26H67ZAXF62&qid=1656415324&rnid=3837712031&sprefix=ac%2Caps%2C327&ref=sr_nr_p_89_5",  # Lloyd
            "https://www.amazon.in/s?k=ac&i=kitchen&bbn=3474656031&rh=n%3A3474656031%2Cp_89%3AVoltas&dc&crid=3U26H67ZAXF62&qid=1656415436&rnid=3837712031&sprefix=ac%2Caps%2C327&ref=sr_nr_p_89_11&ds=v1%3AuE%2FDx0u09caGQMEkIrmtTSG%2B8zhPVT%2FDXErIc5GNVeY",  # Voltas
            "https://www.amazon.in/s?k=ac&i=kitchen&bbn=3474656031&rh=n%3A3474656031%2Cp_89%3APanasonic&dc&crid=3U26H67ZAXF62&qid=1656415575&rnid=3837712031&sprefix=ac%2Caps%2C327&ref=sr_nr_p_89_5&ds=v1%3AXM7%2FS2AHUZmrZ9kk2%2FQ4m7o7Yppf5TbXQ70kfx%2FaDmk",  # Panasonic
            "https://www.amazon.in/s?k=split+ac&i=kitchen&bbn=3474656031&rh=n%3A3474656031%2Cp_89%3ABLUE+STAR&dc&qid=1656415869&rnid=3837712031&ref=sr_nr_p_89_9&ds=v1%3An%2FvKShGNYKgjJtgamXKzK7iR76Z13vfHVS0hkXJit%2F4",  # Blue Star
            "https://www.amazon.in/s?k=ac&i=kitchen&bbn=3474656031&rh=n%3A3474656031%2Cp_89%3ALG&dc&qid=1656415791&rnid=3837712031&ref=sr_nr_p_89_2&ds=v1%3AdqT0Ez0qGBg35HEXphdUcfuHipLGQ%2B1YDjihVKy%2Beqc",  # LG
            "https://www.amazon.in/s?k=ac&i=kitchen&rh=n%3A3474656031%2Cp_89%3ADaikin&dc&ds=v1%3AWGyTW%2BScz8SVk0CwtJBwZvhDaoFGvNWxYuMi%2BTLNZbs&qid=1656415741&rnid=3837712031&ref=sr_nr_p_89_2",  # Daikin
            "https://www.amazon.in/s?k=split+ac&i=kitchen&rh=n%3A3474656031%2Cp_89%3AHitachi&dc&ds=v1%3AzojXKUQduLwBypnu8i7QPZbVjG6noaJ8LbA70N0bemA&qid=1656415844&rnid=3837712031&ref=sr_nr_p_89_11",  # Hitachi
        ],
        brand_search_links=[
            "https://www.amazon.in/stores/page/54732EDB-06B9-4AD5-A3EB-6BDCF50694B0?ingress=0&visitId=b359ab60-ebd7-4bc5-9df9-a5040543b3fb&lp_slot=auto-sparkle-hsa-tetris&store_ref=SB_A04008761LEAKJQUBT3ZQ&ref_=sbx_be_s_sparkle_td_logo",  # Haier Turbocool
            "https://www.amazon.in/stores/page/AC0CFB7B-E819-47BD-B2C2-3E65961C494E?ingress=0&visitId=b359ab60-ebd7-4bc5-9df9-a5040543b3fb&lp_slot=auto-sparkle-hsa-tetris&store_ref=SB_A04008761LEAKJQUBT3ZQ&ref_=sbx_be_s_sparkle_td_logo",  # Haier Puricool Pro
            "https://www.amazon.in/stores/page/3B2B3009-EC07-48CA-A39B-5F80F5019EB8?ingress=0&visitId=b359ab60-ebd7-4bc5-9df9-a5040543b3fb&lp_slot=auto-sparkle-hsa-tetris&store_ref=SB_A04008761LEAKJQUBT3ZQ&ref_=sbx_be_s_sparkle_td_logo",  # Haier Window AC
        ],
        sos_keywords=keywords,
    )
    reactor.run()

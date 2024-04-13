import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Havells-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_havells_washing_machine",
        run_summary_file="run_summary_havells_washing_machine",
        company_client="Havells_Washing_Machine",
        bsr_100_links=[
            "https://www.amazon.in/gp/bestsellers/kitchen/1380369031/ref=zg_bs_nav_kitchen_2_1380263031",
            "https://www.amazon.in/gp/bestsellers/kitchen/1380369031/ref=zg_bs_pg_2?ie=UTF8&pg=2",
        ],
        search_list_links=["https://www.amazon.in/s?k=washing+machine"],
        brand_search_links=[
            "https://www.amazon.in/stores/page/74188F18-4491-46BC-9457-E358D56DA0A0",  # Lloyd
            "https://www.amazon.in/stores/page/1F5BCC61-1042-439E-9FF5-22C4D77B5BA6",  # Haier Fully Automatic Front Load
            "https://www.amazon.in/stores/page/38619A65-1B99-49BA-8AB5-6579B95A4F13",  # Haier Fully Automatic Top Load
            "https://www.amazon.in/stores/page/694011C3-D3F9-485B-89F9-437F95530D6F",  # Haier Semi Automatic
            "https://www.amazon.in/stores/page/436ADFF7-31F6-43A1-B73B-14D4C9610392",  # Bosch
            "https://www.amazon.in/stores/page/8674705A-2198-4A58-BAF0-90A977BAD425",  # Bosch Front Load
            "https://www.amazon.in/stores/page/67F13544-5717-4D86-ADEA-9E586C4CAF34",  # Bosch Top Load
            "https://www.amazon.in/stores/page/8F6158EC-471C-4B43-9001-D53725B0AD38",  # Bosch Washer Dryer
            "https://www.amazon.in/stores/page/480F909A-2723-453F-B31C-3F07E321D9E7",  # Samsung Top Load
            "https://www.amazon.in/stores/page/6468E539-77ED-438A-AD31-FCBBCC461E1C",  # Samsung Front Load
            "https://www.amazon.in/stores/page/A4A19FFC-04CF-4583-8105-6B8C2F4700C7",  # Samsung Washer Dryer
            "https://www.amazon.in/stores/page/DBE77AA5-C256-4510-B88B-EB8014372514",  # Samsung Semi Automatic
            "https://www.amazon.in/stores/page/E78E0B58-8421-4F7E-9127-CD711BB8672F",  # Whirlpool Semi Automatic
            "https://www.amazon.in/stores/page/398444F0-7851-4BFB-A4D6-6469838523A0",  # Whirlpool Fully Automatic Top Load
            "https://www.amazon.in/stores/page/8E6FD661-CC3F-47BC-AE52-690B87772DC1",  # Whirlpool Fully Automatic Front Load
            "https://www.amazon.in/stores/page/732CE73F-969A-4FB9-86AD-0BE101F4806A",  # Whirlpool Premium Fully Automatic Top Load
            "https://www.amazon.in/stores/page/3A7855AC-F0DD-4149-819F-76F9DD303B28",  # Whirlpool 6 & 6.5 Kg
            "https://www.amazon.in/stores/page/2B668C68-5F58-47F0-827A-CEC40B9A9DC5",  # Whirlpool 7 & 7.5 Kg
            "https://www.amazon.in/stores/page/8C416D6D-FC08-4402-9FCD-268CFB8F046E",  # Whirlpool 8 Kg +
            "https://www.amazon.in/stores/page/9481E36A-82F5-42BA-8B23-7A794DE33C24",  # Whirlpool with Built-In Heater
            "https://www.amazon.in/stores/page/C67DACA2-E1E4-4EE9-8936-C4DE3B1710FF",  # Whirlpool with Hard Water Wash
        ],
        sos_keywords=keywords,
    )
    reactor.run()

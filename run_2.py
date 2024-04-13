from twisted.internet import reactor
from run import run

if __name__ == "__main__":
    run(
        bsr_100=True,
        search_list=True,
        brand_search=False,
        price_bsr_move=True,
        comments=False,
        sos=True,
        mongo_db="amazon_marketplace_scraping_all_shampoos",
        run_summary_file="run_summary_all_shampoos",
        company_client="GroupM",
        bsr_100_links=[
            "https://tinyurl.com/x8we47hb",  # https://www.amazon.in/gp/bestsellers/beauty/1374334031/ref=zg_bs_nav_beauty_3_9851597031
            "https://tinyurl.com/hwnp6vz3",  # https://www.amazon.in/gp/bestsellers/beauty/1374334031/ref=zg_bs_pg_2?ie=UTF8&pg=2
        ],
        search_list_links=[
            "https://tinyurl.com/xpme2pv4",  # https://www.amazon.in/s?k=shampoo&i=beauty&rh=n%3A1355016031%2Cp_89%3ABiotique%7CDove%7CHead+%26+Shoulders%7CL%27Oreal+Paris%7CTRESemme
        ],
        sos_keywords=[
            "Anti dandruff shampoo",
            "Shampoo for dry and frizzy hair",
            "Shampoo and conditioner combo",
            "Shampoo and conditioner",
            "Shampoo 1 litre",
            "Dandruff shampoo",
            "Hair fall control shampoo",
            "Shampoo for oily scalp",
            "Shampoo for dry hair",
            "Shampoo for coloured hair",
            "Shampoo for thin hair",
            "Shampoo for men",
            "Shampoo for women",
        ],
    )
    reactor.run()

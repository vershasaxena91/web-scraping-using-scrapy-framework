import sys
from pymongo import MongoClient
from twisted.internet import reactor
from run import run
from run_flipkart import run as run2
from run_nykaa import run as run3

key = sys.argv[1]
client = MongoClient("mongodb://localhost:27017/")

if "amazon" in str(key).lower():
    db = client.amazon_marketplace_scraping
    collection = db.input_data
    configuration = collection.find_one({"_id": key})
    print("Configuration :", configuration)
    run(
        bsr_100=configuration["bsr_100"],
        search_list=configuration["search_list"],
        brand_search=configuration["brand_search"],
        force_info_scrape=configuration["force_info_scrape"],
        price_bsr_move=configuration["price_bsr_move"],
        total_questions_move=configuration["total_questions_move"],
        comments=configuration["comments"],
        questionAnswers=configuration["questionAnswers"],
        sos=configuration["sos"],
        mongo_db=configuration["mongo_db"],
        run_summary_file=configuration["run_summary_file"],
        company_client=configuration["company_client"],
        bsr_100_links=configuration["bsr_100_links"],
        search_list_links=configuration["search_list_links"],
        brand_search_links=configuration["brand_search_links"],
        sos_keywords=configuration["sos_keywords"],
    )
    reactor.run()

elif "flipkart" in str(key).lower():
    db = client.flipkart_marketplace_scraping
    collection = db.input_data
    configuration = collection.find_one({"_id": key})
    print("Configuration :", configuration)
    run2(
        rank_move=configuration["rank_move"],
        insert_at_rank=configuration["insert_at_rank"],
        search_list=configuration["search_list"],
        force_info_scrape=configuration["force_info_scrape"],
        price_bsr_move=configuration["price_bsr_move"],
        comments=configuration["comments"],
        sos=configuration["sos"],
        mongo_db=configuration["mongo_db"],
        run_summary_file=configuration["run_summary_file"],
        company_client=configuration["company_client"],
        rank_list_links=configuration["rank_list_links"],
        search_list_links=configuration["search_list_links"],
        sos_keywords=configuration["sos_keywords"],
    )
    reactor.run()

elif "nykaa" in str(key).lower():
    db = client.flipkart_marketplace_scraping
    collection = db.input_data
    configuration = collection.find_one({"_id": key})
    print("Configuration :", configuration)
    run3(
        brand_search=configuration["brand_search"],
        force_info_scrape=configuration["force_info_scrape"],
        price_move=configuration["price_move"],
        rank_move=configuration["rank_move"],
        comments=configuration["comments"],
        questions=configuration["questions"],
        sos=configuration["sos"],
        mongo_db=configuration["mongo_db"],
        run_summary_file=configuration["run_summary_file"],
        company_client=configuration["company_client"],
        brand_search_ids=configuration["brand_search_ids"],
        rank_list_links=configuration["rank_list_links"],
        sos_keywords=configuration["sos_keywords"],
    )
    reactor.run()

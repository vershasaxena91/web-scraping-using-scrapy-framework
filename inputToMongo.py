from pymongo import MongoClient

check = True
while check:
    print("Please enter the marketplace:")
    marketplace = input().lower()
    if marketplace == "amazon":
        check = False
    if check == True:
        print("Marketplace Incorrect")


check = True
print("Please enter the brand name: ")
brand = input().lower()

print("Please enter the client name: ")
client_name = input()


while check:
    print("Please choose the type of scrapping you want :")
    print("Press 1 for keyword search list:")
    print("Press 2 for brand page product list:")
    print("Press 3 for bsr_100 product list:")
    type = input()
    if type == "1" or type == "2" or type == "3":
        check = False
    if check == True:
        print("Wrong type selected")

client = MongoClient("mongodb://localhost:27017/")
db = client.scrapping_input
collection = db.input_configuration

key = marketplace + "_" + brand
bsr_100 = False
search_list = False
brand_search = False
force_info_scrape = False
price_bsr_move = True
total_questions_move = True
comments = False
questionAnswers = False
sos = False
mongo_db = "amazon_marketplace_scraping_" + brand
run_summary_file = "run_summary_" + brand + marketplace
company_client = client_name
bsr_100_links = []
search_list_links = []
brand_search_links = []
sos_keywords = []

print("Do you want to scrape question answers ?")
print("Press 1 for yes else press 2")
qa_check = input()
print("Do you want to scrape comments?")
print("Press 1 for yes else press 2")
com_check = input()

if qa_check == "1":
    questionAnswers = True
if com_check == "1":
    comments = True

if type == "1":
    print("Please enter the number of urls of keyword search page")
    n = int(input())
    for i in range(n):
        print("Please enter url :")
        search_list_links.append(input())
    search_list = True
    collection.insert_one(
        {
            "_id": key,
            "bsr_100": bsr_100,
            "search_list": search_list,
            "brand_search": brand_search,
            "force_info_scrape": force_info_scrape,
            "price_bsr_move": price_bsr_move,
            "total_questions_move": total_questions_move,
            "comments": comments,
            "questionAnswers": questionAnswers,
            "sos": sos,
            "mongo_db": mongo_db,
            "run_summary_file": run_summary_file,
            "company_client": client_name,
            "bsr_100_links": bsr_100_links,
            "search_list_links": search_list_links,
            "brand_search_links": brand_search_links,
            "sos_keywords": sos_keywords,
        }
    )

elif type == "2":
    print("Please enter the number of urls of brand page")
    n = int(input())
    for i in range(n):
        print("Please enter url :")
        brand_search_links.append(input())
    brand_search = True
    collection.insert_one(
        {
            "_id": key,
            "bsr_100": bsr_100,
            "search_list": search_list,
            "brand_search": brand_search,
            "force_info_scrape": force_info_scrape,
            "price_bsr_move": price_bsr_move,
            "total_questions_move": total_questions_move,
            "comments": comments,
            "questionAnswers": questionAnswers,
            "sos": sos,
            "mongo_db": mongo_db,
            "run_summary_file": run_summary_file,
            "company_client": client_name,
            "bsr_100_links": bsr_100_links,
            "search_list_links": search_list_links,
            "brand_search_links": brand_search_links,
            "sos_keywords": sos_keywords,
        }
    )

elif type == "3":
    print("Please enter the number of urls of bsr_100 page")
    n = int(input())
    for i in range(n):
        print("Please enter url :")
        bsr_100_links.append(input())
    bsr_100 = True
    collection.insert_one(
        {
            "_id": key,
            "bsr_100": bsr_100,
            "search_list": search_list,
            "brand_search": brand_search,
            "force_info_scrape": force_info_scrape,
            "price_bsr_move": price_bsr_move,
            "total_questions_move": total_questions_move,
            "comments": comments,
            "questionAnswers": questionAnswers,
            "sos": sos,
            "mongo_db": mongo_db,
            "run_summary_file": run_summary_file,
            "company_client": client_name,
            "bsr_100_links": bsr_100_links,
            "search_list_links": search_list_links,
            "brand_search_links": brand_search_links,
            "sos_keywords": sos_keywords,
        }
    )

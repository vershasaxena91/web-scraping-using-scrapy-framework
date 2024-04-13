import requests
from bs4 import BeautifulSoup

# from pymongo import MongoClient
# client = MongoClient()
# db = client["amazon_product_data"]
# collection = db["product_data"]
url = "http://amazon.in/dp/B08T3325CD"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36"
}

webpage = requests.get(url, headers=HEADERS)
soup = BeautifulSoup(webpage.content, "html.parser")

for element in soup.find_all(["a", "link"]):
    link = element.get("href")
    # collection.insert_one({'link':link})
    print(link)

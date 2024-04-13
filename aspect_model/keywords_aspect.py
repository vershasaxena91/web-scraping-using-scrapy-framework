import pandas as pd
import gensim


def Read_data_from_csv(filename, modelname, aspectfilename):
    data = pd.read_csv(aspectfilename)
    Aspect_list = []
    for i in data.Aspect:
        if i not in Aspect_list and str(i) != "nan":
            Aspect_list.append(i.lower())
    # Aspect_list = ["pricing","quality","taste","health","flavour","packaging","ingredient","quantity"]

    Aspects = []
    # Aspects_val=[]
    data = pd.read_csv(filename)
    model = gensim.models.Word2Vec.load(modelname)
    # print(model.wv.most_similar("packaging"))
    for i in data.keyword:
        val = -1
        word = ""
        for k in Aspect_list:
            for j in i.split(" "):
                try:
                    x = model.wv.similarity(w1=j.lower(), w2=k.lower())
                    if x > val and x > 0.4:
                        val = x
                        word = k
                except:
                    continue
        if i == "product" or i == "Product":
            word = "Quality"
        elif i == "Amazon" or i == "amazon":
            word = ""
        Aspects.append(word.title())
        # Aspects_val.append(val)
    # print(Aspects)
    data["Aspect"] = Aspects
    # data["Aspects_val"]=Aspects_val
    data.to_csv(filename)


Read_data_from_csv(
    "../data/OutputData/nycil_powder/flipkart_marketplace_scraping/keywords.csv",
    "./model/word2vec-amazon_reviews_Beauty_5.model",
    "./aspect/Beauty.csv",
)

import gensim
import pandas as pd
import json
import gzip


def model(filename):
    data = []
    with gzip.open(filename + ".json.gz") as f:
        for l in f:
            data.append(json.loads(l.strip()))

    # total length of list, this number equals total number of products
    print(len(data))

    # first row of the list
    print(data[0])

    df = pd.DataFrame.from_dict(data)

    print(df.head())

    # Simple Preprocessing & Tokenization
    review_text = df.reviewText.apply(gensim.utils.simple_preprocess)
    print(review_text)

    print(review_text.loc[1])

    print(df.reviewText.loc[5])

    # Train a Word2Vec Model
    model = gensim.models.Word2Vec(
        window=10,
        min_count=2,
        workers=4,
    )

    model.build_vocab(review_text, progress_per=1000)

    model.train(review_text, total_examples=model.corpus_count, epochs=model.epochs)

    model.save("./model/word2vec-amazon_" + filename + ".model")


def model_Test(filename):
    model = gensim.models.Word2Vec.load(
        "./model/word2vec-amazon_" + filename + ".model"
    )

    print(model.wv.most_similar("health"))

    print(model.wv.similarity(w1="packaging", w2="pack"))


model("reviews_Pet_Supplies_5")
# model_Test("reviews_Pet_Supplies_5")

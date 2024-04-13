# export GOOGLE_APPLICATION_CREDENTIALS="C:/Users/vesaxena/amazon_marketplace_product_scraping/web-scraping/google_application_credentials/wtc-amazon-scrp-6484791fa61c.json"
from google.cloud import language_v1
import pandas as pd
import json
from tqdm import tqdm


def sample_analyze_entity_sentiment(text_content):
    """
    Analyzing Entity Sentiment in a String

    Args:
      text_content The text content to analyze
    """

    client = language_v1.LanguageServiceClient()

    # text_content = 'Grapes are good. Bananas are bad.'

    # Available types: PLAIN_TEXT, HTML
    type_ = language_v1.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "en"
    document = {"content": text_content, "type_": type_, "language": language}

    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = language_v1.EncodingType.UTF8

    response = client.analyze_entity_sentiment(
        request={"document": document, "encoding_type": encoding_type}
    )
    # print(response)
    # Loop through entitites returned from the API
    # for entity in response.entities:
    #     print(u"Representative name for the entity: {}".format(entity.name))
    #     # Get entity type, e.g. PERSON, LOCATION, ADDRESS, NUMBER, et al
    #     print(u"Entity type: {}".format(language_v1.Entity.Type(entity.type_).name))
    #     # Get the salience score associated with the entity in the [0, 1.0] range
    #     print(u"Salience score: {}".format(entity.salience))
    #     # Get the aggregate sentiment expressed for this entity in the provided document.
    #     sentiment = entity.sentiment
    #     print(u"Entity sentiment score: {}".format(sentiment.score))
    #     print(u"Entity sentiment magnitude: {}".format(sentiment.magnitude))
    #     # Loop over the metadata associated with entity. For many known entities,
    #     # the metadata is a Wikipedia URL (wikipedia_url) and Knowledge Graph MID (mid).
    #     # Some entity types may have additional metadata, e.g. ADDRESS entities
    #     # may have metadata for the address street_name, postal_code, et al.
    #     for metadata_name, metadata_value in entity.metadata.items():
    #         print(u"{} = {}".format(metadata_name, metadata_value))

    #     # Loop over the mentions of this entity in the input document.
    #     # The API currently supports proper noun mentions.
    #     for mention in entity.mentions:
    #         print(u"Mention text: {}".format(mention.text.content))
    #         # Get the mention type, e.g. PROPER for proper noun
    #         print(
    #             u"Mention type: {}".format(language_v1.EntityMention.Type(mention.type_).name)
    #         )
    #     print("-"*20)
    # # Get the language of the text, which will be the same as
    # # the language specified in the request or, if not specified,
    # # the automatically-detected language.
    # print(u"Language of the text: {}".format(response.language))
    return response


if __name__ == "__main__":
    # content = "Shampoo is good for hairfall but has pungent fragrance... :( "
    # sample_analyze_entity_sentiment(content)
    data = pd.read_csv(
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_comments_for_ABSA.csv"
    )
    # print(data)
    ABSAs = []
    count = 0
    for i in tqdm(data.index):
        count += 1
        # print(count)
        # print(data['description'][i])
        try:
            response = sample_analyze_entity_sentiment(data["description"][i])
            for entity in response.entities:
                # for mention in entity.mentions:
                ABSAs.append(
                    {
                        "ASIN": data["ASIN"][i],
                        "username": data["username"][i],
                        "rating": data["rating"][i],
                        "title": data["title"][i],
                        "date": data["date"][i],
                        "description": data["description"][i],
                        "keyword": entity.name,
                        "importance": entity.salience,
                        "sentiment_score": entity.sentiment.score,
                        "magnitude": entity.sentiment.magnitude,
                        ## Amazon
                        # 'country': data['country'][i],
                        # 'helpful': data['helpful'][i],
                        ## Flipkart
                        "Marketplace": data["Marketplace"][i],
                        "likes": data["likes"][i],
                        "dislikes": data["dislikes"][i],
                        "verified": data["verified"][i],
                    }
                )
        except:
            print(data["ASIN"][i])
            # print(data["description"][i])

    sentiments = pd.DataFrame(ABSAs)

    sentiments.to_csv(
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_prods_with_ABSA.csv",
        index=False,
    )

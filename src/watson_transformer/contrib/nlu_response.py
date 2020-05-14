from pyspark.sql.types import StringType, FloatType, StructType, StructField, Row

"""
"
" default NLU output formatter which parse keywords, concepts, sentiment and emotion
"
"""
def default_features_parser(response, **params):
    """
    @param::output: the output json object from STT
    @return:the transcript join by period in string format
    """
    data = {}
    features = params["features"]
    keywords_limit = features.keywords.limit
    concepts_limit = features.concepts.limit

    # extract keyword data
    if "keywords" in response:
        for i in range(keywords_limit):
            if i < len(response["keywords"]):
                kw = response["keywords"][i]
                data['keyword_%d'%(i)] = kw['text']
                data['keyword_%d_score'%(i)] = kw['relevance']
            else: # when less keywords extracted than limit
                data['keyword_%d'%(i)] = None
                data['keyword_%d_score'%(i)] = None
    # extract concept
    if "concepts" in response:
        for i in range(concepts_limit):
            if i < len(response["concepts"]):
                concept = response["concepts"][i]
                data['concept_%d'%(i)] = concept['text']
                data['concept_%d_score'%(i)] = concept['relevance']
            else: # when less concept extracted than limit
                data['concept_%d'%(i)] = None
                data['concept_%d_score'%(i)] = None
    # extract sentiment
    if "sentiment" in response:
        data["sentiment_score"] = response["sentiment"]["document"]["score"]
        data["sentiment_label"] = response["sentiment"]["document"]["label"]
    # extract "emotion"
    if "emotion" in response:
        data["sadness_score"] = response["emotion"]["document"]["emotion"]["sadness"]
        data["joy_score"] = response["emotion"]["document"]["emotion"]["joy"]
        data["fear_score"] = response["emotion"]["document"]["emotion"]["fear"]
        data["disgust_score"] = response["emotion"]["document"]["emotion"]["disgust"]
        data["anger_score"] = response["emotion"]["document"]["emotion"]["anger"]
     
    return Row(**data)
from pyspark.sql.types import StringType, FloatType, StructType, StructField, Row

"""
"
" default NLU output formatter which parse keywords, concepts, sentiment and emotion
"
"""
def default_features_parser(response):
    """
    @param::output: the output json object from STT
    @return:the transcript join by period in string format
    """
    data = {}
    # extract keyword data
    if "keywords" in response:
        for index, kw in enumerate(response["keywords"]):
            data['keyword_%d'%(index)] = kw['text']
            data['keyword_%d_score'%(index)] = kw['relevance']
    # extract concept
    if "concepts" in response:
        for index, concept in enumerate(response["concepts"]):
            data['concepts_%d'%(index)] = concept['text']
            data['concepts_%d_score'%(index)] = concept['relevance']
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
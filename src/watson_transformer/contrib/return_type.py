from pyspark.sql.types import StringType, FloatType, StructType, StructField, Row


"""
"
" return the default NLU return type
"
"""
def nlu_default_return_type(**params):
    """
    @param::num_keywords: the number of keywords extracted by NLU
    @param::num_concpets: the number of concepts extracted by NLU
    @return: the defined return type
    """
    fields = []
    features = params["features"]
    num_keywords = features.keywords.limit
    num_concepts = features.concepts.limit
    # populate keyword fields
    for i in range(num_keywords):
        fields.append(StructField("keyword_%d"%(i), StringType(), True))
        fields.append(StructField("keyword_%d_score"%(i), FloatType(), True))

    # populate concpet fields
    for i in range(num_concepts):
        fields.append(StructField("concept_%d"%(i), StringType(), True))
        fields.append(StructField("concept_%d_score"%(i), FloatType(), True))

    # populate other fields
    fields.extend([StructField("sentiment_score", FloatType(), True),
                   StructField("sentiment_label", StringType(), True),
                   StructField("sadness_score", FloatType(), True),
                   StructField("joy_score", FloatType(), True),
                   StructField("fear_score", FloatType(), True),
                   StructField("disgust_score", FloatType(), True),
                   StructField("anger_score", FloatType(), True)])

    return StructType(fields)


"""
"
" return the default STT return type
"
"""
def stt_default_return_type(**params):
    """
    @return: the defined return type
    """
    return StringType()
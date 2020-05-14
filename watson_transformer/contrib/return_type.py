from pyspark.sql.types import StringType, FloatType, StructType, StructField, Row


"""
"
" return the default NLU return type
"
"""
def nlu_default_return_type(num_keywords=5, num_concepts=5):
    """
    @param::num_keywords: the number of keywords extracted by NLU
    @param::num_concpets: the number of concepts extracted by NLU
    @return: the defined return type
    """
    fields = []

    # populate keyword fields
    for i in range(num_keywords):
        fields.append(StructField("keyword_%d"%(i), StringType(), False))
        fields.append(StructField("keyword_%d_score"%(i), FloatType(), False))

    # populate concpet fields
    for i in range(num_keywords):
        fields.append(StructField("concept_%d"%(i), StringType(), False))
        fields.append(StructField("concept_%d_score"%(i), FloatType(), False))

    # populate other fields
    fields.extend([StructField("sentiment_score", FloatType(), False),
                   StructField("sentiment_label", StringType(), False),
                   StructField("sadness_score", FloatType(), False),
                   StructField("joy_score", FloatType(), False),
                   StructField("fear_score", FloatType(), False),
                   StructField("disgust_score", FloatType(), False),
                   StructField("anger_score", FloatType(), False)])

    return StructType(fields)


"""
"
" return the default STT return type
"
"""
def stt_default_return_type():
    """
    @return: the defined return type
    """
    return StringType()
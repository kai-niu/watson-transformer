from pyspark.sql.types import StringType, FloatType, StructType, StructField, Row
from pyspark.sql import functions as F

"""
"
" return the default NLU return type
"
"""
def nlu_default_return_type(output_col, **params):
    """
    @param::num_keywords: the number of keywords extracted by NLU
    @param::num_concpets: the number of concepts extracted by NLU
    @return: the defined return type and Pandas UDF data type
    """
    fields = []
    features = params["features"]
    num_keywords = features.keywords.limit
    num_concepts = features.concepts.limit
    # populate keyword fields
    for i in range(num_keywords):
        fields.append(StructField("%s_keyword_%d"%(output_col,i), StringType(), True))
        fields.append(StructField("%s_keyword_%d_score"%(output_col,i), FloatType(), True))

    # populate concpet fields
    for i in range(num_concepts):
        fields.append(StructField("%s_concept_%d"%(output_col,i), StringType(), True))
        fields.append(StructField("%s_concept_%d_score"%(output_col,i), FloatType(), True))

    # populate other fields
    fields.extend([StructField("%s_sentiment_score"%(output_col), FloatType(), True),
                   StructField("%s_sentiment_label"%(output_col), StringType(), True),
                   StructField("%s_sadness_score"%(output_col), FloatType(), True),
                   StructField("%s_joy_score"%(output_col), FloatType(), True),
                   StructField("%s_fear_score"%(output_col), FloatType(), True),
                   StructField("%s_disgust_score"%(output_col), FloatType(), True),
                   StructField("%s_anger_score"%(output_col), FloatType(), True)])

    return StructType(fields)


"""
"
" return the default STT return type
"
"""
def stt_default_return_type(output_col, **params):
    """
    @return: the defined return type and Pandas UDF data type
    """
    return StructField(output_col, StringType(), True)
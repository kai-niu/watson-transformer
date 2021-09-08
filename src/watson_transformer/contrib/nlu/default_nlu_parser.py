import json
import numbers
from watson_transformer.contrib.response_base import ResponseBase
from pyspark.sql.types import StringType, FloatType, StructType, StructField, Row
from pyspark.sql import functions as F


"""
"
" default STT output interpreter which assume one alternative transcript and no speaker detection
"
"""

class DefaultNLUParser(ResponseBase):

    def __init__(self, keywords_limit, concepts_limit):
        """
        @param::keywords_limit: the max number of keywords extracted
        @param::concepts_limit: the max number of concepts extracted
        @return: none
        """
        super(DefaultNLUParser, self).__init__()
        if not isinstance(keywords_limit, numbers.Number):
            raise ValueError('> DefaultNLUParser: keywords_limit must be numeric.')
        if not isinstance(concepts_limit, numbers.Number):
            raise ValueError('> DefaultNLUParser: concepts_limit must be numeric.')
        if keywords_limit <= 0:
            raise ValueError('> DefaultNLUParser: keywords_limit must be greater than 0.')
        if concepts_limit <= 0:
            raise ValueError('> DefaultNLUParser: concepts_limit must be greater than 0.')
        self.keywords_limit = keywords_limit
        self.concepts_limit = concepts_limit

    """
    "
    " default NLU output formatter which parse keywords, concepts, sentiment and emotion
    "
    """
    def __call__(self, json_dumps):
        """
        @param::output: the output json object from STT
        @return:the transcript join by period in string format
        """
        data = {}
        valid_json_dumps = True
        try:
            json_data = json.loads(json_dumps)
        except:
            valid_json_dumps = False

        if valid_json_dumps and json_data:
            # extract keyword data
            if "keywords" in json_data:
                for i in range(self.keywords_limit):
                    if i < len(json_data["keywords"]):
                        kw = json_data["keywords"][i]
                        data['keyword_%d'%(i)] = kw['text']
                        data['keyword_%d_score'%(i)] = kw['relevance']
                    else: # when less keywords extracted than limit
                        data['keyword_%d'%(i)] = None
                        data['keyword_%d_score'%(i)] = None
            else:
                for i in range(self.keywords_limit):
                    data['keyword_%d'%(i)] = None
                    data['keyword_%d_score'%(i)] = None

            # extract concept
            if "concepts" in json_data:
                for i in range(self.concepts_limit):
                    if i < len(json_data["concepts"]):
                        concept = json_data["concepts"][i]
                        data['concept_%d'%(i)] = concept['text']
                        data['concept_%d_score'%(i)] = concept['relevance']
                    else: # when less concept extracted than limit
                        data['concept_%d'%(i)] = None
                        data['concept_%d_score'%(i)] = None
            else:
                for i in range(self.concepts_limit):
                    data['concept_%d'%(i)] = None
                    data['concept_%d_score'%(i)] = None

            # extract sentiment
            if "sentiment" in json_data:
                data["sentiment_score"] = json_data["sentiment"]["document"]["score"]
                data["sentiment_label"] = json_data["sentiment"]["document"]["label"]
            else:
                data["sentiment_score"] = None
                data["sentiment_label"] = None

            # extract "emotion"
            if "emotion" in json_data:
                data["sadness_score"] = json_data["emotion"]["document"]["emotion"]["sadness"]
                data["joy_score"] = json_data["emotion"]["document"]["emotion"]["joy"]
                data["fear_score"] = json_data["emotion"]["document"]["emotion"]["fear"]
                data["disgust_score"] = json_data["emotion"]["document"]["emotion"]["disgust"]
                data["anger_score"] = json_data["emotion"]["document"]["emotion"]["anger"]
            else:
                data["sadness_score"] = None
                data["joy_score"] = None
                data["fear_score"] = None
                data["disgust_score"] = None
                data["anger_score"] = None
        
        # case 1: json dumps is invalid
        # case 2: json dumps is valid but json data is invalid for the parser
        if not data:
            # extract keyword data
            for i in range(self.keywords_limit):
                data['keyword_%d'%(i)] = None
                data['keyword_%d_score'%(i)] = None
            # extract concept
            for i in range(self.concepts_limit):
                data['concept_%d'%(i)] = None
                data['concept_%d_score'%(i)] = None
            # extract sentiment
            data["sentiment_score"] = None
            data["sentiment_label"] = None
            # extract "emotion"
            data["sadness_score"] = None
            data["joy_score"] = None
            data["fear_score"] = None
            data["disgust_score"] = None
            data["anger_score"] = None
            
        return Row(**data)

    """
    "
    " return the default NLU return type
    "
    """
    def get_return_type(self):
        """
        @param::num_keywords: the number of keywords extracted by NLU
        @param::num_concpets: the number of concepts extracted by NLU
        @return: the defined return type and Pandas UDF data type
        """
        fields = []
        # populate keyword fields
        for i in range(self.keywords_limit):
            fields.append(StructField("keyword_%d"%(i), StringType(), True))
            fields.append(StructField("keyword_%d_score"%(i), FloatType(), True))

        # populate concpet fields
        for i in range(self.concepts_limit):
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

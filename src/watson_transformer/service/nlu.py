"""
"
" IBM NLU Service Executable Class
" docu: https://cloud.ibm.com/apidocs/natural-language-understanding?code=python#analyze-text
"
"""

import json
from pyspark import keyword_only
from ibm_watson import NaturalLanguageUnderstandingV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_watson.natural_language_understanding_v1 import Features, KeywordsOptions, ConceptsOptions, SentimentOptions, EmotionOptions

class NLU():
    
    @keyword_only
    def __init__(self, token, endpoint, parser, return_type, **params):
        """
        @param::token: the IBM NLU API access token
        @param::endpoint: the endpoint url for the NLU API
        @param::paser: the object parse NLU API response
        @param::return_type: executable object return the return_type of NLU __call__
        @param::params: the kv params passing to underlying NaturalLanguageUnderstandingV1 constructor
        @return: the output parsed by parser object
        """
        self.token = token
        self.endpoint = endpoint
        self.parser = parser
        self.params = params
        self.return_type = return_type
        
    def __call__(self, text):
        """
        @param::text: the text to perform NLU
        @return: the output formatted by formatter object
        """

        # init nlu client
        authenticator = IAMAuthenticator(self.token)
        nlu = NaturalLanguageUnderstandingV1( version='2019-07-12',authenticator=authenticator)
        nlu.set_service_url(self.endpoint)

        response = nlu.analyze(text = text,
                               **self.params).get_result()
        return self.parser(response, **self.params)
    
    def get_return_type(self):
        return_type = self.return_type(**self.params)
        return return_type
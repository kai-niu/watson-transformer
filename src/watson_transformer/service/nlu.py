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
from ibm_cloud_sdk_core import ApiException
from ibm_watson.natural_language_understanding_v1 import Features, KeywordsOptions, ConceptsOptions, SentimentOptions, EmotionOptions
from pyspark.sql.types import StringType
from watson_transformer.service.service_base import ServiceBase

class NLU(ServiceBase):
    
    @keyword_only
    def __init__(self, token, endpoint, strict_mode=True, **params):
        """
        @param::token: the IBM NLU API access token
        @param::endpoint: the endpoint url for the NLU API
        @param::params: the kv params passing to underlying NaturalLanguageUnderstandingV1 constructor
        @return: the output parsed by parser object
        """
        super(NLU, self).__init__(strict_mode)
        self.token = token
        self.endpoint = endpoint
        self.params = params
        
    def __call__(self, text):
        """
        @param::text: the text to perform NLU
        @return: the output formatted by formatter object
        """

        if text:
            # init nlu client
            authenticator = IAMAuthenticator(self.token)
            nlu = NaturalLanguageUnderstandingV1( version='2019-07-12',authenticator=authenticator)
            nlu.set_service_url(self.endpoint)
            
            try:
                response = nlu.analyze(text = text, **self.params).get_result()
            except ApiException:
                response = None # better to log such execeptions separately
            except Exception:
                if self.strict_mode:
                    raise RuntimeError("*** runtime error caused by input: '%s'"%(text))
                else:
                    response = None
            
            return json.dumps(response) if response else None
        else:
            return None
    
    def get_return_type(self):
        return StringType()

    def get_new_client(self):
        return NLU(token = self.token, 
                   endpoint = self.endpoint,
                   strict_mode = self.strict_mode,
                   **self.params)
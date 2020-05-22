"""
"
" IBM STT Service Executable Class
" docu: https://cloud.ibm.com/apidocs/speech-to-text
"
"""

import json
from pyspark import keyword_only
from ibm_watson import SpeechToTextV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from pyspark.sql.types import StringType
from watson_transformer.service.service_base import ServiceBase

class STT(ServiceBase):
    
    @keyword_only
    def __init__(self, token, endpoint, reader, **params):
        """
        @param::token: the IBM STT API access token
        @param::endpoint: the endpoint url for the STT API
        @param::reader: the object read audio stream using audio file name/id
        @param::params: the kv params passing to underlying SpeechToTextV1 constructor
        @return: the output formatted by formatter executable
        """
        super(STT, self).__init__()
        self.token = token
        self.endpoint = endpoint
        self.reader = reader
        self.params = params

    def __call__(self, audio_file):
        """
        @param::audio_file: the audio filename/id for reader to retrieve the audio stream
        @return: the output formatted by formatter object
        """
        # load asset
        audio_stream = self.reader(audio_file)

        # init stt client
        authenticator = IAMAuthenticator(self.token)
        stt = SpeechToTextV1(authenticator=authenticator)
        stt.set_service_url(self.endpoint)
        response = stt.recognize(
            audio=audio_stream,
            **self.params
        ).get_result()
        return json.dumps(response)
    
    def get_return_type(self):
        """
        @param::output_col: output column name
        @return: the output type struct
        """
        return StringType()

    def get_new_client(self):
        return STT(token = self.token, 
                   endpoint = self.endpoint,
                   reader = self.reader,
                   **self.params)
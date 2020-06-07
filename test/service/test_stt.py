"""
"
" Test IBM STT Service
"
"""

import json
import pytest
from unittest import mock
from watson_transformer.service.stt import STT


class TestSTT():

    def test_nlu_init(self):
        # arange
        token = 'foo'
        endpoint = 'http://www.ibm.com'
        reader = lambda x: "foo is speaking to bar"
        feature = {'foo':'bar'}
        # action
        stt = STT(token = token, 
                  endpoint = endpoint,
                  reader = reader,
                  features = feature)
        # assert
        assert stt.token == token
        assert stt.endpoint == endpoint
        assert 'features' in stt.params
        assert 'foo' in stt.params['features']
        assert stt.params['features']['foo']  == 'bar'
        assert stt.reader('foo') == reader('foo')
 
    def test_service_callable_valid_input(self):
        # patch where the class is located.
        with mock.patch('watson_transformer.service.nlu.IAMAuthenticator'):
            with mock.patch('watson_transformer.service.stt.SpeechToTextV1') as mock_stt_api:
                # arrange
                mock_stt_api.return_value.recognize.return_value.get_result.return_value = {'value':'mock response'} # mock stt.recognize().get_result()
                stt = STT(token = 'foo', 
                          endpoint='http://www.foo.com/bar', 
                          reader = lambda x: "foo is speaking to bar.",
                          features='foo')
                # act
                response = stt('sample.wav')
                # assert
                data = json.loads(response)
                assert 'value' in data
                assert data['value'] == 'mock response'

    def test_service_callable_invalid_input(self):
        # patch where the class is located.
        with mock.patch('watson_transformer.service.nlu.IAMAuthenticator'):
            with mock.patch('watson_transformer.service.stt.SpeechToTextV1') as mock_stt_api:
                # arrange
                mock_stt_api.return_value.recognize.return_value.get_result.return_value = {'value':'mock response'} # mock stt.recognize().get_result()
                stt = STT(token = 'foo', 
                          endpoint='http://www.foo.com/bar', 
                          reader = lambda x: "foo is speaking to bar.",
                          features='foo')
                for value in [None, '']:
                    # act
                    response = stt(value)
                    # assert
                    assert response == None
    
    def test_service_callable_invalid_request(self):
        # patch where the class is located.
        with mock.patch('watson_transformer.service.nlu.IAMAuthenticator'):
             with mock.patch('watson_transformer.service.stt.SpeechToTextV1') as mock_stt_api:
                # arrange
                mock_stt_api.return_value.recognize.side_effect = Exception('STT API raise exception.') # mock stt.recognize().get_result()
                stt = STT(token = 'foo', 
                          endpoint='http://www.foo.com/bar', 
                          reader = lambda x: "foo is speaking to bar.",
                          features='foo')
                for value in ['none_exist.wav', 'invalid.wav']:
                    # act
                    with pytest.raises(Exception) as exinfo:
                        response = stt(value)
                    # assert
                    assert 'STT' in str(exinfo.value)

    def test_get_new_client(self):
        # arrange
        stt = STT(token = 'foo', 
                          endpoint='http://www.foo.com/bar', 
                          reader = lambda x: "foo is speaking to bar.",
                          features='foo')
        # action
        new_stt = stt.get_new_client()
        # assert
        assert stt.token == new_stt.token
        assert stt.endpoint == new_stt.endpoint
        assert 'features' in new_stt.params
        assert new_stt.params['features'] == 'foo'
        assert stt != new_stt

    def test_reader_raise_exception(self):
        # arrange
        reader = mock.MagicMock(side_effect=Exception('failed to read the file.'))
        stt = STT(token = 'foo', 
                  endpoint='http://www.foo.com/bar', 
                  reader = reader,
                  features='foo')
        # act
        with pytest.raises(Exception) as exinfo:
            stt('sample.wav')
        # assert
        assert 'failed to read the file.' in str(exinfo.value)








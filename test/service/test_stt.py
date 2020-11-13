"""
"
" Test IBM STT Service
"
"""

import json
import pytest
from unittest import mock
from ibm_cloud_sdk_core.api_exception import ApiException
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
    
    def test_service_callable_raise_none_api_exception_strict_mode_on(self):
        # patch where the class is located.
        with mock.patch('watson_transformer.service.nlu.IAMAuthenticator'):
             with mock.patch('watson_transformer.service.stt.SpeechToTextV1') as mock_stt_api:
                # arrange
                mock_stt_api.return_value.recognize.side_effect = Exception('STT API raise exception.') # mock stt.recognize().get_result()
                stt = STT(token = 'foo', 
                          endpoint='http://www.foo.com/bar', 
                          reader = lambda x: "foo is speaking to bar.",
                          strict_mode = True,
                          features='foo')
                for value in ['none_exist.wav', 'invalid.wav']:
                    # act
                    with pytest.raises(Exception) as exinfo:
                        response = stt(value)
                    # assert
                    assert value in str(exinfo.value)
                    assert stt.strict_mode == True
    
    def test_service_callable_raise_none_api_exception_strict_mode_off(self):
        # patch where the class is located.
        with mock.patch('watson_transformer.service.nlu.IAMAuthenticator'):
             with mock.patch('watson_transformer.service.stt.SpeechToTextV1') as mock_stt_api:
                # arrange
                mock_stt_api.return_value.recognize.side_effect = Exception('raise general exception.') # mock stt.recognize().get_result()
                stt = STT(token = 'foo', 
                          endpoint='http://www.foo.com/bar', 
                          reader = lambda x: "foo is speaking to bar.",
                          strict_mode = False,
                          features='foo')
                for value in ['none_exist.wav', 'invalid.wav']:
                    # act
                    response = stt(value)
                    # assert
                    assert 'error_message' in response
                    assert 'raise general exception.' in response
                    assert stt.strict_mode == False

    def test_service_callable_raise_api_exception_strict_mode_on(self):
        # patch where the class is located.
        with mock.patch('watson_transformer.service.nlu.IAMAuthenticator'):
             with mock.patch('watson_transformer.service.stt.SpeechToTextV1') as mock_stt_api:
                # arrange
                mock_stt_api.return_value.recognize.side_effect = ApiException('STT API raise exception.') # mock stt.recognize().get_result()
                stt = STT(token = 'foo', 
                          endpoint='http://www.foo.com/bar', 
                          reader = lambda x: "foo is speaking to bar.",
                          strict_mode = True,
                          features='foo')
                for value in ['none_exist.wav', 'invalid.wav']:
                    # act
                    response = stt(value)
                    # assert
                    assert 'api_error_message' in response
                    assert 'STT API raise exception.' in response
                    assert stt.strict_mode == True

    def test_service_callable_raise_api_exception_strict_mode_on(self):
        # patch where the class is located.
        with mock.patch('watson_transformer.service.nlu.IAMAuthenticator'):
             with mock.patch('watson_transformer.service.stt.SpeechToTextV1') as mock_stt_api:
                # arrange
                mock_stt_api.return_value.recognize.side_effect = ApiException('STT API raise exception.') # mock stt.recognize().get_result()
                stt = STT(token = 'foo', 
                          endpoint='http://www.foo.com/bar', 
                          reader = lambda x: "foo is speaking to bar.",
                          strict_mode = False,
                          features='foo')
                for value in ['none_exist.wav', 'invalid.wav']:
                    # act
                    response = stt(value)
                    # assert
                    assert 'api_error_message' in  response
                    assert 'STT API raise exception.' in response
                    assert stt.strict_mode == False

    def test_get_new_client(self):
        # arrange
        stt = STT(token = 'foo', 
                          endpoint='http://www.foo.com/bar', 
                          reader = lambda x: "foo is speaking to bar.",
                          strict_mode=False,
                          features='foo')
        # action
        new_stt = stt.get_new_client()
        # assert
        assert stt.token == new_stt.token
        assert stt.endpoint == new_stt.endpoint
        assert 'features' in new_stt.params
        assert new_stt.params['features'] == 'foo'
        assert stt != new_stt
        assert stt.strict_mode == stt.strict_mode
        assert stt.strict_mode == False

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

    def test_reader_return_none_stream(self):
        # arrange
        reader = mock.MagicMock(return_value = None)
        stt = STT(token = 'foo', 
                  endpoint='http://www.foo.com/bar', 
                  reader = reader,
                  features='foo')
        # act
        response = stt('sample.wav')
        # assert
        assert response == None






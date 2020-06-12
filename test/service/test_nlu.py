"""
"
" Test NLU Service
"
"""

import json
import pytest
from unittest import mock
from ibm_cloud_sdk_core import ApiException
from watson_transformer.service.nlu import NLU


class TestNLU():

    def test_nlu_init(self):
        # arange
        token = 'foo'
        endpoint = 'http://www.ibm.com'
        feature = {'foo':'bar'}
        # action
        nlu = NLU(token = token, 
                  endpoint = endpoint, 
                  features = feature)
        # assert
        assert nlu.token == token
        assert nlu.endpoint == endpoint
        assert 'features' in nlu.params
        assert 'foo' in nlu.params['features']
        assert nlu.params['features']['foo']  == 'bar'
 
    def test_service_callable_valid_input(self):
        # patch where the class is located.
        with mock.patch('watson_transformer.service.nlu.IAMAuthenticator'):
            with mock.patch('watson_transformer.service.nlu.NaturalLanguageUnderstandingV1') as mock_nlu_api:
                # arrange
                mock_nlu_api.return_value.analyze.return_value.get_result.return_value = {'value':'mock response'} # mock nlu.analyze().get_result()
                nlu = NLU(token = 'foo', 
                            endpoint='http://www.foo.com/bar', 
                            features='foo')
                # act
                response = nlu('I love this game.')
                # assert
                data = json.loads(response)
                assert 'value' in data
                assert data['value'] == 'mock response'

    def test_service_callable_invalid_input(self):
        # patch where the class is located.
        with mock.patch('watson_transformer.service.nlu.IAMAuthenticator'):
            with mock.patch('watson_transformer.service.nlu.NaturalLanguageUnderstandingV1') as mock_nlu_api:
                # arrange
                mock_nlu_api.return_value.analyze.return_value.get_result.return_value = {'value':'mock response'} # mock nlu.analyze().get_result()
                nlu = NLU(token = 'foo', 
                            endpoint='http://www.foo.com/bar', 
                            features='foo')
                for value in [None, '']:
                    # act
                    response = nlu(value)
                    # assert
                    assert response == None
    
    def test_service_callable_raise_none_api_exception(self):
        # patch where the class is located.
        with mock.patch('watson_transformer.service.nlu.IAMAuthenticator'):
            with mock.patch('watson_transformer.service.nlu.NaturalLanguageUnderstandingV1') as mock_nlu_api:
                # arrange
                mock_nlu_api.return_value.analyze.side_effect = Exception('NLU API raise exception.') # mock nlu.analyze()
                nlu = NLU(token = 'foo', 
                            endpoint='http://www.foo.com/bar', 
                            features='foo')
                for value in ['      ', '    _', 'one two']:
                    # act
                    with pytest.raises(Exception) as exinfo:
                        response = nlu(value)
                    # assert
                    assert value in str(exinfo.value)

    def test_service_callable_raise_api_exception(self):
        # patch where the class is located.
        with mock.patch('watson_transformer.service.nlu.IAMAuthenticator'):
            with mock.patch('watson_transformer.service.nlu.NaturalLanguageUnderstandingV1') as mock_nlu_api:
                # arrange
                mock_nlu_api.return_value.analyze.side_effect = ApiException('NLU API raise exception.') # mock nlu.analyze()
                nlu = NLU(token = 'foo', 
                            endpoint='http://www.foo.com/bar', 
                            features='foo')
                for value in ['      ', '    _', 'one two']:
                    # act
                    response = nlu(value)
                    # assert
                    assert response == None

    def test_get_new_client(self):
        # arrange
        nlu = NLU(token = 'foo', 
                    endpoint='http://www.foo.com/bar', 
                    features='foo')
        # action
        new_nlu = nlu.get_new_client()
        # assert
        assert nlu.token == new_nlu.token
        assert nlu.endpoint == new_nlu.endpoint
        assert 'features' in new_nlu.params
        assert new_nlu.params['features'] == 'foo'
        assert nlu != new_nlu
    








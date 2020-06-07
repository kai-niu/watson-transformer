"""
"
" test default STT response type
"
"""
import json
import pytest
from unittest import mock
from watson_transformer.contrib.stt.default_stt_parser import DefaultSTTParser

@pytest.fixture(scope='function')
def mock_input(request):
    response = {
        'results':[
            {
            'alternatives':[
                    {'transcript':'foo'},
                    {'transcript':'bar'}
                ]
            },
            {
            'alternatives':[
                    {'transcript':'joe'},
                    {'transcript':'joy'}
                ]
            }
        ]
    }
    return json.dumps(response)

class TestDefaultSTTParser():

    def test_invalid_input(self):
        # arrange
        parser = DefaultSTTParser()
        invalide_json = json.dumps({'foo':'bar'})
        for value in [None, invalide_json, 1, 'null', []]:
            # act
            data = parser(value)
            # assert
            assert data == None
    
    def test_valid_input(self, mock_input):
        # arrange
        parser = DefaultSTTParser()
        input_data = mock_input
        # act
        data = parser(input_data)
        # assert
        assert data == 'foo. joe.'
        assert 'bar' not in data
        assert 'joy' not in data


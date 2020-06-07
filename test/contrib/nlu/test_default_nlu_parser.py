"""
"
" test default STT response type
"
"""
import json
import pytest
from unittest import mock
from pyspark.sql.types import StringType, FloatType, StructType, StructField, Row
from watson_transformer.contrib.nlu.default_nlu_parser import DefaultNLUParser

@pytest.fixture(scope='function')
def mock_input(request):
    response = {
        'keywords':[
            {
            'text':'foo',
            'relevance':1.0
            },
            {
            'text':'bar',
            'relevance':0.5
            },
        ],
        'concepts':[
            {
            'text':'Bayes Rule',
            'relevance':1.0
            },
            {
            'text':'Markov Chain',
            'relevance':0.5
            },
        ],
        'sentiment':{
            'document':{'score':.8, 'label':'positive'}
        },
        'emotion':{
            'document':{
                'emotion':{
                    'sadness':.1,
                    'joy':.5,
                    'fear':.2,
                    'disgust':.3,
                    'anger':.4
                }
            }
        }

    }
    return json.dumps(response)


class TestDefaultNLUParser():

    def test_init_parser_valid_params(self):
        # act
        parser = DefaultNLUParser(keywords_limit=2, concepts_limit=2)
        # assert
        assert parser.keywords_limit == 2
        assert parser.concepts_limit == 2

    def test_init_parser_invalid_params(self):
        # arrange
        invalid_values = [-1, "1", None, []]
        # act
        parser = DefaultNLUParser(keywords_limit=2, concepts_limit=2)
        # assert
        for value in invalid_values:
            with pytest.raises(ValueError) as exinfo:
                DefaultNLUParser(keywords_limit=value, concepts_limit=value)    
            assert 'DefaultNLUParser' in str(exinfo.value)

    def test_invalid_input(self):
        # arrange
        parser = DefaultNLUParser(keywords_limit=2, concepts_limit=2)
        invalide_json = json.dumps({'foo':'bar'})
        for value in [None, 1, 'null', [], invalide_json]:
            # act
            data = parser(value)
            # assert
            data_dict = data.asDict(True)
            assert 'sadness_score' in data_dict
            assert data_dict['sadness_score'] == None
            assert 'joy_score' in data_dict
            assert data_dict['joy_score'] == None
            assert 'fear_score' in data_dict
            assert data_dict['fear_score'] == None
            assert 'disgust_score' in data_dict
            assert data_dict['disgust_score'] == None
            assert 'anger_score' in data_dict
            assert data_dict['anger_score'] == None
            assert 'concept_0' in data_dict
            assert data_dict['concept_0'] == None
            assert data_dict['concept_0_score'] == None
            assert 'concept_1' in data_dict
            assert data_dict['concept_1'] == None
            assert data_dict['concept_1_score'] == None
            assert 'concept_2' not in data_dict
            assert 'keyword_0' in data_dict
            assert data_dict['keyword_0'] == None
            assert data_dict['keyword_0_score'] == None
            assert 'keyword_1' in data_dict
            assert data_dict['keyword_1'] == None
            assert data_dict['keyword_1_score'] == None 
            assert 'keyword_2' not in data_dict
    
    def test_valid_input(self, mock_input):
        # arrange
        parser = DefaultNLUParser(keywords_limit=2, concepts_limit=2)
        # act
        data = parser(mock_input)
        # assert
        data_dict = data.asDict(True)
        assert 'sadness_score' in data_dict
        assert data_dict['sadness_score'] == .1
        assert 'joy_score' in data_dict
        assert data_dict['joy_score'] == .5
        assert 'fear_score' in data_dict
        assert data_dict['fear_score'] == .2
        assert 'disgust_score' in data_dict
        assert data_dict['disgust_score'] == .3
        assert 'anger_score' in data_dict
        assert data_dict['anger_score'] == .4
        assert 'concept_0' in data_dict
        assert data_dict['concept_0'] == 'Bayes Rule'
        assert data_dict['concept_0_score'] == 1.0
        assert 'concept_1' in data_dict
        assert data_dict['concept_1'] == 'Markov Chain'
        assert data_dict['concept_1_score'] == .5
        assert 'concept_2' not in data_dict
        assert 'keyword_0' in data_dict
        assert data_dict['keyword_0'] == 'foo'
        assert data_dict['keyword_0_score'] == 1.0
        assert 'keyword_1' in data_dict
        assert data_dict['keyword_1'] == 'bar'
        assert data_dict['keyword_1_score'] == .5
        assert 'keyword_2' not in data_dict

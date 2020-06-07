"""
"
" test watson service transformer class
"
"""
import pytest
from unittest import mock
from watson_transformer.watson_service_transformer import WatsonServiceTransformer



@pytest.fixture(scope='function')
def mock_service(request):
    # mock the service
    service = mock.MagicMock(return_value='foo response')
    return lambda : service


class TestWatsonServiceTransformer():

    def test_init_input_col_with__valid_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        column_name = "input_column_name"
        # act
        for column_name in ["foo", "foo bar", "foo_bar", "   _"]:
            transformer = WatsonServiceTransformer(inputCol=column_name, 
                                                   outputCol='output_column',
                                                   vectorization=True,
                                                   max_workers = 10,
                                                   service=mocked_service)
            # assert
            assert transformer.getInputCol() == column_name

    def test_init_input_col_with_invalid_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        column_name = None
        # act
        for column_name in [None, "", " ", "    "]:
            with pytest.raises(ValueError) as exception:
                _ = WatsonServiceTransformer(inputCol=column_name, 
                                            outputCol='output_column',
                                            vectorization=True,
                                            max_workers = 10,
                                            service=mocked_service)
            # assert
            assert "input column name" in str(exception.value)

    def test_init_output_col_with_valid_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        column_name = "input_column_name"
        # act
        for column_name in ["foo", "foo bar", "foo_bar", "   _"]:
            transformer =  WatsonServiceTransformer(inputCol='input column', 
                                                    outputCol=column_name,
                                                    vectorization=True,
                                                    max_workers = 10,
                                                    service=mocked_service)
            # assert
            assert transformer.getOutputCol() == column_name

    def test_init_output_col_with_invalid_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        column_name = None
        # act
        for column_name in [None, "", " ", "    "]:
            with pytest.raises(ValueError) as exception:
                WatsonServiceTransformer(inputCol="input column", 
                                         outputCol=column_name,
                                         vectorization=True,
                                         max_workers = 10,
                                         service=mocked_service)
            # assert
            assert "output column name" in str(exception.value)

    def test_init_max_workers_with_valid_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        max_workers = [1,2,3,4,5]
        expect_max_workers = [1,2,3,4,5]
        # act
        for i in range(len(max_workers)):
            transformer =  WatsonServiceTransformer(inputCol='input column', 
                                                    outputCol='output column',
                                                    vectorization=True,
                                                    max_workers = max_workers[i],
                                                    service=mocked_service)
            # assert
            assert transformer.getMax_workers() == expect_max_workers[i]

    def test_init_max_workers_with_default_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        expected_default_value = 5
        # act
        transformer =  WatsonServiceTransformer(inputCol='input column', 
                                                outputCol='output column',
                                                vectorization=True,
                                                service=mocked_service)
        # assert
        assert transformer.getMax_workers() == expected_default_value

    def test_init_max_workers_with_invalid_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        max_workers = [None, "1", -1, 0, -100]
        # act
        for i in range(len(max_workers)):
            with pytest.raises(ValueError) as exinfo:
                WatsonServiceTransformer(inputCol='input column', 
                                         outputCol='output column',
                                         vectorization=True,
                                         max_workers = max_workers[i],
                                         service=mocked_service)
            # assert
            assert "maximum workers" in str(exinfo.value)
    
    def test_init_vectorization_with_valid_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        vectorizations = [0, 1, True, False]
        expect_vectorizations = [False, True, True, False]
        # act
        for i in range(len(vectorizations)):
            transformer =  WatsonServiceTransformer(inputCol='input column', 
                                                    outputCol='output column',
                                                    vectorization=vectorizations[i],
                                                    max_workers = 10,
                                                    service=mocked_service)
            # assert
            assert transformer.getVectorization() == expect_vectorizations[i]

    def test_init_vectorization_with_default_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        # act
        transformer =  WatsonServiceTransformer(inputCol='input column', 
                                                outputCol='output column',
                                                service=mocked_service)
        # assert
        assert transformer.getVectorization() == False
    
    def test_init_valid_service(self):
        # arrange
        mocked_service = lambda x: x+1
        # act
        transformer =  WatsonServiceTransformer(inputCol='input column', 
                                                outputCol='output column',
                                                service=mocked_service)
        # assert
        provided_service = transformer.getService() 
        assert provided_service(10) == 11


    def test_init_none_callable_service(self):
        # arrange
        values = [None, 12, "12"]
        # act
        for i in range(len(values)):
            with pytest.raises(ValueError) as exinfo:
                WatsonServiceTransformer(inputCol='input column', 
                                         outputCol='output column',
                                         service=values[i])
            # assert
            assert "service instance" in str(exinfo.value) and "callable" in str(exinfo.value)
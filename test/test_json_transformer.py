"""
"
" test watson service transformer class
"
"""
import pytest
from unittest import mock
from watson_transformer.json_transformer import JSONTransformer



@pytest.fixture(scope='function')
def mock_service(request):
    # mock the service
    service = mock.MagicMock(return_value='foo response')
    return lambda : service


class TestJSONTransformer():

    def test_init_input_col_with__valid_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        column_name = "input_column_name"
        # act
        for column_name in ["foo", "foo bar", "foo_bar", "   _"]:
            transformer = JSONTransformer(inputCol=column_name, 
                                          outputCol='output_column',
                                          removeInputCol=True,
                                          parser=mocked_service)
            # assert
            assert transformer.getInputCol() == column_name

    def test_init_input_col_with_invalid_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        column_name = None
        # act
        for column_name in [None, "", " ", "    "]:
            with pytest.raises(ValueError) as exception:
                transformer = JSONTransformer(inputCol=column_name, 
                                          outputCol='output_column',
                                          removeInputCol=True,
                                          parser=mocked_service)
            # assert
            assert "input column name" in str(exception.value)

    def test_init_output_col_with_valid_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        column_name = "input_column_name"
        # act
        for column_name in ["foo", "foo bar", "foo_bar", "   _"]:
            transformer = JSONTransformer(inputCol=column_name, 
                                          outputCol='output_column',
                                          removeInputCol=True,
                                          parser=mocked_service)

    def test_init_output_col_with_invalid_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        column_name = None
        # act
        for column_name in [None, "", " ", "    "]:
            with pytest.raises(ValueError) as exception:
                JSONTransformer(inputCol='input_column', 
                                outputCol=column_name,
                                removeInputCol=True,
                                parser=mocked_service)
            # assert
            assert "output column name" in str(exception.value)
    
    def test_init_remove_input_column_with_valid_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        valid_values = [0, 1, True, False]
        expect_values = [False, True, True, False]
        # act
        for i in range(len(valid_values)):
            transformer = JSONTransformer(inputCol='input_column', 
                                          outputCol='output_column',
                                          removeInputCol=valid_values[i],
                                          parser=mocked_service)
            # assert
            assert transformer.getRemoveInputCol() == expect_values[i]

    def test_init_remove_input_column_with_default_value(self, mock_service):
        # arrange
        mocked_service = mock_service()
        # act
        transformer =  JSONTransformer(inputCol='input column', 
                                        outputCol='output column',
                                        parser=mocked_service)
        # assert
        assert transformer.getRemoveInputCol() == False
    
    def test_init_valid_service(self):
        # arrange
        mocked_service = lambda x: x+1
        # act
        transformer =  JSONTransformer(inputCol='input column', 
                                        outputCol='output column',
                                        parser=mocked_service)
        # assert
        provided_service = transformer.getParser() 
        assert provided_service(10) == 11


    def test_init_none_callable_service(self):
        # arrange
        invalid_services = [None, 12, "12"]
        # act
        for i in range(len(invalid_services)):
            with pytest.raises(ValueError) as exinfo:
                JSONTransformer(inputCol='input column', 
                                outputCol='output column',
                                parser=invalid_services[i])
            # assert
            assert "parser instance" in str(exinfo.value) and "callable" in str(exinfo.value)
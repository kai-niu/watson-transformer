"""
"
" test response base class
"
"""
import pytest
from unittest import mock
from watson_transformer.contrib.response_base import ResponseBase

class TestResponseBase():

    def test_callable_not_implemented(self):
        # arrange
        res = ResponseBase()
        # act
        with pytest.raises(NotImplementedError) as exinfo:
            res(None)
        # assert
        assert '__call__' in str(exinfo.value)

    def test_get_return_type_implemented(self):
        # arrange
        res = ResponseBase()
        # act
        with pytest.raises(NotImplementedError) as exinfo:
            res.get_return_type()
        # assert
        assert 'get_return_type' in str(exinfo.value)
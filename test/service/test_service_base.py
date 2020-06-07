"""
"
" test API service base class
"
"""
import pytest
from unittest import mock
from watson_transformer.service.service_base import ServiceBase

@pytest.fixture(scope='function')
def mock_base_service(request):
    # mock the service
    return ServiceBase()

class TestServiceBase():

    def test_callable(self, mock_base_service):
        # arrange
        service = mock_base_service
        # act
        with pytest.raises(NotImplementedError) as exinfo:
            service(None)
        # assert
        assert '__call__' in str(exinfo.value)

    def test_get_return_type(self, mock_base_service):
        # arrange
        service = mock_base_service
        # act
        with pytest.raises(NotImplementedError) as exinfo:
            service.get_return_type(None)
        # assert
        assert 'get_return_type' in str(exinfo.value)

    def test_get_new_client(self, mock_base_service):
        # arrange
        service = mock_base_service
        # act
        with pytest.raises(NotImplementedError) as exinfo:
            service.get_new_client()
        # assert
        assert 'get_new_client' in str(exinfo.value)




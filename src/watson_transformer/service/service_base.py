"""
"
" define the contract/interfact 
"
"""

class ServiceBase():
    def __init__(self, strict_mode=True):
        self.strict_mode = strict_mode

    def __call__(self, data):
        raise NotImplementedError('> service class __call__ method is not implemented.')

    def get_return_type(self, data):
        raise NotImplementedError('> service class get_return_type method is not implemented.')

    def get_new_client(self):
        raise NotImplementedError('> service class get_new_client method is not implemented.')


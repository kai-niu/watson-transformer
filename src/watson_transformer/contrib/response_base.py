"""
"
" response base class
"
"""
class ResponseBase():

    def __call__(self, json_data):
        raise NotImplementedError('> ResponseBase class: __call__ method is not implemented.')

    def get_return_type(self):
        raise NotImplementedError('> ResponseBase class: get_return_type method is not implemented.')

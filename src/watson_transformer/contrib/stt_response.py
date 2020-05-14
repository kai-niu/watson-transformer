
import json

"""
"
" default STT output interpreter which assume one alternative transcript and no speaker detection
"
"""
def text_parser(output, **params):
    """
    @param::output: the output json object from STT
    @return:the transcript join by period in string format
    """
    transcripts = [doc['alternatives'][0]['transcript'].strip() for doc in output['results']]
    return '. '.join(transcripts) + '.'
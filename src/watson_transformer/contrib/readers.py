import types
from botocore.client import Config
import ibm_boto3
def __iter__(self): return 0

"""
"
" IBM COS reader
" boto3 is not thread safe(https://github.com/boto/botocore/issues/1246)
" create new session in each thread(ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html)
"
"""
def ibm_cos_reader(audio_file, bucket, token, endpoint):
    """
    @param::audio_file: the audio file uid
    @param::bukcet: the bucket name in which the audio file is stored
    @param::token: the API access token for IBM COS service
    @param::endpoint: the URL to access IBM COS service
    @return: the audio stream
    """
    session = ibm_boto3.session.Session() 
    cos_client = session.client(service_name='s3',
                                  ibm_api_key_id=token,
                                  ibm_auth_endpoint="https://iam.ng.bluemix.net/oidc/token",
                                  config=Config(signature_version='oauth'),
                                  endpoint_url=endpoint)
    audio_stream = cos_client.get_object(Bucket=bucket, Key=audio_file)['Body']
    if not hasattr(audio_stream, "__iter__"): audio_stream.__iter__ = types.MethodType( __iter__, audio_stream)
    return audio_stream


"""
"
" Local file reader
"
"""
def local_fs_reader(audio_file):
    """
    @param::audio_file: the full path including filename to the audio file
    @return: the filestream of audio file
    """
    return open(audio_file, "rb")
Tutorial
========

Quick tutorial about how to get started with Waston Transformer.

Install Package
---------------

::

    pip install watson_transformer


Watson Transformer Essential
-----------------------------

There are two steps in general in order to initialize the pySpark Transfromer.
Frist, The relevant service instance which will be wrapped as PySpark transfromer, need to be initialized. 
The following code snippet will initialize an IBM NLU service instance:


.. code-block:: python

    from watson_transformer.service import STT
    from ibm_watson.natural_language_understanding_v1 import Features, KeywordsOptions, ConceptsOptions, SentimentOptions, EmotionOptions

    nlu_service = NLU(token = 'IBM NLU API Token',
                      endpoint = 'https://gateway.watsonplatform.net/natural-language-understanding/api',
                      features = Features(keywords=KeywordsOptions(sentiment=True,emotion=False,limit=5),
                                          concepts=ConceptsOptions(limit=5),
                                          sentiment=SentimentOptions(document=True),
                                          emotion=EmotionOptions(document=True)))

After we initialized the service instance, we are ready to create the pySpark transformer
instance. The following code snippet create the NLU pySpark transformer ready to be used
in pySpark ML pipeline:

.. code-block:: python

    from watson_transformer import WatsonServiceTransformer

    nlu = WatsonServiceTransformer(inputCol='transcript', 
                                   outputCol='nlu_response',
                                   vectorization=True,
                                   max_workers = 20,
                                   service=nlu_service)

The general steps are always create the service instance first then use ``WatsonServiceTransformer``
class to wrap around the service instance.


IBM STT Transformer
--------------------
To create an IBM STT transformer, we will follow the two steps discussed in previous
section.

Init The STT Service instance
++++++++++++++++++++++++++++++

To initialize the IBM STT service instance, the following parameters are required:

* IBM STT API access token
* IBM STT endpoint URL
* Reader function
* Parameters pass down to IBM STT API

The access token and endpoint URL are available from the IBM STT API configuration page.
The Reader function has the following signature:

.. code-block:: python

    def reader_function(audio_filename, [param1=value1, ...])->BufferedIOBase:
        pass

Since the audio files are almost always saved to some form of object storage, the reader
function serve the purpose of reading the audio file from cloud storage. The following
code snippet shows an example of reader function which read audio file from IBM Cloud
Object Storage:

.. code-block:: python

    def ibm_cos_reader(audio_file, maximum_size=200*1024*1024):
        """
        @param:: audio_file: the audio file URL
        @param:: maximum_size: ignore the file greater than this size
        return:: the IO stream of given audio file
        """
        session = ibm_boto3.session.Session() 
        cos_client = session.client(service_name='s3',
                                    ibm_api_key_id=api_key,
                                    ibm_auth_endpoint=auth_endpoint,
                                    config=Config(signature_version='oauth'),
                                    endpoint_url=public_endpoint_url)

        file_obj = cos_client.get_object(Bucket=Bucket, Key=audio_file)
        if int(file_obj['ContentLength']) >= maximum_size:
            return None
        else:
            audio_stream = file_obj['Body']
            if not hasattr(audio_stream, "__iter__"): audio_stream.__iter__ = types.MethodType( __iter__, audio_stream)
            return audio_stream

The reader function serve as an extension point to allow developer to apply logics that
address the specific use case. As long as the reader function comply with the required
signature function, it will be able to work with the IBM STT service instance.

The parameters pass down to IBM STT API is straight copy of list named parameter that
will be provided when call IBM STT service, for detail of how to configure IBM STT service,
the technical document is available **here**.

Once we have all parameter sort out, to create the STT service instance is straight forward: 

.. code-block:: python

    stt_service = STT(token = stt_api_token,
                      endpoint = 'https://stream.watsonplatform.net/speech-to-text/api',
                      reader = ibm_cos_reader,
                      model='en-US_ShortForm_NarrowbandModel',
                      profanity_filter=False,
                      max_alternatives=1,
                      split_transcript_at_phrase_end=False,
                      content_type='audio/wav')

The IBM STT service instance is ready to be wrapped by ``WatsonTransformer`` class.

Init IBM STT Transformer
+++++++++++++++++++++++++

Compare to the IBM STT service instance initialization, It is fairly straightforward
to initialize IBM STT transformer. The following code snippet create an STT transformer
using the service instance we declared in the previous section:

.. code-block:: python

    stt = WatsonServiceTransformer(inputCol='audio_file', 
                                   outputCol='stt_response',
                                   vectorization=True,
                                   max_workers=20,
                                   service=stt_service)

The parameter ``inputCol`` and ``outputCol`` are standard parameter for pySpark
transformer to denote the input and output column name. The ``vectorization`` parameter
indicates whether or not utilizing the pyarrow in memory dataframe. To achieve the
best performance, it should be enable whenever it is possible. To learn more about pyArrow and how
to enable it on your cluster, please refer to this **document**. The parameter ``max_worker`` denotes
the maximum number of threads allowed in the process within each node. If the ``vectorization == False``,
this parameter is ignored. The last paramter ``service`` is the reference pointing to the
STT service instance we created in the last section.
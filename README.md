# Watson Transformer
Watson Transformer solves the problem of consuming IBM Watson API services([STT](https://cloud.ibm.com/apidocs/speech-to-text), [NLU](https://cloud.ibm.com/apidocs/natural-language-understanding), etc.) at scale by wrapping the service calls into the Spark transformer. In this way, The IBM services such as STT and NLU can build into the Spark ML pipeline, along with other transformers and estimators to tackle the big data challenge. 

# Install
```
pip install waston-transformer
```

# Design
As the UML chart illustrates, The Watson Transformer Class service as a thin wrapper around the IBM Waston API class. For extensibility purposes, the logic of consuming API service is defined in the Watson Service Class, which is an executable class. It enables any applicable API service to be wrapped into the transformer. On the other hand, the transformer handles mapping input data to API calls and parse the service response to data fields. 

<img style="float: center;" src="document/Watson_Tranformer_Design.svg">  

# Performance
* __Experiment 1__: The following plot shows the performance of doing Speech-To-Text on a dataset has 2K recordings. Base on the measurement on the sample recording, It takes time equal to ~25% of the length of recordings to finish the transcribing process, including the file uploading time. In the distributed case, a 10-nodes spark cluster is provisioned with two vCPU each, and nine datasets contain [1,20,40,80,160,320,640,1280,2000] recordings respectfully were used to measure the performance. Here is the result summary:

  * Time Complexity is between **O(0.001N)** and **O(0.005N)**, *N = total recording seconds in the dataset*
  * 1 clock second can process **~260** recording seconds.
  * Speed up ~ **65X** comparing to sequential case.

<img style="float: center;" src="document/watson_transformer_stt_perf.png"> 

* __Experiment 2__: In this experiment, the only different parameter is that two IBM Waston API transformers are chained together in the Spark ML Pipeline: (Speech To Text) => (Natural Language Understanding). The input is voice recording, and the output is a rich set of features(keywords, concept, sentiment, emotion) parsed from the transcribed speech. The sequential case takes time equal to ~30% of the length of recordings. Here is the result summary:

  * Time Complexity is between **O(0.005N)** and **O(0.01N)**, *N = total recording seconds in the dataset*
  * 1 clock second can process **~140** recording seconds
  * Speed up ~ **40X** comparing to sequential case.

<img style="float: center;" src="document/watson_transformer_perf_full_pipeline.png"> 


# Tutorial

.... to be added

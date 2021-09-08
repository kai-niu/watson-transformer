What is Watson Transformer?
===========================
Watson Transformer solves the problem of consuming 
IBM Watson API services(STT, NLU, etc.) at scale by 
wrapping the service calls into the Spark transformer. 
In this way, The IBM services such as STT and NLU can 
build into the Spark ML pipeline, along with 
other transformers and estimators to tackle the big 
data challenge.

The Design
----------
As the UML chart illustrates, The Watson Transformer 
Class service as a thin wrapper around the IBM Waston 
API class. For extensibility purposes, the logic of 
consuming API service is defined in the Watson Service 
Class, which is an executable class. It enables any 
applicable API service to be wrapped into the transformer. 
On the other hand, the transformer handles mapping input 
data to API calls and parse the service response to 
data fields. 
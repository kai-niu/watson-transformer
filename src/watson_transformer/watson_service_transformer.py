"""
"
" pyspark transformer consume IBM Watson service
" @ref: https://stackoverflow.com/questions/41399399/serialize-a-custom-transformer-using-python-to-be-used-within-a-pyspark-ml-pipel/52467470#52467470
" @ref: https://stackoverflow.com/questions/32331848/create-a-custom-transformer-in-pyspark-ml
"
"""
import numbers
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from pyspark import keyword_only
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from pyspark.ml.pipeline import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable


class WatsonServiceTransformer(Transformer, 
                               HasInputCol, 
                               HasOutputCol,
                               DefaultParamsReadable,
                               DefaultParamsWritable):
    
    """
    "
    " set init transformer and set parameters
    "
    """
    @keyword_only
    def __init__(self, 
                 inputCol=None, 
                 outputCol=None, 
                 vectorization=False,
                 max_workers=5,
                 service=None):
        """
        @param::inputCol: the input column name contains sound file name
        @param::outputCol: the output column name
        @param::vectorizaton: flag indicate whether enable vectorization
        @param::max_workders: the max number of workers for each task
        @param::service: the IBM service object
        @return: none
        """

        super(WatsonServiceTransformer, self).__init__()
        self.service = Param(self, "service", None)
        self._setDefault(service=None)
        self.vectorization = Param(self, "vectorization", False)
        self._setDefault(vectorization=False)
        self.max_workers = Param(self, "max_workers", 5)
        self._setDefault(max_workers=5)
        kwargs = self._input_kwargs
        self._set(**kwargs)

        # make sure parameter: token, endpoint set properly.
        if not callable(service):
            raise ValueError('> The service instance provided must be callable object.')
        if not isinstance(max_workers, numbers.Number) or max_workers <= 0:
            raise ValueError('> The number of maximum workers must greater than 0.')
        if not inputCol or not inputCol.strip():
            raise ValueError('> The input column name is required.')
        if not outputCol or not outputCol.strip():
            raise ValueError('> The output column name is required.')
    
    """
    "
    " set parameters, called from self._set(), inherited method.
    "
    """
    @keyword_only
    def setParams(self):
        """
        @param:: None
        @return: none
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    """
    "
    " set whether or not enable vectorized udf
    "
    """
    def setVectorization(self, value):
        """
        @param::value: boolean value indcating enable vectorized udf 
        @return: None
        """
        return self._set(vectorization=value)
    
    """
    "
    " get enable state of vectorized udf
    "
    """
    def getVectorization(self):
        """
        @param::None
        @return: vectorization flag
        """
        return self.getOrDefault(self.vectorization)

    """
    "
    " set the maximum numbers of workder in each task
    "
    """
    def setMax_workers(self, value):
        """
        @param::value: unsigned int indicate the max number of workers
        @return: None
        """
        return self._set(max_workers=value)
    
    """
    "
    " get the max number of workers in each task
    "
    """
    def getMax_workers(self):
        """
        @param::None
        @return: the configured max workers
        """
        return self.getOrDefault(self.max_workers)
    
    """
    "
    " set the API service object
    "
    """
    def setService(self, value):
        """
        @param::value: the IBM service API object
        @return: None
        """
        return self._set(service=value)
    
    """
    "
    " get the API service object
    "
    """
    def getService(self):
        """
        @param:None
        @return: the configured service object
        """
        return self.getOrDefault(self.service)

    """
    "
    " set input columns name
    "
    """
    def setInputCol(self, value):
        """
        @param::value: the input columns name contains the sound file name
        @return: None
        """
        return self._set(inputCol=value)

    """
    "
    " set output columns name
    "
    """
    def setOutputCol(self, value):
        """
        @param::value: the output column name contains the output result
        @return: None
        """
        return self._set(outputCol=value)

    
    """
    "
    " perform the transform using provided IBM service api
    "
    " exploit arrow and vectorized udf
    " ref: https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html 
    "
    " to prevent udf from being called multiple times, use asNondeterministic()
    " issue: https://github.com/apache/spark/pull/19929/files/cc309b0ce2496365afd8c602c282e3d84aeed940
    " ref:https://stackoverflow.com/questions/58696198/spark-udf-executed-many-times
    "
    """
    def _transform(self, df:DataFrame) -> DataFrame:
        """
        @param::df: the pyspark dataframe
        @return: the transformed dataframe
        """
        # get the new service instance
        service = self.getService()
        enable_vectorization = self.getVectorization()
        max_workers = max(self.getMax_workers(), 1)
        return_type = service.get_return_type()

        # define the (Vectorized) UDF
        if enable_vectorization:
            # vectorized udf
            @F.pandas_udf(return_type, F.PandasUDFType.SCALAR)
            def vectorized_udf(input_data):
                results = []
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    results = executor.map(lambda data:service.get_new_client()(data), input_data)
                return pd.Series(results)
        else:
            # regular udf
            default_udf = F.udf(lambda data:service.get_new_client()(data), return_type)
        udf = vectorized_udf if enable_vectorization else default_udf
        udf = udf.asNondeterministic() # prevent udf from being called mutliple times
        df = df.withColumn(self.getOutputCol(), udf(F.col(self.getInputCol())))
        return df
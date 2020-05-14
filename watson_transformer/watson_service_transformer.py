"""
"
" pyspark transformer consume IBM Watson service
" @ref: https://stackoverflow.com/questions/41399399/serialize-a-custom-transformer-using-python-to-be-used-within-a-pyspark-ml-pipel/52467470#52467470
" @ref: https://stackoverflow.com/questions/32331848/create-a-custom-transformer-in-pyspark-ml
"
"""

from pyspark import keyword_only
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, StructType, StructField, Row
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
                 service=None):
        """
        @param::inputCol: the input column name contains sound file name
        @param::outputCol: the output column name
        @param::service: the IBM service object
        @return: none
        """
        super(WatsonServiceTransformer, self).__init__()
        self.service = Param(self, "service", None)
        self._setDefault(service=None)
        kwargs = self._input_kwargs
        self._set(**kwargs)
        # make sure parameter: token, endpoint set properly.
        if self.service == None:
            raise ValueError('> IBM service configuration must be provided.')
    
    """
    "
    " set parameters, called from self._set(), inherited method.
    "
    """
    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, stt=None):
        """
        @param::inputCol: the input column name contains sound file name
        @param::outputCol: the output column name
        @param::service:: the IBM service object
        @return: none
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)
    
    """
    "
    " set IBM STT configuration includes: api token/endpoint, params to IBM STT service
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
    " get the IBM STT configuration object
    "
    """
    def getService(self):
        """
        @param::value: the IBM service API object
        @return: None
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
    " perform the transform using provided IBM service api
    """
    def _transform(self, df:DataFrame) -> DataFrame:
        """
        @param::df: the pyspark dataframe
        @return: the transformed dataframe
        """
        stt_udf = F.udf(self.getService()(), self.getService()().get_return_type())
        df = df.withColumn(self.getOutputCol(), stt_udf(F.col(self.getInputCol())))
        return df
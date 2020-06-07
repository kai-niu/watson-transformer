"""
"
" pyspark transformer covert JSON data to data column(s)
" @ref: https://stackoverflow.com/questions/41399399/serialize-a-custom-transformer-using-python-to-be-used-within-a-pyspark-ml-pipel/52467470#52467470
" @ref: https://stackoverflow.com/questions/32331848/create-a-custom-transformer-in-pyspark-ml
"
"""
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from pyspark import keyword_only
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, StructType, StructField, Row
from pyspark.sql import DataFrame
from pyspark.ml.pipeline import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable


class JSONTransformer(Transformer, 
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
                 removeInputCol=False,
                 parser=None):
        """
        @param::inputCol: the input column name contains sound file name
        @param::outputCol: the output column name
        @param::removeInputCol: flag indicate whether remove input columns
        @param::parser: parser object parse JSON data to data column(s)
        @return: none
        """

        super(JSONTransformer, self).__init__()
        self.parser = Param(self, "parser", None)
        self._setDefault(parser=None)
        self.removeInputCol = Param(self, "removeInputCol", False)
        self._setDefault(removeInputCol=False)
        kwargs = self._input_kwargs
        self._set(**kwargs)

        # make sure parameter: token, endpoint set properly.
        if not callable(parser):
            raise ValueError('> The parser instance provided must be callable object.')
        if not inputCol or not inputCol.strip():
            raise ValueError('> The input column name is required.')
        if not outputCol or not outputCol.strip():
            raise ValueError('> The output column name is required.')



        # make sure parameter: token, endpoint set properly.
        if parser == None:
            raise ValueError('> JSONTransformer Class: JSON data parser object must be provided.')
    
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
    " set whether or not remove input column from dataframe
    "
    """
    def setRemoveInputCol(self, value):
        """
        @param::value: boolean value indcating enable vectorized udf 
        @return: None
        """
        return self._set(removeInputCol=value)
    
    """
    "
    " get enable state of vectorized udf
    "
    """
    def getRemoveInputCol(self):
        """
        @param:: None
        @return: vectorization flag
        """
        return self.getOrDefault(self.removeInputCol)
    
    """
    "
    " set the parser object
    "
    """
    def setParser(self, value):
        """
        @param::value: the IBM service API object
        @return: None
        """
        return self._set(parser=value)
    
    """
    "
    " get the parser object
    "
    """
    def getParser(self):
        """
        @param:None
        @return: the configured service object
        """
        return self.getOrDefault(self.parser)

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
    " perform the transform 
    "
    """
    def _transform(self, df:DataFrame) -> DataFrame:
        """
        @param::df: the pyspark dataframe
        @return: the transformed dataframe
        """
        # get the new service instance
        parser = self.getParser()
        outputCol = self.getOutputCol()
        inputCol = self.getInputCol()
        removeInputCol = self.getRemoveInputCol()
        return_type = parser.get_return_type()
        # CPU bounded task, not going to benefit from vectorized UDF very much
        parser_udf = F.udf(parser, return_type).asNondeterministic()
        df = df.withColumn(outputCol, parser_udf(F.col(inputCol)))
        # drop input column based on flag
        if removeInputCol:
            df = df.drop(self.getInputCol())
        return df
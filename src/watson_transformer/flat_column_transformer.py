"""
"
" explode the struct column to multiple simple columns
" @ref: https://stackoverflow.com/questions/47669895/how-to-add-multiple-columns-using-udf?rq=1
"
"""

from pyspark import keyword_only
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, StructType, StructField, Row
from pyspark.sql import DataFrame
from pyspark.ml.pipeline import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable



class FlatColumnTransformer(Transformer, 
                            HasInputCol, 
                            DefaultParamsReadable,
                            DefaultParamsWritable):
    
    """
    "
    " set init transformer and set parameters
    "
    """
    @keyword_only
    def __init__(self, 
                 inputCol=None):
        """
        @param::inputCol: the input column name contains sound file name
        @return: none
        """
        super(FlatColumnTransformer, self).__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)
    
    """
    "
    " set parameters, called from self._set(), inherited method.
    "
    """
    @keyword_only
    def setParams(self, inputCol=None):
        """
        @param::inputCol: the input column name contains sound file name
        @return: none
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

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
    " perform the transform using provided IBM service api
    "
    """
    def _transform(self, df:DataFrame) -> DataFrame:
        """
        @param::df: the pyspark dataframe
        @return: the transformed dataframe
        """
        cols = df.columns
        if self.getInputCol() in cols:
            cols.remove(self.getInputCol())
            return df.withColumn('__explode_col_output__',F.explode(F.array(F.col(self.getInputCol())))) \
                     .select(*cols, "__explode_col_output__.*")
        else:
            raise ValueError("> FlatColumnTransformer class: inputCol is not in the dataframe.")
API Reference
**************

This page gives the API reference of Watson Transformer Package.

WatsonServiceTransformer Class
===============================

**WatsonServiceTransformer** (*inputCol=None, outputCol=None, vectorization=False, max_workers=5, service=None*)

**Base**: 
    - ``pySpark.ml.pipeline.Transformer``
    - ``pyspark.ml.param.shared.HasInputCol``
    - ``pyspark.ml.param.shared.HasOutputCol``
    - ``pyspark.ml.util.DefaultParamsReadable``
    - ``pyspark.ml.util.DefaultParamsWritable``

**Parameters**:
    - **inputCol**: The column name use as input data. ``required``
    - **outputCol**: The column name use to output the transformed data. ``required``
    - **vectorization**: Exploiting pyArrow in-memory dataframe. enable vectorization whenever is possible is recommend. The default value is ``False``.
    - **max_workers**: When vectorization is enabled, the maximum number of threads can be utilized to boost the performance. The default value is ``5``.
    - **service**: The API service instance that wrapped by the Watson Transformer. ``required``

**Return**:
    ``WatsonServiceTransformer`` class instance

**Return Type**:
    ``pySpark.ml.pipeline.Transformer``

-----------------------------

**- transform** (*dataframe*)

**Parameters**:
    - **dataframe**: the pySpark dataframe recieve transformation

**Return**:
    pySpark dataframe contains transformation result

**Return Type**:
    ``pyspark.sql.DataFrame``


.. note::

    The ``WatsonServiceTransformer`` is a custom implementation of pySpark transformer. The functions which are inherited from the
    pySpark transformer base classes have been implemented thus avaiable for use.


FlatColumnTransformer Class
============================

**FlatColumnTransformer** (*inputCol=None*)

**Base**: 
    - ``pySpark.ml.pipeline.Transformer``
    - ``pyspark.ml.param.shared.HasInputCol``
    - ``pyspark.ml.util.DefaultParamsReadable``
    - ``pyspark.ml.util.DefaultParamsWritable``

**Parameters**:
    - **inputCol**: The column name use as input data. ``required``

**Return**:
    ``FlatColumnTransformer`` class instance

**Return Type**:
    ``pySpark.ml.pipeline.Transformer``

-----------------------------

**- transform** (*dataframe*)

**Parameters**:
    - **dataframe**: the pySpark dataframe recieve transformation

**Return**:
    pySpark dataframe contains the flattened data from input column

**Return Type**:
    ``pyspark.sql.DataFrame``


.. note::

    The ``FlatColumnTransformer`` is a custom implementation of pySpark transformer. The functions which are inherited from the
    pySpark transformer base classes have been implemented thus avaiable for use. This transformer will
    flatten the nested input column to multiple regular data columns.


JSONTransformer Class
=====================

**JSONTransformer** (*inputCol=None, outputCol=None, removeInputCol=False, parser=None*)

**Base**: 
    - ``pySpark.ml.pipeline.Transformer``
    - ``pyspark.ml.param.shared.HasInputCol``
    - ``pyspark.ml.param.shared.HasOutputCol``
    - ``pyspark.ml.util.DefaultParamsReadable``
    - ``pyspark.ml.util.DefaultParamsWritable``

**Parameters**:
    - **inputCol**: The column name use as input data. ``required``
    - **outputCol**: The column name use to output the transformed data. ``required``
    - **removeInputCol**: Whether or not remove the input column from output dataframe. The default value is ``False``.
    - **parser**: The object parse JSON data to data column(s). ``required``

**Return**:
    ``JSONTransformer`` class instance

**Return Type**:
    ``pySpark.ml.pipeline.Transformer``

-----------------------------

**- transform** (*dataframe*)

**Parameters**:
    - **dataframe**: the pySpark dataframe recieve transformation

**Return**:
    pySpark dataframe contains transformation result

**Return Type**:
    ``pyspark.sql.DataFrame``


.. note::

    The ``WatsonServiceTransformer`` is a custom implementation of pySpark transformer. The functions which are inherited from the
    pySpark transformer base classes have been implemented thus avaiable for use.




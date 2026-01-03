# Data Profiler Example


## Imports

```python

from pyspark.sql import SparkSession
from dqf.data_profiler import *

```

## Create SparkSession
```python

spark = (SparkSession
                     .builder
                     .master("local[1]")
                     .appName("test")
                     .getOrCreate())

```

## Create example dataframe

```python

input_df = spark.createDataFrame(
            data=[[None, None, 1],
                  ['Bill', None, 2],
                  ['Bill', "John", 0]],
            schema=['colA', 'colB', 'colC'])

```

## Instantiate DataFrameProfiler

```python

df_profiler = DataFrameProfiler(input_df)

```

## Get full profile of the dataframe (statisticas and metrics)

```python

df_stats, col_stats, df_metrics = df_profiler.get_full_profile()

```

## As a result we get three dictionaries:

### Dataframe general statistics
```python

{
    'count': 3, 
    'distinct_count': 3, 
    'duplicates_count': 0
}

```

### Columns statistics
```python

{
    'nulls_percentage': {'colA': 33, 'colB': 66, 'colC': 0}, 
    'zeros_percentage': {'colA': 0, 'colB': 0, 'colC': 33}, 
    'max_values_percentage': {'colA': 66, 'colB': 33, 'colC': 33}, 
    'min_values_percentage': {'colA': 66, 'colB': 33, 'colC': 33}, 
    'distinct_count': {'colA': 2, 'colB': 2, 'colC': 3}, 
    'max_value': {'colA': 'Bill', 'colB': 'John', 'colC': 2}, 
    'min_value': {'colA': 'Bill', 'colB': 'John', 'colC': 0}, 
    'null_count': {'colA': 1, 'colB': 2, 'colC': 0}, 
    'zeros_count': {'colA': 0, 'colB': 0, 'colC': 1}, 
    'max_values_count': {'colA': 2, 'colB': 1, 'colC': 1}, 
    'min_values_count': {'colA': 2, 'colB': 1, 'colC': 1}
}

```

### Dataframe metrics according to Daimler Global Data and Information Policy

```python

{
    'completeness': 67.0, 
    'uniqueness': 100
}

```

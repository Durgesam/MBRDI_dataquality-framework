# Statistical Functions For DQF
> Click on Stats accessing the Reference Guides

| Stats Function Name | Short Description | Performance Impact |
| :---- | :---- | :---- |
| [get_row_count](#get_row_count) | Returns the number of rows of the dataframe/table. | |
| [get_distinct_count](#get_distinct_count) | Calculates the number of distinct rows for each column. |  |
| [get_null_count](#get_null_count) | Return number of null values in a specific column. |  |
| [get_duplicates](#get_duplicates) | Return number of duplicates in a specific column. |  |
| [get_min_value](#get_min_value) | 	Returns the minimum value in a column. |  |
| [get_max_value](#get_max_value) | Returns the maximum value in a column. |  |
| [get_median_value](#get_median_value) | 	Returns the median value in a column. |  |
| [get_mean_value](#get_mean_value) | Returns the mean value in a column. |  |

# Stats Reference Guide

## get_row_count
**Description:**

> This function helps to get the count of total rows in the passed dataframe/table.


**Example:**

> As an example, consider the table below with 3 rows, on applying `get_row_count()` function we will get 3 as the output 
> 
> Name | EmpId 
> :---- | :---- | 
> Rob | 1234 | 
> Kate | 1235 | 
> Josh | 1238 | 
> 


**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
> *returns*: ``Integer value depicting total rows``

**Code Implementation**

```python
from dqf.shared.stats import GetRowCount
# result will be an Integer showing count of rows
result = GetRowCount(table=dataframe_object).run()
 ```

---

## get_distinct_count
**Description:**

> This function gives the distinct count of values in each column of the dataframe/table.

**Example:**

> As an example, consider the table below with 2 columns `column1` and `column2`, on applying `get_distinct_count()` function we will get the distinct count of values for each column
> `{ column1: 3, column2: 4`}
> 
> column1 | column2 
> :---- | :---- | 
> Rob | 1 | 
> Kate | 2 | 
> Josh | 2 | 
> Rob  | 4 |
> Josh | 3 |

**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
> 
>       **columns**: ``List[str], Tuple[str], str``

>
> *returns*: ``Dictionary with column as key and distinct count as value``

**Code Implementation**

```python
from dqf.shared.stats import GetDistinctCount
# result will return a dictionary
result = GetDistinctCount(table=dataframe_object, columns=columns).run()
```

---

## get_null_count
**Description:**

> This function returns the count of `null` values in each column of the dataframe/table.
>
> Usually, in case of csv format, null values can be a string.
> The optional argument `string_check` allows you to additionally count the "null" string in columns.
> **It accepts either a boolean value, or case-insensitive `"True"` or `"False"` string value.

**Example:**

> As an example, consider the table below with 3 columns `Name` , `State`  and `Gender`, on applying `get_null_count()` function we will get the count of `null` values for each column
> 
> Name | State | Gender |
> :---- | :---- | :---- |
> Rob | null | M|
> Kate | NY | F|
> Josh | null | "null"|



**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
>       **string_check** ``bool, str``, optional, defaults to `False`
>
> *returns*: ``Dictionary with column as key and count of null as value``

**Code Implementation**

```python
from dqf.shared.stats import GetNullCount
# result will return a dictionary
result = GetNullCount(table=dataframe_object, columns=columns).run()
```
> `{ Name: 0, State: 2, Gender: 0}`

```python
from dqf.shared.stats import GetNullCount
# result will return a dictionary
result = GetNullCount(table=dataframe_object, columns=columns, string_check=True).run()
```
> `{ Name: 0, State: 2, Gender: 1}`

---

## get_duplicates
**Description:**

> This function helps to get the count of duplicate values in each column of the dataframe/table.

**Example:**

> As an example, consider the table below with 2 columns `Name`  and `Country`, on applying `get_duplicates()` function we will get the count of duplicate values for each column
> `{ Name: 0, Country: 5}`
> 
> Name | Country |
> :---- | :---- | 
> Rob | Country_2 | 
> Kate | Country_1 | 
> Josh | Country_2 | 
> Steve | Country_2 | 
> Julie | Country_1 | 
> thresa | Country_3 |

**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
> *returns*: ``Dictionary with column as key and count of null as value``

**Code Implementation**

```python
from dqf.shared.stats import GetDuplicateCount
# result will return a dictionary
result = GetDuplicateCount(table=dataframe_object, columns=columns).run()
```

---

## get_min_value

**Description:**

> This function helps to get the minimum value in each column of the dataframe/table. 

**Example:**

> As an example, consider the table below with 2 columns `Column1`  and `Column2`, on applying `get_min_value()` function we will get the minimum value for each column
> `{ Column1: 0, Column2: 5}`
> 
> Column1 | Column2 |
> :---- | :---- | 
> 0 | 7 | 
> 3 | 9 |  
> 7 | 5 | 
> 9 | 10 |

**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
> *returns*: ``Dictionary with column as key and minimun value of the column as value``

**Code Implementation**

```python
from dqf.shared.stats import GetMinValue
# result will return a dictionary
result = GetMinValue(table=dataframe_object, columns=columns).run()
```
## get_max_value
**Description:**

> This function helps to get the maximum value in each column of the dataframe/table.

**Example:**

> As an example, consider the table below with 2 columns `Column1`  and `Column2`, on applying `get_max_value()` function we will get the maximum value for each column
> `{ Column1: 9, Column2: 10}`
> 
> Column1 | Column2 |
> :---- | :---- | 
> 0 | 7 | 
> 3 | 9 |  
> 7 | 5 | 
> 9 | 10 |


**Python Class usage:**

> **Arguments:** 
>
>        **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>

> *returns*: ``Dictionary with column as key and maximum value of the column as value``

**Code Implementation**

```python
from dqf.shared.stats import GetMaxValue
# result will return a dictionary
result = GetMaxValue(table=dataframe_object, columns=columns).run()
```
## get_median_value
**Description:**

>  This function helps to get the median value of each column of the dataframe provided the column is of integer, float or long datatype

**Example:**

> As an example, consider the table below with 2 columns `Column1`  and `Column2`, on applying `get_median_value()` function we will get the median value for each column
> `{ Column1: 3.0, Column2: 7.0}`
> 
> Column1 | Column2 |
> :---- | :---- | 
> 0 | 7 | 
> 3 | 9 |  
> 7 | 5 | 
> 9 | 10 |

**Python Class usage:**

> **Arguments:** 
>
>        **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>

> *returns*: ``Dictionary with column as key and median value of the column as value``

**Code Implementation**

```python
from dqf.shared.stats import  GetMedianValue
# result will return a dictionary
result =  GetMedianValue(table=dataframe_object, columns=columns).run()
```




---



## get_mean_value
**Description:**

> This function returns the mean value of each column of the dataframe/table provided the column is of integer, float or long datatype.

**Example:**

> As an example, consider the table below with 2 columns `Column1`  and `Column2`, on applying `get_mean_value()` function we will get the mean value for each column
> `{ Column1: 4.75, Column2: 7.75}`
> 
> Column1 | Column2 |
> :---- | :---- | 
> 0 | 7 | 
> 3 | 9 |  
> 7 | 5 | 
> 9 | 10 |

**Python Class usage:**

> **Arguments:** 
>
>        **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>

> *returns*: ``Dictionary with column as key and mean value of the column as value``

**Code Implementation**

```python
from dqf.shared.stats import GetMeanValue
# result will return a dictionary
result = GetMeanValue(table=dataframe_object, columns=columns).run()
```

---

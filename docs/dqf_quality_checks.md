# Available Quality Checks
> Click on checks accessing the Reference Guides

| Check Name | Short Description | Type |
| :---- | :---- | :---- |
| [check_continuous_samples](#check_continuous_samples) | Checks continuity of sample values. | ColumnLevelCheck |
| [check_data_relation](#check_data_relation) |  Checks for each row whether values in one column are always at least as large as values in another column. | TableLevelCheck |
| [check_datatype](#check_datatype) | Checks whether the data type of the column is as expected. | ColumnLevelCheck |
| [check_empty_df](#check_empty_df) | Checks whether the table/dataframe is empty or not. | TableLevelCheck |
| [check_empty_string](#check_empty_string) | Checks whether any column has empty strings. | ColumnLevelCheck |
| [check_equal_dataframes](#check_equal_dataframes) | In Development. | ColumnLevelCheck |
| [check_foreign_key](#check_foreign_key) | Checks for foreign key constraints. | DatabaseLevelCheck |
| [check_max_timestamp](#check_max_timestamp) | Checks if values in column are violating a maximum Timestamp. | ColumnLevelCheck |
| [check_max_value](#check_max_value) | Checks whether the values are violating a maximum value. | ColumnLevelCheck |
| [check_min_timestamp](#check_min_timestamp) | Checks if values in column are violating a minimum Timestamp. | ColumnLevelCheck |
| [check_min_value](#check_min_value) | Checks whether the values are violating a minimum value. | ColumnLevelCheck |
| [check_null](#check_null) | Checks whether a column has any null values. | ColumnLevelCheck |
| [check_regex](#check_regex) | Checks whether a column has any values that do not match the given pattern. | ColumnLevelCheck |
| [check_string_consistency](#check_string_consistency) | Checks whether column values have consistent number of characters. | ColumnLevelCheck |
| [check_string_pattern](#check_string_pattern) | Checks whether string values start or end with certain pattern. | ColumnLevelCheck |
| [check_unique_tuple](#check_unique_tuple) | Checks whether a combination of column values is unique or not. | ColumnLevelCheck |
| [check_unique](#check_unique) | Checks whether the given columns contain only unique values or not. | ColumnLevelCheck |
| [check_value_in_range](#check_value_in_range)  |Checks whether the values of a column are in a specific range. | ColumnLevelCheck |
| [check_enum](#check_enum)  |Checks whether the column contains valid values. | ColumnLevelCheck |

# Check Reference Guide

## check_continuous_samples
**Description:**

> Checks whether there are only continuous samples in a dataframe.
> For further explanation, take a look at the example below.

**Example:**

> As an example, consider the table below showing a dataframe which contains `temperature` and `force` measurements in the same table from different time spans. 
> 
> channel | start_time | end_time | value |
> :---- | :---- | :---- | :---- |
> temperature | 01:23:00 | 01:24:02 | 43 |
> temperature | 01:24:02 | 01:25:01 | 42 |
> temperature | 01:25:01 | 01:25:59 | 44 |
> force | 04:01:37 | 04:01:53 | 184 |
> force | 04:01:53 | 04:02:14 | 192 |
> 
> Here, we want to validate that the `start_time` is always the same as the `end_time` of the previous sample of the same channel.
>
> There is also a non-strict mode validating that `start_time` is at least as large as  the `end_time` of the previous sample.


**Definition in check profile:**

````json
{
  "type": "check_continuous_samples",
  "kwargs": {
    "table": "table_name",
    "sample_key": "channel",
    "column1": "start_time",
    "column2": "end_time",
    "strict": true
  }
}
````
Please note that `strict` is an optional argument and defaults to `True`.

**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
> 
>       **sample_key**: ``str``
>
>       **column1**: ``str``
>
>       **column2**: ``str``
> 
>       **strict**: ``bool``, optional, defaults to `True`
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckContinuousSamples
# result will return True or False
result = CheckContinuousSamples(table=dataframe_object, sample_key="channel", column1="start_time", column2="end_time").run()
# Or, if you want to use the non-strict mode:
result = CheckContinuousSamples(table=dataframe_object, sample_key="channel", column1="start_time", column2="end_time", strict=False).run()
```

---

## check_data_relation
**Description:**

> Checks for each row whether values in one column are always at least as large as values in another column.

**Example:**

> As an example, consider the table below showing a dataframe which contains the `birthdate` and `graduation_date` of persons.
> Here we can apply `check_data_relation` since the person must already be alive when graduating.
> 
> birthdate | graduation_date |
> :---- | :---- |
> 1967-05-12 | 1990-08-30 |
> 1993-09-01 | 2019-02-17 |

**Definition in check profile:**

````json
{
  "type": "check_data_relation",
  "kwargs": {
    "table": "table_name",
    "column1": "birthdate",
    "column2": "graduation_date"
  }
}
````

**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **column1**: ``str``
>
>       **column2**: ``str``
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckDataRelation
# result will return True or False
result = CheckDataRelation(table=dataframe_object, column1="birthdate", column2="graduation_date").run()
```

---

## check_datatype
**Description:**

> Checks whether the data type of one or more columns is as expected.

**Example:**

> Returns ``True`` if the data type of ``column_a`` matches with given data type for one column.
>
> Returns ``True`` if the data type of ``column_a`` and ``column_b`` matches with given data type for multiple columns.

**Definition in check profile:**

````json
{
  "type": "check_datatype",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a", "column_b"],
    "d_type": "int"
  }
}
````

**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
>       **d_type**: str
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckDatatype
# result will return True or False
result = CheckDatatype(table=dataframe_object, columns=["column_a", "column_b"], d_type="int").run()
```

```python
from dqf.shared.checks import CheckDatatype
# result will return True or False
result = CheckDatatype(table=dataframe_object, columns="column_a", dtype="str").run()
```

---

## check_empty_df
**Description:**

> Checks whether the table/dataFrame is empty or not.

**Example:**

> Returns ``True`` if the table/dataframe is not empty.

**Definition in check profile:**

````json
{
  "type": "check_empty_df",
  "kwargs": {
    "table": "table_name"
  }
}
````

**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckEmptyDF
# result will return True or False
result = CheckEmptyDF(table=dataframe_object).run()
```

---

## check_empty_string

**Description:**

> Checks whether one or more columns have empty strings.

**Example:**

> Returns ``True`` if ``column_a`` does not have any empty strings for one column.
>
> Returns ``True`` if ``column_a`` and ``column_b`` do not have any empty strings for multiple columns.

**Definition in check profile:**

````json
{
  "type": "check_empty_string",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a", "column_b"]
  }
}
````

**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckEmptyString
# result will return True or False
result = CheckEmptyString(table=dataframe_object, columns=["column_a", "column_b"]).run()
```

```python
from dqf.shared.checks import CheckEmptyString
# result will return True or False
result = CheckEmptyString(table=dataframe_object, columns="column_b").run()
```

---

## check_equal_dataframes
In Development

---

## check_foreign_key
**Description:**

> Checks for foreign key constraints.

**Example:**

> Returns ``True`` if foreign key constraints are satisified.

**Definition in check profile:**

````json
{
  "type": "check_foreign_key",
  "kwargs": {
    "primary_table": "primary_table_name",
    "foreign_table": "foreign_table_name",
    "primary_key_column": "column_a",
    "foreign_key_column": "column_b"
  }
}
````

**Python Class usage:**

> **Arguments:** 
>
>       **primary_table**: ``str, pathlib.Path, DataFrame``
>
>       **foreign_table**: ``str, pathlib.Path, DataFrame``
>
>       **primary_key_column**: ``str``
>
>       **foreign_key_column**: ``str``
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckForeign
# result will return True or False
result = CheckForeign(primary_table=primary_dataframe_object, foreign_table=foreign_dataframe_object, column1="column_a", column2="column_b").run()
```

---

## check_max_timestamp
**Description:**


> Checks if the timestamps in a specific column(s) are below the given maximum timestamp. Values are date strings. 

> For this check passing ``today`` as value will automatically pick the date when check run will be performed.
> Running the check on 21th of October 2021, keyword ``today`` will pass timestamp ``2021-10-21``.

**Example:**

> Returns ```True``` if the timestamp values are higher than the value ``1988-01-01`` in the defined columns
> (All timestamps have to be after 1st January 1988). 
> If one or more samples have a lower timestamp e.g. ``1980-01-01`` the check will return ```False```.
>
> Example 1 will return ```False``` if there is any timestamp 
> value higher than ```2020-04-04```, if not, will return ```True```.
> 
> Example 2 will return ```False``` if there is any timestamp value in the defined columns which 
> are higher than ```today```s Timestamp. (Timestamp will be created when check will be run... see the description)


**Definition in check profile:**

Example 1:
````json
{
  "type": "check_max_timestamp",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a", "column_b"],
    "timestamp": "2020-04-04"
  }
}
````
Example 2:
````json
{
  "type": "check_max_timestamp",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a", "column_b"],
    "timestamp": "today"
  }
}
````

**Python Class usage:**

> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
>       **timestamp**: ``DateType, str``
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckMaxTimestamp
# result will return True or False
result = CheckMaxTimestamp(table=dataframe_object, columns=["column_a", "column_b"], timestamp="today").run()
```

---

## check_max_value
**Description:**

> Checks if the values in a specific column(s) are below the given maximum value. Values can be *integers* or *floats*.

**Example:**

> Returns ```False``` if there are values which are higher than the value ``1000`` in the defined columns.
> If all values are smaller than the given value, the check will return ```True``.

**Definition in check profile:**

````json
{
  "type": "check_max_value",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a", "column_b"],
    "value": 1000
  }
}
````

**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>        **columns**: ``List[str], Tuple[str], str``
>
>       **value**: ``int, float``
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckMaxValue
#result will return True or False
result = CheckMaxValue(table=dataframe_object, columns=["column_a", "column_b"], value=1000).run()
```

---

## check_min_timestamp
**Description:**

> Checks if the timestamps in a specific column(s) are above the given minimum timestamp. Values are date strings.

**Example:**

> Returns ```True``` if the timestamp values are higher than the value ``1988-01-01`` in the defined columns
> (All timestamps have to be after 1st January 1988).
> If one or more samples have a lower timestamp e.g. ``1980-01-01`` the check will return ```False```

**Definition in check profile:**

````json
{
  "type": "check_min_timestamp",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a", "column_b"],
    "timestamp": "1988-01-01"
  }
}
````

**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
>       **timestamp**: ``DateType, str``
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckMinTimestamp
# result will return True or False
result = CheckMinTimestamp(dataframe=yourDataFrameObject, columns=["column_a", "column_b"], timestamp="1988-01-01").run()
```

---

## check_min_value
**Description:**

> Checks if the values in a specific column(s) are above the given minimum value. Values can be *integers* or *floats*.

**Example:**

> Returns ```False``` if there are values which are lower than the value ``1000`` in the defined columns.
> If all values are higher than the given value, the check will return ```True```.

**Definition in check profile:**

````json
{
  "type": "check_min_value",
  "kwargs": {
    "table": "yourTableName",
    "columns": ["column_a", "column_b"],
    "value": 1000
  }
}
````

**Python Class usage:**

> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
>       **value**: ``int, float``
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckMinValue
# result will return True or False
result = CheckMinValue(dataframe=yourDataFrameObject, columns=["column_a", "column_b"], value=1000).run()
```

---

## check_null
**Description:**

> Checks whether one or more columns have null values.
>
> Usually, in case of csv format, null values can be a string.
> The optional argument `string_check` allows you to additionally check for "null" string in columns.
> **It accepts either a boolean value, or case-insensitive `"True"` or `"False"` string value.

**Example:**

> Returns ``True`` if ``column_a`` and ``column_b`` do not have any null values for multiple columns.
>
> Returns ``True`` if ``column_a`` does not have any null values for one column.
>
> Returns ``True`` if ``column_a`` and ``column_b`` do not have any null or "null" string values for multiple columns.

**Definition in check profile:**

````json
{
  "type": "check_null",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a", "column_b"],
    "string_check": false, ...optional
  }
}
````

**Python Class usage:**
> **Arguments:** ``
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
>       **string_check** ``bool, str``, optional, defaults to `False`
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckNull
# result will return True or False
result = CheckNull(table=dataframe_object, columns=["column_a", "column_b"]).run()
```

```python
from dqf.shared.checks import CheckNull
# result will return True or False
result = CheckNull(table=dataframe_object, columns="column_b").run()
```

```python
from dqf.shared.checks import CheckNull
# result will return True or False
result = CheckNull(table=dataframe_object, columns=["column_a", "column_b"], string_check=True).run()
```

---

## check_regex
**Description:**

> Checks column values against a regular expression and returns `True` when all values match, `False` otherwise.
>
> The optional argument `invert_result` can be used to check for the inverted regular expression, hence returning `True`
> when no value matches the regular expression, `False` otherwise.

**Example:**

> Returns ``True`` if ``column_a`` does not have any values that do not match the given pattern for one column.
>
> Returns ``True`` if ``column_a`` and ``column_b`` do not have any values that do not match the given pattern for multiple columns.


**Example:**

> As an example, consider the table below showing a dataframe containing the `email` address of persons.
> Here we can apply `check_regex` to validate that we have at least the `@` symbol inside the mail address.
> Please note that the given regular expression is only an illustrative example and not sufficient to check for real email addresses.
>
> name | email |
> :---- | :---- |
> Sue Winter | sue.winter@mail.com |
> John Doe | john.the.greatest@gmail.com |
> Petik Martinek | petik@martinek.de |


**Definition in check profile:**

````json
{
  "type": "check_regex",
  "kwargs": {
    "table": "table_name",
    "columns": "email",
    "regex": ".+@.+",
    "invert_result": false, ...optional
  }
}
````

**Python Class usage:**
> **Arguments:**
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
>       **invert_result** ``bool``, optional, defaults to `False`
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckRegex
# result will return True or False
result = CheckRegex(table=dataframe_object, columns="email", regex=".+@.+").run()
```

```python
from dqf.shared.checks import CheckRegex
# result will return True or False
result = CheckRegex(table=dataframe_object, columns=["column_a", "column_b"], regex="re", invert_result=True).run()
```

---

## check_string_consistency
**Description:**

> Checks whether one or more columns have values that have consistent number of characters when using default method
> "exact". method "min" or "max" can be used to check if values of a specific have a minimum or 
> maximum count of characters

**Example:**

> Returns ``True`` with method **"exact"** if ``column_a`` does not have any values that have inconsistent number of characters for one column.
>
> Returns ``True`` with method **"exact"** if ``column_a`` and ``column_b`` do not have any values that have inconsistent number of characters for multiple columns.
> 
> Returns ``True`` with method **"min"** if ``column_a`` and ``column_b`` contains only values which have more characters then the given string_length.
> 
> Returns ``True`` with method **"max"** if ``column_a`` and ``column_b`` contains only values which have less characters then the given string_length..


**Definition in check profile:**

````json
{
  "type": "check_string_consistency",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a", "column_b"],
    "string_length": 6,
    "method": "exact", ...optional
  }
}
````

**Python Class usage:**

> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
>       **string_length**: ``int``
> 
>       **method**: ''str'' optional, defaults to 'exact'
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckStringConsistency
# result will return True or False
result = CheckStringConsistency(table=dataframe_object, columns=["column_a", "column_b"], string_length=6).run()
```

```python
from dqf.shared.checks import CheckStringConsistency
# result will return True or False
result = CheckStringConsistency(table=dataframe_object, columns="column_a", string_length=7).run()
```

```python
from dqf.shared.checks import CheckStringConsistency
# result will return True or False
result = CheckStringConsistency(table=dataframe_object, columns="column_a", string_length=7, method="min").run()
```

```python
from dqf.shared.checks import CheckStringConsistency
# result will return True or False
result = CheckStringConsistency(table=dataframe_object, columns="column_a", string_length=7, method="max").run()
```

---

## check_string_pattern
**Description:**

> Checks whether one or more columns have values that start or end with certain pattern.

**Example:**

> Returns ``True`` if ``column_a`` does not have any values that do not start or end with certain pattern for one column.
>
> Returns ``True`` if ``column_a`` and ``column_b`` do not have any values that do not start or end with certain pattern for multiple columns.

**Definition in check profile:**

Example 1:
````json
{
  "type": "check_string_pattern",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a", "column_b"],
    "starts_with": "se",
    "ends_with": "um",
    "case_sensitive": false
  }
}
````

Example 2:
````json
{
  "type": "check_string_pattern",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a", "column_b"],
    "starts_with": "Se",
    "ends_with": "um",
    "case_sensitive": true
  }
}
````

**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
>       **starts_with**: ``str``
>
>       **ends_with**: ``str``
>
>       **case_sensitive**: ``bool``
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckStringPattern
# result will return True or False
result = CheckStringPattern(table=dataframe_object, columns=["column_a", "column_b"], starts_with="se", ends_with="um", case_sensitive=False).run()
```

```python
from dqf.shared.checks import CheckStringPattern
# result will return True or False
result = CheckStringPattern(table=dataframe_object, columns="column_a", starts_with="Se", ends_with="um", case_sensitive=True).run()
```

---

## check_unique_tuple
**Description:**

> Checks if a combination of values from different columns are unique in the table or dataframe.
> Columns has to be passed as an array. Otherwise if you want to check just on column for unique values, please use [check_unique](#check_unique)

**Example:**

> The shown Definition will return ```True``` if all combinations of column_a and column_b are unique in the whole dataframe.

> This will return ```True``` because all samples are unique:

| column_a | column_b |
| :---- | :---- |
| FIRST_STREET |CHICAGO|
| SECOND_STREET |NEW-YORK|
| THIRD_STREET |CHICAGO|
| THRID_STREET |NEW-YORK|

> This will return ```False``` because row 1 and 3 are similar combination:


| column_a | column_b |
| :---- | :---- |
| FIRST_STREET |CHICAGO|
| SECOND_STREET |NEW-YORK|
| FIRST_STREET |CHICAGO|
| THRID_STREET |NEW-YORK|


**Definition in check profile:**

````json
{
  "type": "check_unique_tuple",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a", "column_b"],
  }
}
````

**Python Class usage:**

> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str]``
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckUniqueTuple
# result will return True or False
result = CheckUniqueTuple(table=dataframe_object, columns=["column_a", "column_b"]).run()
```

---

## check_unique
**Description:**

> Checks whether one or more columns have only unique values.

**Example:**

> Returns ``True`` if ``column_a`` does not have only unique values for one column.
>
> Returns ``True`` if ``column_a`` and ``column_b`` do not have only unique values for multiple columns.

**Definition in check profile:**

````json
{
  "type": "check_unique",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a", "column_b"]
  }
}
````

**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckUnique
# result will return True or False
result = CheckUnique(table=dataframe_object, columns=["column_a", "column_b"]).run()
```

```python
from dqf.shared.checks import CheckUnique
# result will return True or False
result = CheckUnique(table=dataframe_object, columns="column_a").run()
```

---

## check_value_in_range
**Description:**

> Checks whether one or more columns have values that fall in the given range.

**Example:**

> Returns ``True`` if ``column_a`` does not have values that do not fall in the given range for one column.
>
> Returns ``True`` if ``column_a`` and ``column_b`` do not have values that do not fall in the given range for multiple columns.

**Definition in check profile:**

````json
{
  "type": "check_value_in_range",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a", "column_b"],
    "min_value": 10,
    "max_value": 100
  }
}
````

**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
>       **min_value**: ``int``
>
>       **max_value**: ``int``
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckValueInRange
# result will return True or False
result = CheckValueInRange(table=dataframe_object, columns=["column_a", "column_b"], min_value=10, max_value=100).run()
```

```python
from dqf.shared.checks import CheckValueInRange
# result will return True or False
result = CheckValueInRange(table=dataframe_object, columns="column_a", min_value=5, max_value=50).run()
```

---

## check_enum
**Description:**

> Checks whether one or more columns have values that are part of valid entries list.
>
> check can be applied for integer and string values.

**Example:**

> Consider a table with two columns `column_a` and `column_b` and enum list= [4,5,8]
>
> Returns ``True`` if all the distinct values of ``column_a`` are part of valid values list.
>
> Returns ``True`` if all the distinct values of ``column_a`` and ``column_b``  are part of valid values list.

> This will return ```True``` because all samples have valid values from list [4,5,8]:

| column_a | column_b |
| :---- | :---- |
| 5 |4|
| 8 |8|
| 4 |5|
| 4 |8|

> This will return ```False``` because second row has value 7 which is not valid:


| column_a | 
| :---- | 
| 5 |
| 7 |
| 8 |
| 5 |


**Definition in check profile:**

Example 1:
````json
{
  "type": "check_enum",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a"],
    "enum": ["MALE","FEMALE","OTHERS"],
    "case_sensitive": false, ...optional
  }
}
````

Example 2:
````json
{
  "type": "check_enum",
  "kwargs": {
    "table": "table_name",
    "columns": ["column_a", "column_b"],
    "enum": [4,3,7]
  }
}
````

**Python Class usage:**
> **Arguments:** 
>
>       **table**: ``str, pathlib.Path, DataFrame``
>
>       **columns**: ``List[str], Tuple[str], str``
>
>       **enum**: ``List[str], Tuple[str], str, List[int], Tuple[int], int``
> 
>       **case_sensitive**: ``bool`` -optional defaults to False (can be used for columns with string values)
>
> *returns*: ``True/False``

**Code Implementation**

```python
from dqf.shared.checks import CheckEnum
# result will return True or False
result = CheckEnum(table=dataframe_object, columns=["column_a", "column_b"], enum=["yes","no"], case_sensitive=True).run()
```

```python
from dqf.shared.checks import CheckEnum
# result will return True or False
result = CheckEnum(table=dataframe_object, columns="column_a", enum=[4,7,2]).run()
```

---

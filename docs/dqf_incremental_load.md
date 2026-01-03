# Incremental load functionality for DQF
> This functionality allows you to apply checks on filtered records for a definite period of time using specific incremental load keys in DQF configuration file.

The two keys used for incremental load functionality are:
> ###### 1. [incremental_column](#1.-incremental_column)
> ###### 2. [incremental_filter](#2.-incremental_filter)

# Incremental load Reference Guide

## 1. incremental_column
**Description:**

> This key allow you to specify the column name on which the `incremental_filter` should be applied on. The column should be of type **date**/**datetime**.

## 2. incremental_filter
**Description:**
> This key allows various options as value, that help filter the `incremental_column` based on time. 

The options available for `incremental_filter` are:

| Filter Value | Short Description | 
| :---- | :---- | 
| [previous_day](#previous_day) | One day's of records will be processed. |
| [previous_week](#previous_week) | Last one week's of records will be processed. |
| [previous_month](#previous_month) | Last one month's records will be processed. |
| [years=*no_of_years*](#years=no_of_years) | Number of years of records that you wish to process. Ex: `years=1` |
| [months=*no_of_months*](#months=no_of_months) | Number of months of records that you wish to process. Ex: `months=6` |
| [weeks=*no_of_weeks*](#weeks=no_of_weeks) | Number of weeks of records that you wish to process. Ex: `weeks=3` |
| [days=*no_of_days*](#days=no_of_days) | Number of days of records that you wish to process. Ex: `days=3` |
| [yyyy-mm-dd format](#yyyy-mm-dd-format) | Records from a certain date that you wish to process. Ex: `2021-02-01` |

**Example:**

> As an example, consider the table below with 4 columns `customer_id`, `customer_name`, `customer_since` and `car_id`. 
> 
> customer_id | customer_name | customer_since | car_id|
> :---- | :---- | :---- | :---- |
> 1 | Rob | 2017-01-02 | 132
> 2 | Kate | 2020-03-24 | 23
> 3 | Anne | 2020-07-15 | 45
> 4 | Louis | 2020-12-24 | 28
> 5 | Josh | 2021-01-06 | 99
> 6 | Harry | 2021-02-01 | 11
> 7 | Carter | 2021-07-11 | 01
> 8 | Mitchell | 2021-07-14 | 52
>
> To run DQF as incremental based on the column `customer_since`, which is of type *date*, we need to specify **incremental_column** value as `column_since` and **incremental_filter** as one the following, based on requirement. 

* #### previous_day

  > **Definition in DQF configuration:**
  > ````json
  > dqf_config = {
  >   "datasource_path": "/dbfs/mnt/projects/DataQualityFramework/Customer/",
  >   "profile_name": "customers_incremental_load",
  >   "data_format": "delta",
  >   "incremental_column": "customer_since",
  >   "incremental_filter": "previous_day",
  >   "checks": [
  >     { 
  >       "type": "check_datatype",
  >       "kwargs": {
  >         "table": "customers",
  >         "columns": ["customer_id", "car_id"],
  >         "d_type": "int"
  >        }
  >     }
  >   ]
  > }
  > ````
  > With `previous_day` **incremental_filter**, we can process the checks on the records for last one day.
  >
  > Below are the records that will be processed, if current date is `2021-07-15`:
  >
  > customer_id | customer_name | customer_since | car_id |
  > :---- | :---- | :---- | :---- |
  > 8 | Mitchell | 2021-07-14 | 52 |
         
* #### previous_week

  > **Definition in DQF configuration:**
  > ````json
  > dqf_config = {
  >   "datasource_path": "/dbfs/mnt/projects/DataQualityFramework/Customer/",
  >   "profile_name": "customers_incremental_load",
  >   "data_format": "delta",
  >   "incremental_column": "customer_since",
  >   "incremental_filter": "previous_week",
  >   "checks": [
  >     { 
  >       "type": "check_datatype",
  >       "kwargs": {
  >         "table": "customers",
  >         "columns": ["customer_id", "car_id"],
  >         "d_type": "int"
  >        }
  >     }
  >   ]
  > }
  > ````
  > With `previous_week` **incremental_filter**, we can process the checks on the records for last one week.
  >
  > Below are the records that will be processed, if current date is `2021-07-15`:
  >
  > customer_id | customer_name | customer_since | car_id |
  > :---- | :---- | :---- | :---- |
  > 7 | Carter | 2021-07-11 | 01 |
  > 8 | Mitchell | 2021-07-14 | 52 |

* #### previous_month

  > **Definition in DQF configuration:**
  > ````json
  > dqf_config = {
  >   "datasource_path": "/dbfs/mnt/projects/DataQualityFramework/Customer/",
  >   "profile_name": "customers_incremental_load",
  >   "data_format": "delta",
  >   "incremental_column": "customer_since",
  >   "incremental_filter": "previous_month",
  >   "checks": [
  >     { 
  >       "type": "check_datatype",
  >       "kwargs": {
  >         "table": "customers",
  >         "columns": ["customer_id", "car_id"],
  >         "d_type": "int"
  >        }
  >     }
  >   ]
  > }
  > ````
  > With `previous_month` **incremental_filter**, we can process the checks on the records for last one month.
  >
  > Below are the records that will be processed, if current date is `2021-07-15`:
  >
  > customer_id | customer_name | customer_since | car_id |
  > :---- | :---- | :---- | :---- |
  > 7 | Carter | 2021-07-11 | 01 |
  > 8 | Mitchell | 2021-07-14 | 52 |

* #### years=*no_of_years*

  > **Definition in DQF configuration:**
  > ````json
  > dqf_config = {
  >   "datasource_path": "/dbfs/mnt/projects/DataQualityFramework/Customer/",
  >   "profile_name": "customers_incremental_load",
  >   "data_format": "delta",
  >   "incremental_column": "customer_since",
  >   "incremental_filter": "years=1",
  >   "checks": [
  >     { 
  >       "type": "check_datatype",
  >       "kwargs": {
  >         "table": "customers",
  >         "columns": ["customer_id", "car_id"],
  >         "d_type": "int"
  >        }
  >     }
  >   ]
  > }
  > ````
  > With `previous_week` **incremental_filter**, we can process the checks on the records for last one year.
  >
  > Below are the records that will be processed, if current date is `2021-07-15`:
  >
  > customer_id | customer_name | customer_since | car_id |
  > :---- | :---- | :---- | :---- |
  > 5 | Josh | 2021-01-06 | 99 |
  > 6 | Harry | 2021-02-01 | 11 |
  > 7 | Carter | 2021-07-11 | 01 |
  > 8 | Mitchell | 2021-07-14 | 52 |
                                          
* #### months=*no_of_months*

  > **Definition in DQF configuration:**
  > ````json
  > dqf_config = {
  >   "datasource_path": "/dbfs/mnt/projects/DataQualityFramework/Customer/",
  >   "profile_name": "customers_incremental_load",
  >   "data_format": "delta",
  >   "incremental_column": "customer_since",
  >   "incremental_filter": "months=6",
  >   "checks": [
  >     { 
  >       "type": "check_datatype",
  >       "kwargs": {
  >         "table": "customers",
  >         "columns": ["customer_id", "car_id"],
  >         "d_type": "int"
  >        }
  >     }
  >   ]
  > }
  > ````
  > With `months=6` **incremental_filter**, we can process the checks on the records for last 6 months.
  >
  > Below are the records that will be processed, if current date is `2021-07-15`:
  >
  > customer_id | customer_name | customer_since | car_id |
  > :---- | :---- | :---- | :---- |
  > 6 | Harry | 2021-02-01 | 11 |
  > 7 | Carter | 2021-07-11 | 01 |
  > 8 | Mitchell | 2021-07-14 | 52 |
                           
* #### weeks=*no_of_weeks*

  > **Definition in DQF configuration:**
  > ````json
  > dqf_config = {
  >   "datasource_path": "/dbfs/mnt/projects/DataQualityFramework/Customer/",
  >   "profile_name": "customers_incremental_load",
  >   "data_format": "delta",
  >   "incremental_column": "customer_since",
  >   "incremental_filter": "weeks=3",
  >   "checks": [
  >     { 
  >       "type": "check_datatype",
  >       "kwargs": {
  >         "table": "customers",
  >         "columns": ["customer_id", "car_id"],
  >         "d_type": "int"
  >        }
  >     }
  >   ]
  > }
  > ````
  > With `weeks=3` **incremental_filter**, we can process the checks on the records for last 3 weeks.
  >
  > Below are the records that will be processed, if current date is `2021-07-15`:
  >
  > customer_id | customer_name | customer_since | car_id |
  > :---- | :---- | :---- | :---- |
  > 7 | Carter | 2021-07-11 | 01 |
  > 8 | Mitchell | 2021-07-14 | 52 |
                         
* #### days=*no_of_days*

  > **Definition in DQF configuration:**
  > ````json
  > dqf_config = {
  >   "datasource_path": "/dbfs/mnt/projects/DataQualityFramework/Customer/",
  >   "profile_name": "customers_incremental_load",
  >   "data_format": "delta",
  >   "incremental_column": "customer_since",
  >   "incremental_filter": "days=3",
  >   "checks": [
  >     { 
  >       "type": "check_datatype",
  >       "kwargs": {
  >         "table": "customers",
  >         "columns": ["customer_id", "car_id"],
  >         "d_type": "int"
  >        }
  >     }
  >   ]
  > }
  > ````
  > With `days=3` **incremental_filter**, we can process the checks on the records for last 3 days.
  >
  > Below are the records that will be processed, if current date is `2021-07-15`:
  >
  > customer_id | customer_name | customer_since | car_id |
  > :---- | :---- | :---- | :---- |
  > 8 | Mitchell | 2021-07-14 | 52 |
                       
* #### *yyyy-mm-dd* format

  > **Definition in DQF configuration:**
  > ````json
  > dqf_config = {
  >   "datasource_path": "/dbfs/mnt/projects/DataQualityFramework/Customer/",
  >   "profile_name": "customers_incremental_load",
  >   "data_format": "delta",
  >   "incremental_column": "customer_since",
  >   "incremental_filter": "2021-07-11",
  >   "checks": [
  >     { 
  >       "type": "check_datatype",
  >       "kwargs": {
  >         "table": "customers",
  >         "columns": ["customer_id", "car_id"],
  >         "d_type": "int"
  >        }
  >     }
  >   ]
  > }
  > ````
  > With `2021-07-11` **incremental_filter**, we can process the checks on the records from 11th of July, 2021.
  >
  > Below are the records that will be processed, if current date is `2021-07-15`:
  >
  > customer_id | customer_name | customer_since | car_id|
  > :---- | :---- | :---- | :---- |
  > 7 | Carter | 2021-07-11 | 01 |
  > 8 | Mitchell | 2021-07-14 | 52 |
   
*Please note, these are the only options supported in DQF configuration file for incremental load currently and should appear as in the examples.*

---
**Python Class usage:** 
> **Arguments:** 
>
> ````config: `Configuration of CheckProject` ````
>
> **returns** ````A list of (result, string) pairs with check validation error messages````

---
**Code Implementation**

```python
from dqf.main import CheckProject

# result will return list of (result, string) pairs with check validation error messages
result = CheckProject(config=dqf_config).run()
```
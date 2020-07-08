<a name=".mod6"></a>
# mod6

<a name=".Main"></a>
# Main

<a name=".Main.main"></a>
#### main

```python
main()
```

This main()function is all that anyone working on this project should need to alter. This provides sample code on how to retrieve a Pandas DataFrame from a KSIFeed object, and how to retrieve the column mapping for categorical data

<a name=".Main.ColumnType"></a>
## ColumnType Objects

```python
class ColumnType():
 |  ColumnType(name: str, datatype: str)
```

This base class represents a column in a data feed.
Initialize it with a datatype to give hints on how to transform the value.
Override transform_value() in order to have fine grained control on how to construct a dataframe

<a name=".Main.ColumnType.__str__"></a>
#### \_\_str\_\_

```python
 | __str__()
```

This returns a string representation of the ColumnType
If it is called before the data is parsed, categorical data will not yet be mapped
Therefore, this should be accessed via the __str__() method of the ColumnMapper object as the categorical data
is mapped on ColumnMapper creation and will always return the full mapping for categorical data

<a name=".Main.ColumnType.override_map"></a>
#### override\_map

```python
 | override_map(dict)
```

This is used by the ColumnMapper to handle ordinal values

<a name=".Main.ColumnType.transform_value"></a>
#### transform\_value

```python
 | transform_value(value: str)
```

Columns are transformed via the ollowing logicfollowing logic:
-  None values are returned as is
- "Yes" or "No" are retuyned as a Python bool
- if the number looks like an Integer, return a Python int
- if the number looks like an Float, return a Python float
- if the number is a categorical value, ie it did not pass the previous checks, then
build a map of the values as we encounter them and return a Python int. The value of the int
represents the order in which the categorical value appeared in the feed, these are not ordinal values.
In order to return ordinal values, subclass this object and pre-populate the ColumnMapper with an instance
of this subclass

<a name=".Main.KSIColumnType"></a>
## KSIColumnType Objects

```python
class KSIColumnType(ColumnType)
```

The KSI data feed has two significant problems that this object tries to address:
1. Objects are stored and returned as a string, even if the data is a DateTime, Integer, Float, or Boolean value
2. For categorical values, there is no list of possible values in the KSI documentation
Therefore, this object has two main functions:
1. Parse the appropriate Python datatype for the column
2. Maintain an internal map of Integer values for categorical data
In the case that we want the Integer values to be Ordinal, we will have to modify this code so that the ColumnMapper is pre-populated with some subclassess of ColumnType where necessary

<a name=".Main.KSIColumnType.transform_value"></a>
#### transform\_value

```python
 | transform_value(value: str)
```

This is the "meat" of the project. The logic is as follows:
1. None values are returned as is
2. Columns with sql datatype "Integer" are returned as a Python int
3. Columns with sql datatype "Date" are returned as a Python int
4. Columns with sql datatype "OID" are returned as a Python int
5. Columns with sql datatype "String" require the following logic:
    - "Yes" or "No" are retuyned as a Python bool
    - if the number looks like an Integer, return a Python int
    - if the number looks like an Float, return a Python float
    - if the number is a categorical value, ie it did not pass the previous checks, then
    build a map of the values as we encounter them and return a Python int. The value of the int
    represents the order in which the categorical value appeared in the feed, these are not ordinal values.
    In order to return ordinal values, subclass this object and pre-populate the ColumnMapper with an instance
    of this subclass

<a name=".Main.ColumnMapper"></a>
## ColumnMapper Objects

```python
class ColumnMapper():
 |  ColumnMapper()
```

This object builds a list of ColumnType objects from the "fields" section of the JSON. Use this object to interact with ColumnTypes instead of instatiating ColumnTypes directly

<a name=".Main.ColumnMapper.__str__"></a>
#### \_\_str\_\_

```python
 | __str__()
```

Prints out the current list of ColumnTypes
:returns str

<a name=".Main.ColumnMapper.load_columns_from_json"></a>
#### load\_columns\_from\_json

```python
 | load_columns_from_json(columns_json: dict)
```

to be called after calling set_ordinal_values() but before calling transform_value()

<a name=".Main.ColumnMapper.transform_value"></a>
#### transform\_value

```python
 | transform_value(value: tuple)
```

Convenience method to look up the correct ColumnType for a value
:argument tuple ( name of column , column value to transform
:returns the transformed value in the python data type that makes the most sense as per the
corresponding ColumnType object's transform_value() method

<a name=".Main.ColumnMapper.get_column_types"></a>
#### get\_column\_types

```python
 | get_column_types()
```

:returns list a list of all the ColumnTypes this object found inj the "fields" array of the JSON on init.

<a name=".Main.ColumnMapper.get_column_names"></a>
#### get\_column\_names

```python
 | get_column_names()
```

:returns list a list of all the column names, useful in creating a pandas DataFrame

<a name=".Main.KSIColumnMapper"></a>
## KSIColumnMapper Objects

```python
class KSIColumnMapper(ColumnMapper):
 |  KSIColumnMapper()
```

<a name=".Main.KSIColumnMapper.load_columns_from_json"></a>
#### load\_columns\_from\_json

```python
 | load_columns_from_json(columns_json: dict)
```

to be called after calling set_ordinal_values() but before calling transform_value()

<a name=".Main.Feed"></a>
## Feed Objects

```python
class Feed():
 |  Feed(baseQuery: str, mapper: ColumnMapper = ColumnMapper())
```

Abstract class representing a feed.  Override parse() to implement.

<a name=".Main.Feed.set_ordinal_values"></a>
#### set\_ordinal\_values

```python
 | set_ordinal_values(column_name: str, value_map: dict)
```

This must be called before parse

<a name=".Main.Feed.parse"></a>
#### parse

```python
 | parse(json: object = {})
```

Override this method in order to read a feed.

<a name=".Main.Feed.run"></a>
#### run

```python
 | run()
```

Calls parse.

<a name=".Main.Feed.get_query"></a>
#### get\_query

```python
 | get_query()
```

:returns the query that this object called the API with. Cut and paste into a browser to see the raw data

<a name=".Main.Feed.get_rows"></a>
#### get\_rows

```python
 | get_rows()
```

:returns list a python 2 dimensional list of mixed datatypes in consistent order

<a name=".Main.Feed.get_column_mapper"></a>
#### get\_column\_mapper

```python
 | get_column_mapper()
```

:returns ColumnMapper object

<a name=".Main.Feed.get_data_frame"></a>
#### get\_data\_frame

```python
 | get_data_frame()
```

:returns Pandas DataFrame. This is the main method of this class

<a name=".Main.PagedFeedConfiguration"></a>
## PagedFeedConfiguration Objects

```python
class PagedFeedConfiguration():
 |  PagedFeedConfiguration(page_size: int = 2000, page_size_param_name: str = "resultRecordCount", offset_param_name: str = "resultOffset")
```

This configuration assumes that the model for pagination is to set page size and page number as URL paramters.
This configuratrion does not support next page tokens embedded in response json

<a name=".Main.PagedFeed"></a>
## PagedFeed Objects

```python
class PagedFeed(Feed):
 |  PagedFeed(baseQuery: str, mapper: ColumnMapper = ColumnMapper(), paging_config: PagedFeedConfiguration = PagedFeedConfiguration())
```

This class extends Feed.  It handles the case where page size is limited by the server and you must iterate through many pages.
Page size and page number URL parameters are configured via the PagedFeedConfiguration object.

<a name=".Main.PagedFeed.get_json"></a>
#### get\_json

```python
 | get_json(page: int = 0)
```

:returns the json that the API returned, already parsed in Python
defaults to the first page retrieved unless you specify the page number

<a name=".Main.PagedFeed.get_query"></a>
#### get\_query

```python
 | get_query(page: int = 0)
```

:returns the query that this object called the API with. Cut and paste into a browser to see the raw data
defaults to the first page retrieved unless you specify the page number

<a name=".Main.PagedFeed.run"></a>
#### run

```python
 | run()
```

RECURSIVE METHOD that calls parse() repeatedly until the number of results is less than the page size
Due to time constraints, edge cases not tested.

<a name=".Main.PagedFeed.parse"></a>
#### parse

```python
 | parse(json: object = {})
```

Override this method in order to read a feed.

<a name=".Main.KSIFeed"></a>
## KSIFeed Objects

```python
class KSIFeed(PagedFeed):
 |  KSIFeed(index_start: int = None, index_end: int = None, year_start: int = None, year_end: int = None)
```

This is the main object for calling the KSI API and retrieving a Pandas DataFrame object.
You may also retrieve the ColumnMapper from this object to see how the values were mapped.
:argument index_start the starting index of the values we want to retrieve, if not set will retrieve all indexes
:argument index_end largest index of the values we want to retrieve, , if not set will retrieve all indexes.
index_start and index_end must both be set to be included in the query
:argument year_start the starting year of the values we want to retrieve, if not set will retrieve all indexes.
:argument year_end largest index of the values we want to retrieve, , if not set will retrieve all indexes.
year_start and year_end must both be set to be included in the query

<a name=".Main.KSIFeed.parse"></a>
#### parse

```python
 | parse(json: object = {})
```

This this will go get the data and populate the internal map.
It is called repeatedly by the run() method of the parent class until there are no more pages of rows


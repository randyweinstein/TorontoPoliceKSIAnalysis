<a name=".Main"></a>
# Main

<a name=".Main.main"></a>
#### main

```python
main()
```

This main()function is all that anyone working on this project should need to alter. This provides sample code on how to retrieve a Pandas DataFrame from a KSIFeed object, and how to retrieve the column mapping for categorical data

<a name=".Main.KSIFeed"></a>
## KSIFeed Objects

```python
class KSIFeed():
 |  KSIFeed(index_start: int = 1, index_end: int = 3389167)
```

This is the main object for calling the KSI API and retrieving a Pandas DataFrame object.
You may also retrieve the ColumnMapper from this object to see how the values were mapped.
:argument index_start the starting index of the values we want to retrieve, defaults to 0
:argument index_end largest index of the values we want to retrive, defaults to 3389167,
which gives us 167 rows at present.

<a name=".Main.KSIFeed.get_json"></a>
#### get\_json

```python
 | get_json()
```

:returns the json that the API returned, already parsed in Python

<a name=".Main.KSIFeed.get_query"></a>
#### get\_query

```python
 | get_query()
```

:returns the query that this object called the API with. Cut and paste into a browser to see the raw data

<a name=".Main.KSIFeed.get_rows"></a>
#### get\_rows

```python
 | get_rows()
```

:returns list a python 2 diment

<a name=".Main.ColumnType"></a>
## ColumnType Objects

```python
class ColumnType():
 |  ColumnType(name: str, datatype: str)
```

The KSI data feed has two significant problems that this object tries to address:
1. Objects are stored and returned as a string, even if the data is a DateTime, Integer, Float, or Boolean value
2. For categorical values, there is no list of possible values in the KSI documentation
Therefore, this object has two main functions:
1. Parse the appropriate Python datatype for the column
2. Maintain an internal map of Integer values for categorical data
In the case that we want the Integer values to be Ordinal, we will have to modify this code so that the ColumnMapper is pre-populated with some subclassess of ColumnType where necessary

<a name=".Main.ColumnType.__str__"></a>
#### \_\_str\_\_

```python
 | __str__()
```

This returns a string representation of the ColumnType
If it is called before the data is parsed, categorical data will not yet be mapped
Therefore, this should be accessed via the __str__() method of the ColumnMapper object as the categorical data
is mapped on ColumnMapper creation and will always return the full mapping for categorical data

<a name=".Main.ColumnType.transform_value"></a>
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
 |  ColumnMapper(columns_json: dict)
```

This object builds a list of ColumnType objects from the "fields" section of the JSON. Use this object to interact with ColumnTypes instead of instatiating ColumnTypes directly

<a name=".Main.ColumnMapper.__str__"></a>
#### \_\_str\_\_

```python
 | __str__()
```

Prints out the current list of ColumnTypes
:returns str

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


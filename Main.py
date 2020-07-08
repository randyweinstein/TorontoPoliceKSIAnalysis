import json
import urllib.request
import ssl
import numpy as np
import pandas as pd
import datetime
from datetime import timezone



# Internal note to Randy:
# To update the docs do the following:
# $ pipx install mkdocs
# $ pydoc-markdown --bootstrap-mkdocs
# $ pydoc-markdown --server --open-browser
# cp /build/docs/content/api-documentation.md /README.md

def main():
    """This main()function is all that anyone working on this project should need to alter. This provides sample code on how to retrieve a Pandas DataFrame from a KSIFeed object, and how to retrieve the column mapping for categorical data"""

    ksi = KSIFeed(year_start=2010, year_end=2019)

    print(ksi.get_query())

    injury = dict()
    injury["None"] = 1
    injury["Minimal"] = 2
    injury["Minor"] = 3
    injury["Major"] = 4
    injury["Fatal"] = 5
    ksi.set_ordinal_values("INJURY", injury)

    invage = dict()
    invage["0 to 4"] = 1
    invage["5 to 9"] = 2
    invage["10 to 14"] = 3
    invage["15 to 19"] = 4
    invage["20 to 24"] = 5
    invage["25 to 29"] = 6
    invage["30 to 34"] = 7
    invage["35 to 39"] = 8
    invage["40 to 44"] = 9
    invage["45 to 49"] = 10
    invage["50 to 54"] = 11
    invage["55 to 59"] = 12
    invage["60 to 64"] = 13
    invage["65 to 69"] = 14
    invage["70 to 74"] = 15
    invage["75 to 79"] = 16
    invage["80 to 84"] = 17
    invage["85 to 89"] = 18
    invage["90 to 94"] = 19
    invage["Over 95"] = 20
    invage["unknown"] = 21
    ksi.set_ordinal_values("INVAGE", invage)

    ksi.run()

    # if you want to see the legend for mapped categorical values
    # print("Column Mapping:")
    # print(ksi.get_column_mapper())

    # the main call. Here is your Pandas DataFrame
    df = ksi.get_data_frame()

    print(df.dtypes)
    print(df)
    print(df['VISIBILITY'])


# it is safe to ignore anything past this line.  I will own this code


class ColumnType:
    """This base class represents a column in a data feed.
    Initialize it with a datatype to give hints on how to transform the value.
    Override transform_value() in order to have fine grained control on how to construct a dataframe
    """

    def __init__(self, name: str, datatype: str):
        self.name: str = name
        self.datatype: str = datatype
        self.possible_values: dict = dict()

    def __str__(self):
        """This returns a string representation of the ColumnType
        If it is called before the data is parsed, categorical data will not yet be mapped
        Therefore, this should be accessed via the __str__() method of the ColumnMapper object as the categorical data
        is mapped on ColumnMapper creation and will always return the full mapping for categorical data"""
        current_map = "ColumnType Object: \n"
        current_map += "\tName: " + self.name + "\n"
        current_map += "\tType: " + self.datatype + "\n"
        for categorical_data in self.possible_values.keys():
            current_map += "\t\t" + str(self.possible_values[categorical_data]) + ":  " + categorical_data + "\n"
        return current_map

    def override_map(self, dict):
        """This is used by the ColumnMapper to handle ordinal values"""
        if self.possible_values.__len__() > 1:
            print("Cannot ovveride value map for " + self.name + ", the current map is not empty");
        else:
            self.possible_values = dict

    def transform_value(self, value: str):
        """Columns are transformed via the ollowing logicfollowing logic:
        -  None values are returned as is
        - "Yes" or "No" are retuyned as a Python bool
        - if the number looks like an Integer, return a Python int
        - if the number looks like an Float, return a Python float
        - if the number is a categorical value, ie it did not pass the previous checks, then
        build a map of the values as we encounter them and return a Python int. The value of the int
        represents the order in which the categorical value appeared in the feed, these are not ordinal values.
        In order to return ordinal values, subclass this object and pre-populate the ColumnMapper with an instance
        of this subclass"""
        # don't wast time on null values.  Just return the null value
        if value is None:
            return value
        if isinstance(value, str):
            if value == "Yes":
                return True
            if value == "No":
                return False

            # if this an integer masquerading as a string
            if value.isdigit():
                return int(value)

            # if this is a float masquerading as a string
            if value.isdecimal():
                return float(value)

            # if this is categorical data, we are are going to assign int values incrementally
            # and always return the same int for a given string value
            if value not in self.possible_values:
                self.possible_values[value] = int(len(self.possible_values) + 1)
            return self.possible_values[value]


class KSIColumnType(ColumnType):
    """The KSI data feed has two significant problems that this object tries to address:
    1. Objects are stored and returned as a string, even if the data is a DateTime, Integer, Float, or Boolean value
    2. For categorical values, there is no list of possible values in the KSI documentation
    Therefore, this object has two main functions:
    1. Parse the appropriate Python datatype for the column
    2. Maintain an internal map of Integer values for categorical data
    In the case that we want the Integer values to be Ordinal, we will have to modify this code so that the ColumnMapper is pre-populated with some subclassess of ColumnType where necessary"""

    def transform_value(self, value: str):
        """This is the "meat" of the project. The logic is as follows:
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
            of this subclass"""
        # don't wast time on null values.  Just return the null value
        if value is None:
            return value
        # if the values is already an integer just use it as is
        if self.datatype == "Integer":
            return int(value)

        # if the value is a date then we might want to treat it differently at some point
        if self.datatype == "Date":
            return int(value)

        # OID is a special type in KIS but it looks to just be an int
        if self.datatype == "OID":
            return int(value)

        # everything seems to be a string
        if self.datatype == "String":

            # if this value is boolean masquerading as a string
            if value == "Yes":
                return True
            if value == "No":
                return False

            # if this an integer masquerading as a string
            if value.isdigit():
                return int(value)

            # if this is a float masquerading as a string
            if value.isdecimal():
                return float(value)

            # if this is categorical data, we are are going to assign int values incrementally
            # and always return the same int for a given string value
            if value not in self.possible_values:
                self.possible_values[value] = int(len(self.possible_values) + 1)
            return self.possible_values[value]


class ColumnMapper:
    '''This object builds a list of ColumnType objects from the "fields" section of the JSON. Use this object to interact with ColumnTypes instead of instatiating ColumnTypes directly'''

    def __init__(self):
        self.columns: dict = dict()
        self.ordinals_override: dict = dict()

    def __str__(self):
        """Prints out the current list of ColumnTypes
        :returns str"""
        current_map: str = "ColumnMapper Object\n"
        for column_type in self.columns.values():
            current_map += str(column_type)
        return current_map

    def set_ordinal_values(self, column_name: str, value_map: dict):
        column_type: ColumnType = ColumnType(column_name, "OVERRIDE")
        column_type.override_map(value_map)
        self.ordinals_override[column_name] = column_type

    def load_columns_from_json(self, columns_json: dict):
        """ Override"""
        raise NotImplementedError()


    def transform_value(self, value: tuple):
        """Convenience method to look up the correct ColumnType for a value
        :argument tuple ( name of column , column value to transform
        :returns the transformed value in the python data type that makes the most sense as per the
        corresponding ColumnType object's transform_value() method"""
        column_type: ColumnType = self.columns[value[0]]
        return column_type.transform_value(value[1])

    def get_column_types(self):
        """:returns list a list of all the ColumnTypes this object found inj the "fields" array of the JSON on init."""
        return self.columns

    def get_column_names(self):
        """:returns list a list of all the column names, useful in creating a pandas DataFrame"""
        return self.columns.keys()


class KSIColumnMapper(ColumnMapper):

    def __init__(self):
        super().__init__()

    def set_ordinal_values(self, column_name: str, value_map: dict):
        column_type: KSIColumnType = KSIColumnType(column_name, "OVERRIDE")
        column_type.override_map(value_map)
        self.ordinals_override[column_name] = column_type

    def load_columns_from_json(self, columns_json: dict):
        """ to be called after calling set_ordinal_values() but before calling transform_value()"""
        for field in columns_json["fields"]:
            name = field["name"]
            sql_type: str = str(field["type"])
            formatted_type = sql_type[13:]
            self.columns[name] = KSIColumnType(name, formatted_type)
        for ordinal in self.ordinals_override.keys():
            self.columns[ordinal] = self.ordinals_override[ordinal]


class Feed:
    '''Abstract class representing a feed.  Override parse() to implement.'''

    # static SSL context for static _get_response() method
    context = ssl._create_unverified_context()

    def __init__(self, baseQuery: str, mapper: ColumnMapper = ColumnMapper()):
        self._column_mapper: mapper
        self._query = baseQuery
        self._rows = list();


    def set_ordinal_values(self, column_name: str, value_map: dict):
        """This must be called before parse"""
        self.get_column_mapper().set_ordinal_values(column_name, value_map)

    def parse(self, json:object = {}):
        """Override this method in order to read a feed."""
        raise NotImplementedError()

    def run(self):
        """Calls parse."""
        self._rows = self.parse()

    def get_query(self):
        """:returns the query that this object called the API with. Cut and paste into a browser to see the raw data"""
        return self._query

    def get_rows(self):
        """:returns list a python 2 dimensional list of mixed datatypes in consistent order"""
        return self._rows

    def get_column_mapper(self):
        """:returns ColumnMapper object"""
        return self._column_mapper

    def get_data_frame(self):
        """:returns Pandas DataFrame. This is the main method of this class"""
        df = pd.DataFrame(data=self._rows, columns=self.get_column_mapper().get_column_names())
        return df

    @staticmethod
    def _get_response(query):
        '''
        Installing certs in Python to open SSL connections is challenging.
        Based on the level of difficultly people had installing numpy & pandas, I decided to just circumvent the necessity by
        using _create_unverified_context().  This prints an error on a non 200 HTTP response code.
        :return: raw json
        '''
        open_url = urllib.request.urlopen(query, context=Feed.context)
        if open_url.getcode() == 200:
            data = open_url.read()
        else:
            print("Error receiving data", open_url.getcode())
        return data


class PagedFeedConfiguration:
    """This configuration assumes that the model for pagination is to set page size and page number as URL paramters.
    This configuratrion does not support next page tokens embedded in response json"""
    def __init__(self, page_size:int = 2000, page_size_param_name: str = "resultRecordCount", offset_param_name: str = "resultOffset"):
        self._page_size = page_size
        self._page_size_param_name = page_size_param_name
        self._offset_param_name = offset_param_name

    def get_page_size(self):
        return self._page_size

    def get_page_size_param_name(self):
        return self._page_size_param_name

    def get_offset_param_name(self):
        return self._offset_param_name

    def get_page_parameters(self, page: int = 0):
        offset = str(page * self.get_page_size())
        page_size = str(self.get_page_size())
        return "&" + self.get_offset_param_name() + "=" + offset + "&" + self.get_page_size_param_name() + "=" + page_size




class PagedFeed(Feed):
    """This class extends Feed.  It handles the case where page size is limited by the server and you must iterate through many pages.
    Page size and page number URL parameters are configured via the PagedFeedConfiguration object. """

    def __init__(self, baseQuery: str, mapper: ColumnMapper = ColumnMapper(), paging_config: PagedFeedConfiguration = PagedFeedConfiguration()):
        self._current_page: int = 0
        self._paging_config = paging_config
        self._json_parsed = list()
        self._rows = list()
        super().__init__(baseQuery, mapper)


    def get_json(self, page:int = 0):
        """:returns the json that the API returned, already parsed in Python
        defaults to the first page retrieved unless you specify the page number"""
        return self._json_parsed[page]

    def get_query(self, page: int = 0):
        """:returns the query that this object called the API with. Cut and paste into a browser to see the raw data
        defaults to the first page retrieved unless you specify the page number"""
        return self._query + self._paging_config.get_page_parameters(page)


    def run(self):
        """RECURSIVE METHOD that calls parse() repeatedly until the number of results is less than the page size
        Due to time constraints, edge cases not tested."""
        query:str = self.get_query(self._current_page)
        json_raw = Feed._get_response(query)
        json_parsed = json.loads(json_raw)
        self._json_parsed.append(json_parsed)
        rows = self.parse(json_parsed)
        for row in rows:
            self._rows.append(row)
        if rows.__len__() == self._paging_config.get_page_size():
            self._current_page = self._current_page + 1
            self.run()


    def parse(self, json:object = {}):
        """Override this method in order to read a feed."""
        raise NotImplementedError()


class KSIFeed(PagedFeed):
    """This is the main object for calling the KSI API and retrieving a Pandas DataFrame object.
    You may also retrieve the ColumnMapper from this object to see how the values were mapped.
    :argument index_start the starting index of the values we want to retrieve, if not set will retrieve all indexes
    :argument index_end largest index of the values we want to retrieve, , if not set will retrieve all indexes.
    index_start and index_end must both be set to be included in the query
    :argument year_start the starting year of the values we want to retrieve, if not set will retrieve all indexes.
    :argument year_end largest index of the values we want to retrieve, , if not set will retrieve all indexes.
    year_start and year_end must both be set to be included in the query"""

    def __init__(self, index_start: int = None, index_end: int = None, year_start: int = None, year_end: int = None):

        self._column_mapper = KSIColumnMapper()

        query: str = "https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/KSI/FeatureServer/0/query?&outFields=*&outSR=4326&f=json&resultRecordCount=16000&"
        # if type(index_start)__name__ == "int" and type(index_end) == "int":
        if isinstance(index_start, int) and isinstance(index_end, int):
            query.join("where=Index_%20%3E%3D%20", str(index_start), "%20AND%20Index_%20%3C%3D%20", str(index_end))
        if isinstance(year_start, int) and isinstance(year_end, int):
            # start = int(datetime.datetime(year=year_start, day=1, month=1, tzinfo=timezone.utc).timestamp())
            # end = int(datetime.datetime(year=year_end, day=1, month=1, tzinfo=timezone.utc).timestamp())
            query = query + "where=YEAR%20%3E%3D%20" + str(year_start) + "%20AND%20YEAR%20%3C%3D%20" + str(year_end)
        if query.find("where") == -1:
            query += "where=1%3D1"

        super().__init__(baseQuery=query, mapper=self._column_mapper)

    def parse(self, json: object = {}):
        """This this will go get the data and populate the internal map.
        It is called repeatedly by the run() method of the parent class until there are no more pages of rows"""
        # column names and metadata are stored in a parallel level as the data in the JSON.
        # We'll use this metadata to help parse the data
        if self._current_page == 0:
            self._column_mapper.load_columns_from_json(json)

        rows_json = json["features"]
        rows = list()
        for rows_json in rows_json:
            row = list()
            rows_json: dict = rows_json["attributes"]
            row_json = rows_json.items()
            for value in row_json:
                row.append(self._column_mapper.transform_value(value))
            rows.append(row)
        return rows

main()

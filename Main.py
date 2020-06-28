import json
import urllib.request
import ssl
import numpy as np
import pandas as pd
import datetime

debug = True

# Internal note to Randy:
# To update the docs do the following:
# $ pipx install mkdocs
# $ pydoc-markdown --bootstrap-mkdocs
# $ pydoc-markdown --server --open-browser
# cp /build/docs/content/api-documentation.md /README.md

def main():
    """This main()function is all that anyone working on this project should need to alter. This provides sample code on how to retrieve a Pandas DataFrame from a KSIFeed object, and how to retrieve the column mapping for categorical data"""
    if debug:
        print("debug mode ON")
    else:
        print("debug mode OFF")

    ksi = KSIFeed(index_start=1, index_end=3389167)

    if debug:
        # if you want to see what API call was made
        print("Query:")
        print(ksi.get_query())

        # if you want to see what the API returned
        print("Json:")
        print(ksi.get_json());

        # if you want to see the legend for mapped categorical values
        print("Column Mapping:")
        print(ksi.get_column_mapper())

    # the main call. Here is your Pandas DataFrame
    df = ksi.get_data_frame()

    print(df.dtypes)
    print(df)

# it is safe to ignore anything past this line.  I will own this code
class KSIFeed:
    """This is the main object for calling the KSI API and retrieving a Pandas DataFrame object.
    You may also retrieve the ColumnMapper from this object to see how the values were mapped.
    :argument index_start the starting index of the values we want to retrieve, defaults to 0
    :argument index_end largest index of the values we want to retrive, defaults to 3389167,
    which gives us 167 rows at present."""
    def __init__(self, index_start: int = 1, index_end: int = 3389167):
        self._query: str = "https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/KSI/FeatureServer/0/query?where=Index_%20%3E%3D%20" + str(
            index_start) + "%20AND%20Index_%20%3C%3D%20" + str(index_end) + "&outFields=*&outSR=4326&f=json"
        json_raw = self.__get_response(self._query)
        self._json_parsed = json.loads(json_raw)

        # column names and metadata are stored in a parallel level as the data in the JSON.
        # We'll use this metadata to help parse the data
        self._column_mapper: ColumnMapper = ColumnMapper(self._json_parsed)

        rows_json = self._json_parsed["features"]
        self._rows = list()
        for rows_json in rows_json:
            row = list()
            rows_json: dict = rows_json["attributes"]
            row_json = rows_json.items()
            for value in row_json:
                row.append(self._column_mapper.transform_value(value))
            self._rows.append(row)

    def get_json(self):
        """:returns the json that the API returned, already parsed in Python"""
        return self._json_parsed

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
        df = pd.DataFrame(data=self._rows, columns=self._column_mapper.get_column_names())
        return df

    def __get_response(self, query):
        '''
        Installing certs in Python to open SSL connections is challenging.
        Based on the level of difficultly people had installing numpy & pandas, I decided to just circumvent the necessity by
        using _create_unverified_context().  This prints an error on a non 200 HTTP response code.
        :return: raw json
        '''
        context = ssl._create_unverified_context()
        open_url = urllib.request.urlopen(query, context=context)
        if open_url.getcode() == 200:
            data = open_url.read()
        else:
            print("Error receiving data", open_url.getcode())
        return data


class ColumnType:
    """The KSI data feed has two significant problems that this object tries to address:
    1. Objects are stored and returned as a string, even if the data is a DateTime, Integer, Float, or Boolean value
    2. For categorical values, there is no list of possible values in the KSI documentation
    Therefore, this object has two main functions:
    1. Parse the appropriate Python datatype for the column
    2. Maintain an internal map of Integer values for categorical data
    In the case that we want the Integer values to be Ordinal, we will have to modify this code so that the ColumnMapper is pre-populated with some subclassess of ColumnType where necessary"""
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
    def __init__(self, columns_json: dict):
        self.columns: dict = dict()
        for field in columns_json["fields"]:
            name = field["name"]
            sql_type: str = str(field["type"])
            formatted_type = sql_type[13:]
            self.columns[name] = ColumnType(name, formatted_type)

    def __str__(self):
        """Prints out the current list of ColumnTypes
        :returns str"""
        current_map: str = "ColumnMapper Object\n"
        for column_type in self.columns.values():
            current_map += str(column_type)
        return current_map

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


main()

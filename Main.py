import json
import urllib.request
import ssl
import numpy as np
import pandas as pd
import datetime

index_start: int = 1
index_end: int = 3389167

query: str = "https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/KSI/FeatureServer/0/query?where=Index_%20%3E%3D%20" + str(index_start) + "%20AND%20Index_%20%3C%3D%20"+ str(index_end) + "&outFields=*&outSR=4326&f=json"

debug: bool = True


def main():
    if debug:
        print("debug mode ON")
    else:
        print("debug mode OFF")
    json_raw = get_response(query)
    json_parsed = json.loads(json_raw)
    if debug:
        print("json_parsed: ")
        print(json_parsed)

    # column names and metadata are stored in a parallel level as the data in the JSON. We'll use this metadata to help parse the data
    column_mapper: ColumnMapper = ColumnMapper(json_parsed)
    if debug:
        print(column_mapper)


    rowsJson = json_parsed["features"]
    rows = list()
    for rowJson in rowsJson:
        row = list()
        rows_json: dict = rowJson["attributes"]
        row_json = rows_json.items()
        for value in row_json:
            row.append(column_mapper.get_int_for_value(value))
        rows.append(row)

    df = pd.DataFrame(data=rows, columns=column_mapper.get_column_names())

    print(df)
    print(df.dtypes)

    if debug:
        print(column_mapper)


# Installing certs in Python to open SSL connections is challenging.
# Based on the level of difficultly people had installing numpy & pandas, I decided to just circumvent the necessity by
# using _create_unverified_context().  This prints an error on a non 200 HTTP response code.
def get_response(query):
    if (debug):
        print("calling KSI API with the following URL: ")
        print(query)
    context = ssl._create_unverified_context()
    open_url = urllib.request.urlopen(query, context=context)
    if open_url.getcode() == 200:
        data = open_url.read()
    else:
        print("Error receiving data", open_url.getcode())
    return data


class ColumnType:
    def __init__(self, name: str, datatype: str):
        self.name: str = name
        self.datatype: str = datatype
        self.possible_values: dict = dict()


    def __str__(self):
        current_map = "ColumnType Object: \n"
        current_map += "\tName: " + self.name + "\n"
        current_map += "\tType: " + self.datatype + "\n"
        for categorical_data in self.possible_values.keys():
            current_map += "\t\t" + str(self.possible_values[categorical_data]) + ":  " + categorical_data + "\n"
        return current_map

    def transform_value(self, value: str):
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
    def __init__(self, columns_json: dict):
        self.columns: dict = dict()
        for field in columns_json["fields"]:
            name = field["name"]
            sql_type: str = str(field["type"])
            formatted_type = sql_type[13:]
            self.columns[name] = ColumnType(name, formatted_type)

    def __str__(self):
        current_map: str = "ColumnMapper Object\n"
        for column_type in self.columns.values():
            current_map += str(column_type)
        return current_map


    def get_int_for_value(self, value: tuple):
        column_type: ColumnType = self.columns[value[0]]
        return column_type.transform_value(value[1]);

    def get_column_types(self):
        return self.columns

    def get_column_names(self):
        return self.columns.keys()


main()

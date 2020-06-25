import json
import urllib.request
import ssl
import numpy as np
import pandas as pd


query: str = "https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/KSI/FeatureServer/0/query?where=Index_%20%3E%3D%203389067%20AND%20Index_%20%3C%3D%203389167&outFields=*&outSR=4326&f=json"
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

    #column names and metadata are stored in a parallel level as the data in the JSON. We'll use this metadata to help parse the data
    columns = get_columns(json_parsed)
    if debug:
        print("columns: ")
        print(columns)

    data_array = get_rows(json_parsed)
    if debug:
        print("data_array: ")
        print(data_array)

    #df = pd.read_json(path_or_buf=dataRowsJson, orient="records")


def get_rows(json:dict):
    rows = []
    rowsJson = json["features"]
    for rowJson in rowsJson:
        row = []
        row_json:dict = rowJson["attributes"]
        row_values:tuple = row_json.items()
        for row_value in row_values:
            print(row_value)
            print(type(row_value))
    return rows

def categorical_to_ordinal_or_numerical(row: dict, ):

    values_array = []







# Installing certs in Python to open SSL connections is challenging.
# Based on the level of difficultly people had installing numpy & pandas, I decided to just circumvent the necessity by
# using _create_unverified_context().  This prints an error on a non 200 HTTP response code.
def get_response(query):
    if(debug):
        print("calling KSI API with the folling URL: ")
        print(query)
    context = ssl._create_unverified_context()
    open_url = urllib.request.urlopen(query, context=context)
    if open_url.getcode() == 200:
        data = open_url.read()
    else:
        print("Error receiving data", open_url.getcode())
    return data

def get_columns(json:dict):
    columns:dict = {}
    for field in json["fields"]:
        name = field["name"]
        sql_type:str = str(field["type"])
        columns[name] = sql_type[13:]
    return columns

main()
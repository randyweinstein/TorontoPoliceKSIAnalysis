import json
import urllib.request
import ssl
import numpy as np
import pandas as pd


query: str = "https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/KSI/FeatureServer/0/query?where=Index_%20%3E%3D%203389067%20AND%20Index_%20%3C%3D%203389167&outFields=*&outSR=4326&f=json"
debug: bool = True

def main():

    json_raw = get_response(query)
    json_parsed = json.loads(json_raw)
    print(debug)
    if debug:
        print(json_parsed)

    #column names and metadata are stored in a parallel level as the data in the JSON
    columnNames = []
    columnDataTypes =[]
    for field in json_parsed["fields"]:
        columnNames.append(field["name"])
        columnDataTypes.append(field["type"])
        if debug:
            print()

    #now that we have column headers let's get the actual data
    dataRowsJson = json_parsed["features"]
    for row in dataRowsJson:
        row_values = row["attributes"]
        print(row_values)
        print(type(row_values))

    #df = pd.read_json(path_or_buf=dataRowsJson, orient="records")





# Installing certs in Python to open SSL connections is challenging.
# Based on the level of difficultly people had installing numpy & pandas, I decided to just circumvent the necessity by
# using _create_unverified_context().  This prints an error on a non 200 HTTP response code.
def get_response(query):
    context = ssl._create_unverified_context()
    open_url = urllib.request.urlopen(query, context=context)
    if open_url.getcode() == 200:
        data = open_url.read()
    else:
        print("Error receiving data", open_url.getcode())
    return data

main()
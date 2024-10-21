import json
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from geoalchemy2 import Geometry, WKTElement

## overpass api url
url = 'http://overpass-api.de/api/interpreter'
query = """ 
[out:json];
(node['amenity'='restaurant'](-1.293, 36.815, -1.28, 36.83););
out center;
"""
## response object containing the data on restaurants within cbd
response = requests.get(url=url, params={'data': query})
## response object converted to json
data = response.json()

def extract_data(data:json):
    long = []
    lats = []
    names = []
    ids = []

    for i in range(len(data['elements'])):
        lon = data['elements'][i]['lon']
        lat = data['elements'][i]['lat']
        id = data['elements'][i]['id']
        if 'name' in data['elements'][i]['tags']:
            name = data['elements'][i]['tags']['name']
            ids.append(id)
            long.append(lon)
            lats.append(lat)
            names.append(name)
    print(f"length of ids list: {len(ids)}")
    print(f"length of longitude list: {len(long)}")
    print(f"length of latitude list: {len(lats)}")
    print(f"length of names list: {len(names)}")

    return ids, names, long, lats ## return the 4 lists

location_id, names, longitude, latitude = extract_data(data=data)

## creating a dictionary from the 4 previously extracted lists of data
## this dictionary will be used to create a pandas df and finally a geopandas gdf

def make_dict():
    restaurant_dict = {
        'location_id': location_id,
        'name': names,
        'longitude': longitude,
        'latitude': latitude
    }

    return restaurant_dict

restaurant_dict = make_dict()
## create a df and gdf from the previously created dictionary
def make_table(input_dict:dict):
    df = pd.DataFrame(input_dict)
    ##create a point geometry column so that data can be gdf compatible
    geometry = gpd.points_from_xy(df['longitude'], df['latitude'])
    ## create gdf from first 2 columns of df and the geometry geoseries just created
    gdf = gpd.GeoDataFrame(data=df[['location_id', 'name']], geometry=geometry, crs="EPSG:4326")
    print(gdf.head())
    return gdf

restaurant_gdf = make_table(input_dict=restaurant_dict)

##database connection information
user = 'postgres'
password = 'Pgadmin2024#'
host ='127.0.0.1'
port = 5432
database = 'nbo_cbd'

def get_connection(user:str, password:str, host:str, port:int, database:str):
    return create_engine(url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
        user, password, host, port, database
    ))

connection = get_connection(user=user, password=password, host=host, port=port, database=database)
print(connection)

restaurant_gdf.to_postgis(name='cbd_restaurant', con=connection, dtype={'geom':Geometry('POINT', srid=4326)}, if_exists='append')
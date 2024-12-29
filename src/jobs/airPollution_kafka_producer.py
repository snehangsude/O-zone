# type: ignore
import time
import json
import uuid
import pathlib
import aiohttp
import asyncio
import requests 
from datetime import datetime
from kafka import KafkaProducer 
from modules.YamlReader import YamlReader
from modules.class_BigQuery import BigQueryManager
from google.cloud import bigquery

# Predominatly for dev-testing
import sys

current_path = pathlib.Path(__file__)
config = YamlReader.read_config(f"{current_path.parents[1]}/config/config.yml")

API_KEY = config["openWeatherData_API"]
OPENWEATHER_ENDPOINT = config["openWeatherData_endpoint"]
OPENMETEO_ENDPOINT = config["openMeteoData_endpoint"]
BQ_LOCATION = config["biqQuery_location"]
BQ_PROJECT = config["bigQuery_projectId"]
BQ_DATASET = config["bigQuery_datasetId"]
BQ_TABLE = config["bigQuery_tableId"]




def get_coordinates():

    client = bigquery.Client()
    bigquery_engine = BigQueryManager(
        location=BQ_LOCATION,
        dataset_id=BQ_DATASET,
        table_id=BQ_TABLE, 
        client=client
    )

    job_config = bigquery.QueryJobConfig(use_query_cache=True)
    query = f"SELECT lat, lon, name FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` LIMIT 10"
    zipcodes = bigquery_engine.extract_data(query=query)
    return zipcodes

async def fetch_openWeather_data(session, zipcodes):
    
    url = f"{OPENWEATHER_ENDPOINT}/data/2.5/air_pollution"
    current_datetime = datetime.now().replace(second=0, microsecond=0)    
    timeblock_data = []

    for ele in zipcodes:
        params = {'lat': ele['lat'], 'lon': ele['lon'], 'appid': API_KEY}
        async with session.get(url=url, params=params) as response:
            openWeather_data = await response.json()
            normalized_data = {
                'message_id' : uuid.uuid4().hex,
                'date': current_datetime.strftime('%Y-%m-%d'),
                'time': current_datetime.strftime('%H:%M'),    
                'lat': openWeather_data['coord']['lat'],
                'lon': openWeather_data['coord']['lon'],
                'city': ele['name'],
                'aqi': openWeather_data['list'][0]['main']['aqi'],
                'carbon_monoxide': openWeather_data['list'][0]['components']['co'],
                'nitrogen_monoxide': openWeather_data['list'][0]['components']['no'],
                'nitrogen_dioxide': openWeather_data['list'][0]['components']['no2'],
                'ozone': openWeather_data['list'][0]['components']['o3'],
                'sulphur_dioxide': openWeather_data['list'][0]['components']['so2'],
                'fine_particles2.5': openWeather_data['list'][0]['components']['pm2_5'],
                'coarse_particles10': openWeather_data['list'][0]['components']['pm10'],
                'ammonia': openWeather_data['list'][0]['components']['nh3']
            }
            timeblock_data.append(normalized_data)

    return timeblock_data


async def fetch_openMeteo_data(session, zipcodes):
    current_datetime = datetime.now().replace(second=0, microsecond=0)
    timeblock_data = []
    
    for ele in zipcodes:
        params = {
            "latitude": ele['lat'],
            "longitude": ele['lon'],
            "current": ["us_aqi", "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", 
                        "sulphur_dioxide", "ozone", "aerosol_optical_depth", "dust", 
                        "uv_index", "uv_index_clear_sky"]
        }
        
        async with session.get(OPENMETEO_ENDPOINT, params=params) as response:
            openMeteo_data = await response.json() 
            current_data = openMeteo_data['current']    
        
            normalized_data = {
                'message_id': uuid.uuid4().hex,
                'date': current_datetime.strftime('%Y-%m-%d'),
                'time': current_datetime.strftime('%H:%M'),
                'lat': ele['lat'],
                'lon': ele['lon'],
                'city': ele['name'],
                'aqi': current_data['us_aqi'],
                'carbon_monoxide': current_data['carbon_monoxide'],
                'nitrogen_dioxide': current_data['nitrogen_dioxide'],
                'ozone': current_data['ozone'],
                'sulphur_dioxide': current_data['sulphur_dioxide'],
                'fine_particles2.5': current_data['pm2_5'],
                'coarse_particles10': current_data['pm10'],
                'aerosol_optical_depth': current_data['aerosol_optical_depth'],
                'dust': current_data['dust'],
                'uv_index': current_data['uv_index'],
                'uv_index_clear_sky': current_data['uv_index_clear_sky']
            }
            timeblock_data.append(normalized_data)
    return timeblock_data


async def produce_data():

    zipcodes = get_coordinates()
    kafka_producer_openweather = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    kafka_producer_openmeteo = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


    async with aiohttp.ClientSession() as session:
        while True:
            openWeather_Data = await fetch_openWeather_data(session, zipcodes)
            openMeteo_data = await fetch_openMeteo_data(session, zipcodes)

            for message in openWeather_Data:
                kafka_producer_openweather.send(
                    topic='openWeather_pollution_data',
                    value=message
                )
                print(f"Produced to OpenWeather topic: {message}\n")

            for message in openMeteo_data:
                kafka_producer_openmeteo.send(
                    topic='openMeteo_pollution_data',
                    value=message
                )
                print(f"Produced to OpenMeteo topic: {message}\n")

            kafka_producer_openweather.flush()
            kafka_producer_openmeteo.flush()
            
            await asyncio.sleep(10)



def main():
    asyncio.run(produce_data())

if __name__ == "__main__":
    main()
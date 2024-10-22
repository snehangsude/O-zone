# type: ignore
import time
import json
import pathlib
import requests 
from datetime import datetime
from kafka import KafkaProducer 
import uuid
from modules.YamlReader import YamlReader
from modules.class_BigQuery import BigQueryManager
from google.cloud import bigquery

# Predominatly for dev-testing
import sys

current_path = pathlib.Path(__file__)
config = YamlReader.read_config(f"{current_path.parents[1]}/config/config.yml")

API_KEY = config["openWeatherData_API"]
OPENWEATHER_ENDPOINT = config["openWeatherData_endpoint"]
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
    query = f"SELECT lat, lon, name FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` LIMIT 10"
    zipcodes = bigquery_engine.extract_data(query=query)
    return zipcodes

def get_openWeather_airPollution_data():
    
    url = f"{OPENWEATHER_ENDPOINT}/data/2.5/air_pollution"
    current_datetime = datetime.now().replace(second=0, microsecond=0)
    
    timeblock_data = []
    zipcodes = get_coordinates()

    for ele in zipcodes:
        ele['appid'] = API_KEY
        openWeather_data = requests.get(url=url, params=ele).json()
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



def main():

    kafka_producer = KafkaProducer(
          bootstrap_servers = "localhost:9092",
          value_serializer = lambda v: json.dumps(v).encode("utf-8"),
    )

    while True:
        airPollution_data = get_openWeather_airPollution_data()
        for messages in airPollution_data:
            kafka_producer.send(
                topic='openWeather_pollution_data',
                value=messages
            )
            print(f"Produced: {messages}\n")
            kafka_producer.flush()
        
        time.sleep(10)

if __name__ == "__main__":
    main()
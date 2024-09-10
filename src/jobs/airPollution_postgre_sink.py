# type: ignore

import json
import logging
from datetime import datetime

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import JdbcSink
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
)
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

KAFKA_HOST = "kafka:19092"
POSTGRES_HOST = "postgres:5432"

def parse_data(data: str) -> Row:
    data = json.loads(data)
    date = datetime.strptime(data['date'], "%Y-%m-%d")
    time = data['time']
    lat = data['lat']
    lon = data['lon']
    city = data['city']
    aqi = data['aqi']
    carbon_monoxide = data['carbon_monoxide']
    nitrogen_monoxide = data['nitrogen_monoxide']
    nitrogen_dioxide = data['nitrogen_dioxide']
    ozone = data['ozone']
    sulphur_dioxide = data['sulphur_dioxide']
    fine_particles2_5 = data['fine_particles2.5']
    coarse_particles10 = data['coarse_particles10']
    ammonia = data['ammonia']
    
    return Row(
        date, 
        time,
        lat,
        lon,
        city,
        aqi,
        carbon_monoxide,
        nitrogen_monoxide,
        nitrogen_dioxide,
        ozone,
        sulphur_dioxide,
        fine_particles2_5,
        coarse_particles10,
        ammonia
    )

# TODO: PUT AQI THRESHOLD AND START THE CODE
# TODO 1: Get the data for the threshold or condition to keep
def filter_temperatures(value: str) -> str | None:
    AQI_THRESHOLD = 3
    data = json.loads(value)
    date = datetime.strptime(data['date'], "%Y-%m-%d")
    time = data['time']
    lat = data['lat']
    lon = data['lon']
    city = data['city']
    aqi = data['aqi']
    carbon_monoxide = data['carbon_monoxide']
    nitrogen_monoxide = data['nitrogen_monoxide']
    nitrogen_dioxide = data['nitrogen_dioxide']
    ozone = data['ozone']
    sulphur_dioxide = data['sulphur_dioxide']
    fine_particles2_5 = data['fine_particles2.5']
    coarse_particles10 = data['coarse_particles10']
    ammonia = data['ammonia']
    if aqi > AQI_THRESHOLD:
        alert_message = {
            "date" : date,
            "time" : time,
            "lat" : lat,
            "lon" : lon,
            "city" : city,
            "aqi" : aqi,
            "carbon_monoxide" : carbon_monoxide,
            "nitrogen_monoxide" : nitrogen_monoxide,
            "nitrogen_dioxide" : nitrogen_dioxide,
            "ozone" : ozone,
            "sulphur_dioxide" : sulphur_dioxide,
            "fine_particles2_5" : fine_particles2_5,
            "coarse_particles10" : coarse_particles10,
            "ammonia" : ammonia,
            "message": "Pollution Level High"
        }
        return json.dumps(alert_message)
    return None


def initialize_env() -> StreamExecutionEnvironment:
    """Makes stream execution environment initialization"""
    env = StreamExecutionEnvironment.get_execution_environment()

    # Get current directory
    root_dir_list = __file__.split("/")[:-2]
    root_dir = "/".join(root_dir_list)

    # Adding the jar to the flink streaming environment
    env.add_jars(
        f"file://{root_dir}/lib/flink-connector-jdbc-3.1.2-1.18.jar",
        f"file://{root_dir}/lib/postgresql-42.7.3.jar",
        f"file://{root_dir}/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
    )
    return env


def configure_source(server: str, earliest: bool = False) -> KafkaSource:
    """Makes kafka source initialization"""
    properties = {
        "bootstrap.servers": server,
        "group.id": "air-pollution-data",
    }

    offset = KafkaOffsetsInitializer.latest()
    if earliest:
        offset = KafkaOffsetsInitializer.earliest()

    kafka_source = (
        KafkaSource.builder()
        .set_topics("openWeather_pollution_data")
        .set_properties(properties)
        .set_starting_offsets(offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return kafka_source


def configure_postgre_sink(sql_dml: str, type_info: Types) -> JdbcSink:
    """Makes postgres sink initialization. Config params are set in this function."""
    return JdbcSink.sink(
        sql_dml,
        type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(f"jdbc:postgresql://{POSTGRES_HOST}/airPollution_db")
        .with_driver_name("org.postgresql.Driver")
        .with_user_name("admin_user")
        .with_password("user_admin")
        .build(),
        JdbcExecutionOptions.builder()
        .with_batch_interval_ms(1000)
        .with_batch_size(200)
        .with_max_retries(5)
        .build(),
    )


def configure_kafka_sink(server: str, topic_name: str) -> KafkaSink:

    return (
        KafkaSink.builder()
        .set_bootstrap_servers(server)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic_name)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


# TODO 2: Make changes to DML and schema
def main() -> None:
    """Main flow controller"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    # Initialize environment
    logger.info("Initializing environment")
    env = initialize_env()

    # Define source and sinks
    logger.info("Configuring source and sinks")
    kafka_source = configure_source(KAFKA_HOST)
    sql_dml = (
    "INSERT INTO air_pollution_data (date, time, lat, lon, city, aqi, carbon_monoxide, nitrogen_monoxide, nitrogen_dioxide, ozone, sulphur_dioxide, fine_particles2_5, coarse_particles10, ammonia, message) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

    TYPE_INFO = Types.ROW(
        [
            Types.SQL_DATE(),  # date
            Types.STRING(), # time
            Types.FLOAT(),  # lat
            Types.FLOAT(),  # lon
            Types.STRING(), # city
            Types.INT(),    # aqi
            Types.FLOAT(),  # carbon_monoxide
            Types.FLOAT(),  # nitrogen_monoxide
            Types.FLOAT(),  # nitrogen_dioxide
            Types.FLOAT(),  # ozone
            Types.FLOAT(),  # sulphur_dioxide
            Types.FLOAT(),  # fine_particles2_5
            Types.FLOAT(),  # coarse_particles10
            Types.FLOAT(),  # ammonia
            Types.STRING(), # message
        ]
    )

    jdbc_sink = configure_postgre_sink(sql_dml, TYPE_INFO)
    kafka_sink = configure_kafka_sink(KAFKA_HOST, "high_pollution_data")
    logger.info("Source and sinks initialized")

    # Create a DataStream from the Kafka source and assign watermarks
    data_stream = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka sensors topic"
    )

    # Make transformations to the data stream
    transformed_data = data_stream.map(parse_data, output_type=TYPE_INFO)
    alarms_data = data_stream.map(
        filter_temperatures, output_type=Types.STRING()
    ).filter(lambda x: x is not None)
    logger.info("Defined transformations to data stream")

    logger.info("Ready to sink data")
    alarms_data.print()
    alarms_data.sink_to(kafka_sink)
    transformed_data.add_sink(jdbc_sink)

    # Execute the Flink job
    env.execute("OpenWeather: Air Pollution Data")


if __name__ == "__main__":
    main()

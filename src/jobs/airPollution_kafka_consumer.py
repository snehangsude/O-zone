import json

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource


def main() -> None:
    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Get current directory
    current_dir_list = __file__.split("/")[:-1]
    current_dir = "/".join(current_dir_list)

    # Adding the jar to the flink streaming environment
    env.add_jars(
        f"file://{current_dir}/flink-sql-connector-kafka-3.1.0-1.18.jar"
    )

    properties = {
        "bootstrap.servers": "kafka:19092",
        "group.id": "airPollution-data",
    }

    earliest = False
    offset = (
        KafkaOffsetsInitializer.earliest()
        if earliest
        else KafkaOffsetsInitializer.latest()
    )

    # Create a Kafka Source
    # NOTE: FlinkKafkaConsumer class is deprecated
    kafka_source = (
        KafkaSource.builder()
        .set_topics("high_pollution_data")
        .set_properties(properties)
        .set_starting_offsets(offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Create a DataStream from the Kafka source and assign watermarks
    data_stream = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka sensors topic"
    )

    # Print line for readablity in the console
    print("start reading data from kafka")

    # Filter events with temperature above threshold
    airPollution = data_stream.filter(lambda x: x is not None)

    # Show the alerts in the console
    airPollution.print()

    # Execute the Flink pipeline
    env.execute("Kafka Sensor Consumer")


if __name__ == "__main__":
    main()

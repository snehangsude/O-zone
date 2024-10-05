### Execute in order


docker compose exec kafka kafka-topics --create --topic openWeather_pollution_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

docker compose exec kafka kafka-topics --create --topic high_pollution_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

docker exec -it o-zone-postgres-1 psql -U admin_user -d airPollution_db


CREATE TABLE air_pollution_data (
    message_id VARCHAR(255) PRIMARY KEY,
    date VARCHAR(255) NOT NULL,
    time VARCHAR(255) NOT NULL,
    lat FLOAT NOT NULL,
    lon FLOAT NOT NULL,
    city VARCHAR(255) NOT NULL,
    aqi INT NOT NULL,
    carbon_monoxide FLOAT NOT NULL,
    nitrogen_monoxide FLOAT NOT NULL,
    nitrogen_dioxide FLOAT NOT NULL,
    ozone FLOAT NOT NULL,
    sulphur_dioxide FLOAT NOT NULL,
    fine_particles2_5 FLOAT NOT NULL,
    coarse_particles10 FLOAT NOT NULL,
    ammonia FLOAT NOT NULL
);


docker compose exec flink-jobmanager flink run -py /opt/air-pollution/jobs/airPollution_postgre_sink.py


Check: docker-compose exec flink-jobmanager /bin/bash



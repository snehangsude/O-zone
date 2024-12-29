#!/bin/bash

GREEN='\033[0;32m'
NC='\033[0m'

# Create Kafka topics
echo "========================================="
echo -e "${GREEN}Creating Kafka topics...${NC}"
echo
docker compose exec kafka kafka-topics --create --topic openWeather_pollution_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker compose exec kafka kafka-topics --create --topic openMeteo_pollution_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker compose exec kafka kafka-topics --create --topic openweather_high_pollution --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker compose exec kafka kafka-topics --create --topic openmeteo_high_pollution --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

echo
echo -e "${GREEN}Kafka topics created successfully.${NC}"
echo

# Connect to PostgreSQL and create tables
echo -e "${GREEN}Creating tables in PostgreSQL...${NC}"
echo
docker exec -i o-zone-postgres-1 psql -U admin_user -d airPollution_db <<EOF
CREATE TABLE pollutantsOpenWeather (
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

CREATE TABLE pollutantsOpenMeteo (
    message_id VARCHAR(255) PRIMARY KEY,
    date VARCHAR(255) NOT NULL,
    time VARCHAR(255) NOT NULL,
    lat FLOAT NOT NULL,
    lon FLOAT NOT NULL,
    city VARCHAR(255) NOT NULL,
    aqi INT NOT NULL,
    carbon_monoxide FLOAT NOT NULL,
    nitrogen_dioxide FLOAT NOT NULL,
    ozone FLOAT NOT NULL,
    sulphur_dioxide FLOAT NOT NULL,
    fine_particles2_5 FLOAT NOT NULL,
    coarse_particles10 FLOAT NOT NULL,
    aerosol_optical_depth FLOAT NOT NULL,
    dust FLOAT NOT NULL,
    uv_index FLOAT NOT NULL,
    uv_index_clear_sky FLOAT NOT NULL
);
EOF

echo
echo -e "${GREEN}Tables created successfully.${NC}"
echo "============================================="



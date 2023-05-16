set -e

export BOOTSTRAP_SERVERS="localhost:9092"
export INPUT_DATA_PATH="data/yellow_tripdata_2023-01.parquet"
export KAFKA_TOPIC="trips"

echo "created envonment variables!!"
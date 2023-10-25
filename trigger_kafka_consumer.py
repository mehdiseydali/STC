# Function to trigger the Kafka consumer in Spark Streaming
def trigger_kafka_consumer(worker_count, file_dict):
    # Add your code here to trigger the Kafka consumer in Spark Streaming
    print("Kafka consumer in Spark Streaming is triggered.")
    create_spark_streaming_cluster(worker_count, file_dict)
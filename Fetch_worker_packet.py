"""
To achieve the described functionality using Python with Spark Streaming and Kafka, you can follow these steps:

Set up your Spark Streaming application with the necessary dependencies for Kafka integration.
Consume data from the Kafka topic called "packet topics" using Spark Streaming.
Preprocess the packets by filtering out those that do not contain data in their data field.
Determine the total number of packets stored on the brokers.
Distribute the packets among the workers based on the number of workers.
Process and store the allocated packets on each worker.
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

def determine_start_end(worker_id, packets_per_worker, total_packets):
    # Calculate the start and end positions for packets assigned to this worker
    start_packet = worker_id * packets_per_worker
    end_packet = min((worker_id + 1) * packets_per_worker - 1, total_packets - 1)
    return start_packet, end_packet

def extract_header_payload_packets(packet):
    # Function to extract header, payload, and label from packets
    if 'data' in packet and packet['data']:
        label = packet.get('label', "")  # Extract the label
        header = packet.get('header', "")
        payload = packet.get('data', "")
        # Pad or truncate the payload to a length of 1500
        if len(payload) < 1500:
            payload = payload.ljust(1500, '0')
        else:
            payload = payload[:1500]
        return {"label": label, "header": header, "data": payload}
    else:
        # Return None for packets that do not meet the criteria
        return None

def process_packets(rdd, worker_id, packets_per_worker):
    packets = rdd.collect()
    start_packet, end_packet = determine_start_end(worker_id, packets_per_worker, len(packets))
    packets_to_process = packets[start_packet:end_packet + 1]

    processed_packets = []  # To store the processed packets

    for packet in packets_to_process:
        processed_packet = extract_header_payload_packets(packet, packet['label'])
        if processed_packet:
            # Process and store the packet as required
            processed_packets.append(processed_packet)
            # Normalize the 'data' field to be between 0 and 1
            normalized_data = normalize_data(processed_packet['data'])
            processed_packet['data'] = normalized_data

    # You can work with the processed packets as needed
    for packet in processed_packets:
        pass
def normalize_data(data):
    # Perform normalization of data field to be between 0 and 1
    # You can use techniques like min-max scaling or other normalization methods
    max_value = max(data)
    normalized_data = [value / max_value for value in data]
    return normalized_data
def Fetch_worker_packet(N):
    # Initialize Spark context
    sc = SparkContext(appName="PacketProcessingApp")
    ssc = StreamingContext(sc, batchDuration=5)  # Adjust batch duration as needed

    # Create a Kafka stream to consume packets
    kafkaParams = {"metadata.broker.list": "kafka-broker-server"}
    topic = "packet topics"
    kafkaStream = KafkaUtils.createStream(ssc, "worker-group", {topic: 1}, kafkaParams)

    # Determine the total number of packets
    total_packets = kafkaStream.count()

    # Distribute packets among workers
    num_workers = N  # Set the number of workers
    packets_per_worker = total_packets // num_workers

    # Worker ID based on Spark application ID
    worker_id = int(sc.applicationId) % num_workers

    kafkaStream.foreachRDD(lambda rdd: process_packets(rdd, worker_id, packets_per_worker))

    # Start the Spark Streaming context
    ssc.start()
    ssc.awaitTermination()

# Example usage:
# Fetch_worker_packet(5)  # Call the function with the desired number of workers (e.g., 5)

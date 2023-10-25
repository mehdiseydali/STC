from scapy.all import rdpcap
from kafka import KafkaProducer
import time

from trigger_kafka_consumer import trigger_kafka_consumer

def Kafka_producer(pcap_files, kafka_topic, kafka_broker, duration, worker_count, file_dict):
    # Initialize the Kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_broker)

    # Get the start time
    start_time = time.time()

    for pcap_file in pcap_files:
        # Read packets from the PCAP file
        packets = rdpcap(pcap_file)

        for packet in packets:
            try:
                # Send the packet to Kafka
                producer.send(kafka_topic, value=packet.build())
                print(f"Sent packet to Kafka: {packet.summary()}")

                # Check if the specified time duration has passed
                if time.time() - start_time >= duration:
                    print(f"Received packets for {duration} seconds. Training the model now.")
                   
                    # Trigger your Kafka consumer in Spark Streaming here
                    trigger_kafka_consumer(worker_count, file_dict)
                    # Add your training logic here
                    break  # Exit the loop after the specified time duration

            except Exception as e:
                print(f"Error sending packet to Kafka: {str(e)}")

    producer.close()



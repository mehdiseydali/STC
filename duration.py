# Example usage:
pcap_files_to_replay = ["file1.pcap", "file2.pcap"]
Kafka_producer(pcap_files_to_replay, "pcap_packets", "localhost:9092", 60)
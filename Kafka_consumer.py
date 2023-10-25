from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Conv1D, GRU, Dense

from confluent_kafka.admin import AdminClient

from packet_normalization import packet_normalization
from extract_header_payload_packets import extract_header_payload_packets
from define_Bi-GRU_model_params import define_Bi-GRU_model_params
from cnn_build_model import cnn_build_model
from Bi-GRU_build_model import Bi-GRU_build_model
from defin_1D-CNN_model_params import defin_1D-CNN_model_params
from fc_build_model import fc_build_model
from define_FC_model_params import define_FC_model_params
from cnn_Traffic_classification import cnn_Traffic_classification
from Bi-LSTM_Traffic_classification  import Bi-LSTM_Traffic_classification 

def determine_start_end(worker_id, packets_per_worker, total_packets):
    # Calculate the start and end positions for packets assigned to this worker
    start_packet = worker_id * packets_per_worker
    end_packet = min((worker_id + 1) * packets_per_worker - 1, total_packets - 1)
    return start_packet, end_packet

def fetch_kafka_brokers(bootstrap_servers):
    # Define an AdminClient with your Kafka broker configuration
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # Fetch the list of brokers
    metadata = admin_client.list_topics()
    brokers = metadata.brokers

    # Extract the broker host and port information
    broker_info = ["{}:{}".format(broker.host, broker.port) for broker in brokers.values()]

    return broker_info

 # Function to build and train the TensorFlow model (worker-specific)
def build_and_train_model(rdd, worker_index, file_dict, batch_size):
    root_normalized_dir = 'media/mehdi/linux/normalized_data/'
    # Initialize a TensorFlow session for each executor (worker node)
    # define network model parameters ( hyper parameters )
    net_params = network_parameters_initializer()

    #Define 1D-CNN the model parameters
    cnn_model_params = defin_1D-CNN_model_params()
    # Define Bi-GRU the model parameters
    Bi-GRU_model_params = define_Bi-GRU_model_params()
    
    with tf.Session() as sess:
        #model = build_tensorflow_model()  # Implement this function to create your model
        for packet_data in rdd.collect():
            extracted_packet_root_dir = extract_header_payload_packets(packet_data,file_dict)  # Implement this function
            cnn_output,cnn_saved_model,cnn_saved_weights,1d-cnn_path = cnn_Traffic_classification(root_normalized_dir,net_params,cnn_model_params, batch_size,worker_index)
            
            bi-GRU_output,biGRU_saved_model,biGRU_saved_weights,bi-GRU_path = Bi-GRU_Traffic_classification(root_normalized_dir,net_params,Bi-GRU_model_params, batch_size,worker_index)
            model.train_on_batch(preprocessed_data, label)
            # Use the trained model for further processing or save it if needed
        # Combine the outputs into a single input array
        fc_model_params = define_FC_model_params()
        mlp_model = FC-traffic-classification(root_normalized_dir,net_params,fc_model_params, 1d-cnn_path,cnn_saved_model,bi-GRU_path,biGRU_saved_model,worker_index)
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
def normalize_data(data):
    # Perform normalization of data field to be between 0 and 1
    # You can use techniques like min-max scaling or other normalization methods
    max_value = max(data)
    normalized_data = [value / max_value for value in data]
    return normalized_data
    
    
def aggregate_weights(worker_weights, shared_weights):
    for worker_id, local_weights in worker_weights:
        shared_weights.value[worker_id] = local_weights
    averaged_weights = np.mean([np.array(w) for w in shared_weights.value], axis=0)
    shared_weights.value = [tf.Variable(averaged_weights, dtype=tf.float32) for _ in range(num_workers)]
    
def create_spark_streaming_cluster(worker_count, file_dict):    
    files = []
    for file in os.listdir(extracted_packet_root_dir):
        if os.path.isfile(os.path.join(extracted_packet_root_dir, file)):
        files.append(file)


    # Define your Spark application name
    app_name = "SparkStreamingTensorFlowApp"

    # Define the master URL (use the Spark Master URL of your cluster)
    master_url = "spark://spark-master:7077"  # Replace with your Spark Master URL

    # Create a SparkContext with the master URL and application name
    sc = SparkContext(master=master_url, appName=app_name)

    # Create a StreamingContext with the SparkContext and batch duration
    batch_duration = 1  # Adjust as needed
    ssc = StreamingContext(sc, batch_duration)

    # Define the Kafka parameters for each worker node
    # Define your Kafka bootstrap servers
    bootstrap_servers = "kafka-broker1:9092,kafka-broker2:9092"  # Comma-separated list

    # Fetch the list of Kafka brokers
    kafka_brokers = fetch_kafka_brokers(bootstrap_servers)

    # Print the list of Kafka brokers
    print("Kafka Brokers:", kafka_brokers)
    

    # Define the Kafka topics for each worker node
    topics_list = [
        {"pcap_packets"}, # Worker node 1 topic
         # Worker node 2 topic
        # Add more topics for additional worker nodes as needed
    ]
    
    # Create a shared variable for model weights on the master node
    model_weights = [tf.Variable(np.random.rand(5), dtype=tf.float32) for _ in range(num_workers)]
    shared_weights = sc.broadcast(model_weights)
    
    # Distribute packets among workers
    num_workers = N  # Set the number of workers
    packets_per_worker = total_packets // num_workers

    # Worker ID based on Spark application ID
    worker_id = int(sc.applicationId) % num_workers
    
    # Create Kafka streams for each worker node
    kafka_streams = []
    for i in range(worker_count):
        kafka_stream = KafkaUtils.createDirectStream(ssc, {topics_list[i]: 1}, kafka_params_list[i])
        kafka_streams.append(kafka_stream)

    batch_sizes = [64, 128, 256, 512]
    
    # Process the Kafka streams and train the TensorFlow model for each worker node
    for i, kafka_stream in enumerate(kafka_streams):
        # Train models with different batch sizes
        for batch_size in batch_sizes: 
            normalized_stream = kafka_stream.map(packet_normalization(files)
             kafkaStream.foreachRDD(lambda rdd: process_packets(rdd, worker_id, packets_per_worker))
            kafka_stream.foreachRDD(lambda rdd, idx=i: build_and_train_model(rdd, idx, file_dict, batch_size,shared_weights))
            # Define an aggregation interval (e.g., every 30 seconds)
            kafka_stream.window(30).foreachRDD(lambda rdd: aggregate_weights(rdd, shared_weights))
            

    # Start the Spark Streaming context
    ssc.start()

    # Await termination or use a termination condition
    ssc.awaitTermination()


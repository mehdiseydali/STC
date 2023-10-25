from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Conv1D, GRU, Dense

def create_spark_streaming_cluster(worker_count):
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
    kafka_params_list = [
        {"bootstrap.servers": "kafka-broker1:9092"},  # Worker node 1 Kafka broker
        {"bootstrap.servers": "kafka-broker2:9092"},  # Worker node 2 Kafka broker
        # Add more Kafka parameters for additional worker nodes as needed
    ]

    # Define the Kafka topics for each worker node
    topics_list = [
        {"worker1_topic"},  # Worker node 1 topic
        {"worker2_topic"},  # Worker node 2 topic
        # Add more topics for additional worker nodes as needed
    ]

    # Create Kafka streams for each worker node
    kafka_streams = []
    for i in range(worker_count):
        kafka_stream = KafkaUtils.createDirectStream(ssc, {topics_list[i]: 1}, kafka_params_list[i])
        kafka_streams.append(kafka_stream)

    # Function to build and train the TensorFlow model (worker-specific)
    def build_and_train_model(rdd, worker_index):
        # Initialize a TensorFlow session for each executor (worker node)
        with tf.Session() as sess:
            model = build_tensorflow_model()  # Implement this function to create your model
            for packet_data in rdd.collect():
                preprocessed_data = preprocess_packet_data(packet_data)  # Implement this function
                label = extract_label(packet_data)  # Implement this function
                model.train_on_batch(preprocessed_data, label)
                # Use the trained model for further processing or save it if needed

    # Process the Kafka streams and train the TensorFlow model for each worker node
    for i, kafka_stream in enumerate(kafka_streams):
        kafka_stream.foreachRDD(lambda rdd, idx=i: build_and_train_model(rdd, idx))

    # Start the Spark Streaming context
    ssc.start()

    # Await termination or use a termination condition
    ssc.awaitTermination()


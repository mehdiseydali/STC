from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Conv1D, GRU, Dense

# Initialize Spark
sc = SparkContext(appName="PCAPProcessing")
ssc = StreamingContext(sc, batchDuration=1)

# Kafka settings
kafkaParams = {"bootstrap.servers": "localhost:9092"}
topics = {"pcap_topic"}

# Function to create a 1D-CNN TensorFlow model
def build_1d_cnn_model():
    model = Sequential()
    model.add(Conv1D(filters=64, kernel_size=3, activation='relu', input_shape=(input_shape, 1))
    model.add(Dense(num_classes, activation='softmax'))
    model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
    return model

# Function to create a Bi-GRU TensorFlow model
def build_bi_gru_model():
    model = Sequential()
    model.add(GRU(64, return_sequences=True))
    model.add(Dense(num_classes, activation='softmax'))
    model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
    return model

# Create a function to build and train the TensorFlow model
def build_and_train_model(rdd, model_builder):
    # Initialize a TensorFlow session for each executor
    with tf.Session() as sess:
        model = model_builder()  # Create the model using the specified builder function
        for packet_data in rdd.collect():
            preprocessed_data = preprocess_packet_data(packet_data)  # Implement this function
            label = extract_label(packet_data)  # Implement this function
            model.train_on_batch(preprocessed_data, label)
            # Use the trained model for further processing or save it if needed

# Create a DStream to consume data from Kafka
kafkaStream = KafkaUtils.createDirectStream(ssc, topics, kafkaParams)

# Process the PCAP data and train the 1D-CNN model
kafkaStream.foreachRDD(lambda rdd: build_and_train_model(rdd, build_1d_cnn_model))

# You can also process the data and train the Bi-GRU model if needed
# kafkaStream.foreachRDD(lambda rdd: build_and_train_model(rdd, build_bi_gru_model))

ssc.start()
ssc.awaitTermination()

import tensorflow as tf

def Bi-GRU_build_model(params):
    # Define the model architecture
    model = tf.keras.Sequential()

    # Add the Bidirectional GRU layers
    model.add(tf.keras.layers.Bidirectional(tf.keras.layers.GRU(params['input_shape'][1], return_sequences=True), input_shape=params['input_shape']))
    model.add(tf.keras.layers.Bidirectional(tf.keras.layers.GRU(params['input_shape'][1], return_sequences=True)))
    model.add(tf.keras.layers.Bidirectional(tf.keras.layers.GRU(params['input_shape'][1], return_sequences=True)))

    # Add the fully connected layers (MLP)
    model.add(tf.keras.layers.Dense(params['dense_neurons'][0], activation=params['activation']))
    model.add(tf.keras.layers.Dense(params['dense_neurons'][1], activation=params['activation']))
    model.add(tf.keras.layers.Dense(params['dense_neurons'][2], activation=params['activation']))
    model.add(tf.keras.layers.Dense(params['dense_neurons'][3], activation=params['activation']))

    # Save the model
    model.save('bigru_model.h5')

    return model

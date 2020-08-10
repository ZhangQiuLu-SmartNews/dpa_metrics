# read tfreocrd (csv -> tfrecord)
# define tf_data
# define tf_model
# define tf train loop
# define tf summary


# raw api train
# estimator

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras import Model
import numpy as np
import sys


if __name__ == '__main__':

    def build_two_tower_model():
        def item_embedding_layer():
            return layers.Embedding(input_dim=20000, output_dim=128, mask_zero=True)

        item_embedding = item_embedding_layer()

        def user_tower_model():
            input = keras.Input(shape=(None,), name="user_seq")
            embedding = item_embedding(input)
            pooling = layers.GlobalAveragePooling1D()(embedding)
            output = layers.Dense(32, activation=tf.nn.relu)(pooling)
            return input, output

        def item_tower_model():
            input = keras.Input(shape=(1,), name="item_input")
            embedding = item_embedding(input)
            output = layers.Dense(32, activation=tf.nn.relu)(embedding)
            output = tf.reshape(output, [-1, 32])
            return input, output

        user_tower_input, user_tower_output = user_tower_model()
        item_tower_input, item_tower_output = item_tower_model()

        dot = tf.multiply(user_tower_output,
                          item_tower_output, name='user_item_dot')
        dot_reduce = tf.reduce_sum(dot, axis=1, keepdims=True)
        return keras.Model(
            inputs=[user_tower_input, item_tower_input],
            outputs=[dot_reduce, user_tower_output, item_tower_output]
        )

    def convert_to_prob(labels, logits):
        labels = tf.ones_like(labels)
        labels_prob = tf.concat([labels, 1 - labels], axis=1)
        logits_prob = tf.concat([logits, 1 - logits], axis=1)
        return labels_prob, logits_prob

    def build_loss(labels_prob, logits_prob):
        bianray_loss = tf.keras.losses.BinaryCrossentropy(from_logits=False)
        bianray_loss_obj = bianray_loss(labels_prob, logits_prob)
        return bianray_loss_obj

    def build_metric():
        train_loss = tf.keras.metrics.Mean()
        train_accuracy = tf.keras.metrics.BinaryAccuracy()
        return train_loss, train_accuracy

    def apply_metrics(labels_prob, logits_prob, loss_obj, loss_metric, accuracy_metric):
        return loss_metric(loss_obj), accuracy_metric(labels_prob, logits_prob)

    @tf.function
    def train_step(model, input, labels, optimizer, loss_metric, accuracy_metric):
        with tf.GradientTape() as tape:
            logits = model(input)[0]
            labels_prob, logits_prob = convert_to_prob(labels, logits)
            loss = build_loss(labels_prob, logits_prob)
            apply_metrics(labels_prob, logits_prob, loss,
                          loss_metric, accuracy_metric)
        gradients = tape.gradient(loss, model.trainable_variables)
        optimizer.apply_gradients(zip(gradients, model.trainable_variables))
        return loss

    @tf.function
    def test_step(model, input, labels, loss_metric, accuracy_metric):
        logits = model(input)[0]
        labels_prob, logits_prob = convert_to_prob(labels, logits)
        loss = build_loss(labels_prob, logits_prob)
        apply_metrics(labels_prob, logits_prob, loss,
                      loss_metric, accuracy_metric)

    def build_optimizer():
        return tf.keras.optimizers.Adam()

    def _parse_function(example_proto):
        keys_to_features = {
            'item_seq': tf.io.VarLenFeature(tf.int64),
            'item_target': tf.io.VarLenFeature(tf.int64),
            'label': tf.io.VarLenFeature(tf.int64)
        }
        parsed_features = tf.io.parse_single_example(
            example_proto, keys_to_features)
        return parsed_features

    def load_dataset(file_name):
        dataset = tf.data.TFRecordDataset(file_name)
        dataset = dataset.map(_parse_function)

        feature_dt = dataset.batch(20)
        return iter(feature_dt)

    def train_dataset_mock():
        train_user = np.random.rand(100000, 64)
        train_item = np.random.rand(100000, 64)
        train_label = np.random.choice(
            [0, 1], size=(100000, 1), p=[1./3, 2./3])
        return tf.data.Dataset.from_tensor_slices((train_user, train_item, train_label)).shuffle(10000).batch(32)

    def validation_dataset_mock():
        validation_user = np.random.rand(30000, 64)
        validation_item = np.random.rand(30000, 64)
        validation_label = np.random.choice(
            [0, 1], size=(30000, 1), p=[1./3, 2./3])
        return tf.data.Dataset.from_tensor_slices((validation_user, validation_item, validation_label)).shuffle(10000).batch(32)

    EPOCHS = 5
    train_loss_metric, train_accuracy_metric = build_metric()
    validation_loss_metric, validation_accuracy_metric = build_metric()

    optimizer = build_optimizer()

    two_tower_model = build_two_tower_model()

    train_ds = load_dataset(sys.argv[1])
    validation_ds = load_dataset(sys.argv[2])
    two_tower_model.summary()

    for epoch in range(EPOCHS):
        # Reset the metrics at the start of the next epoch
        train_loss_metric.reset_states()
        train_accuracy_metric.reset_states()
        validation_loss_metric.reset_states()
        validation_accuracy_metric.reset_states()

        while True:
            train_batch = train_ds.get_next()
            item_seq = tf.sparse.to_dense(train_batch['item_seq'])
            item_target = tf.sparse.to_dense(train_batch['item_target'])
            labels = tf.sparse.to_dense(train_batch['label'])
            loss = train_step(two_tower_model, [
                              item_seq, item_seq], labels, optimizer, train_loss_metric, train_accuracy_metric)
            print(loss)

        for user, item, labels in validation_ds:
            validation_batch = validation_ds.get_next()
            item_seq = tf.sparse.to_dense(validation_ds['item_seq'])
            item_target = tf.sparse.to_dense(validation_ds['item_target'])
            labels = tf.sparse.to_dense(validation_ds['label'])
            test_step(two_tower_model, [
                      item_seq, item_seq], labels, validation_loss_metric, validation_accuracy_metric)

        template = 'Epoch {}, Loss: {}, Accuracy: {}, Test Loss: {}, Test Accuracy: {}'
        print(template.format(epoch + 1,
                              train_loss_metric.result(),
                              train_accuracy_metric.result(),
                              validation_loss_metric.result(),
                              validation_accuracy_metric.result()))

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
import pandas as pd
import random
import sys


if __name__ == '__main__':

    def build_two_tower_model():
        def item_embedding_layer():
            return layers.Embedding(input_dim=25000, output_dim=128, mask_zero=True, embeddings_initializer=tf.keras.initializers.GlorotNormal)

        item_embedding = item_embedding_layer()

        def user_tower_model():
            input = keras.Input(shape=(None,), name="user_seq")
            embedding = item_embedding(input)
            pooling = layers.GlobalAveragePooling1D()(embedding)
            output = layers.Dense(
                64, activation=tf.nn.elu, kernel_initializer=tf.keras.initializers.GlorotNormal)(pooling)
            return input, output

        def item_tower_model():
            input = keras.Input(shape=(1,), name="item_input")
            embedding = item_embedding(input)
            output = layers.Dense(
                64, activation=tf.nn.elu, kernel_initializer=tf.keras.initializers.GlorotNormal)(embedding)
            output = tf.reshape(output, [-1, 64])
            return input, output

        user_tower_input, user_tower_output = user_tower_model()
        item_tower_input, item_tower_output = item_tower_model()

        dot = tf.multiply(user_tower_output,
                          item_tower_output, name='user_item_dot')
        dot_reduce = tf.reduce_sum(dot, axis=1, keepdims=True)
        dot_sigmoid = tf.math.sigmoid(dot_reduce)
        return keras.Model(
            inputs=[user_tower_input, item_tower_input],
            outputs=[dot_sigmoid, dot_reduce,
                     user_tower_output, item_tower_output]
        )

    def convert_to_prob(labels, logits):
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
            model_out = model(input)
            logits_sigmoid = model_out[0]
            logits = model_out[1]
            labels_prob, logits_prob = convert_to_prob(labels, logits_sigmoid)
            loss = build_loss(labels_prob, logits_prob)
            #apply_metrics(labels_prob, logits_prob, loss, loss_metric, accuracy_metric)
        gradients = tape.gradient(loss, model.trainable_variables)
        optimizer.apply_gradients(zip(gradients, model.trainable_variables))
        return logits, logits_sigmoid, loss

    @tf.function
    def test_step(model, input, labels, loss_metric, accuracy_metric):
        model_out = model(input)
        logits_sigmoid = model_out[0]
        logits = model_out[1]
        labels_prob, logits_prob = convert_to_prob(labels, logits_sigmoid)
        loss = build_loss(labels_prob, logits_prob)
        #apply_metrics(labels_prob, logits_prob, loss, loss_metric, accuracy_metric)
        return logits, logits_sigmoid, loss

    def build_optimizer():
        return tf.keras.optimizers.Adam(learning_rate=0.0001)

    def _parse_function(example_proto):
        keys_to_features = {
            'ad_id': tf.io.VarLenFeature(tf.string),
            'item_seq': tf.io.VarLenFeature(tf.int64),
            'item_target': tf.io.VarLenFeature(tf.int64),
            'label': tf.io.VarLenFeature(tf.int64)
        }
        parsed_features = tf.io.parse_single_example(
            example_proto, keys_to_features)
        return parsed_features

    def load_dataset(file_pattern, batch_size=100):
        file_name = tf.io.gfile.glob(file_pattern)
        print(file_name)
        dataset = tf.data.TFRecordDataset(file_name)
        dataset = dataset.map(_parse_function)

        feature_dt = dataset.batch(batch_size)
        return iter(feature_dt)

    def load_postive_item_dict(file_name):
        df = pd.read_csv(file_name, sep='\001', names='ad_id,item'.split(','))
        df['item'] = df['item'].str.split(',')
        user_item_dict = {}
        for row in df.itertuples():
            ad_id = getattr(row, 'ad_id')
            item = getattr(row, 'item')
            user_item_dict.update({ad_id: item})
        return user_item_dict

    def negtive_sample(ad_id, postive_item_dict, item_range):
        # find ad_id in each row
        # extend row with 10x negtive sample
        item_negtive = []
        for row in ad_id:
            _ad_id = row[0].decode('utf-8')
            if _ad_id in postive_item_dict:
                while True:
                    rds = random.randrange(1, item_range + 1)
                    if rds not in postive_item_dict[_ad_id]:
                        item_negtive.append([rds])
                        break
            else:
                rds = random.randrange(1, item_range + 1)
                item_negtive.append([rds])
        return tf.convert_to_tensor(item_negtive)

    def validate_performance(validation_ds, model, validation_auc_metric, validation_loss_metric, validation_accuracy_metric):
        validation_batch = validation_ds.get_next()
        ad_id = tf.sparse.to_dense(
            validation_batch['ad_id']).numpy()
        item_seq = tf.sparse.to_dense(validation_batch['item_seq'])
        item_target = tf.sparse.to_dense(
            validation_batch['item_target'])
        labels = tf.sparse.to_dense(validation_batch['label'])
        logits, logits_sigmoid, loss = test_step(model, [
            item_seq, item_target], tf.ones_like(labels), validation_loss_metric, validation_accuracy_metric)
        validation_auc_metric.update_state(
            tf.ones_like(labels), logits_sigmoid)
        print("val loss: ", loss)
        print("val logits sigmoid: ", logits_sigmoid.numpy()[:10])
        print("validation auc_metric: ",
              validation_auc_metric.result().numpy())

    EPOCHS = 5
    train_loss_metric, train_accuracy_metric = build_metric()
    validation_loss_metric, validation_accuracy_metric = build_metric()
    train_auc_metric = tf.keras.metrics.AUC(num_thresholds=200)
    validation_auc_metric = tf.keras.metrics.AUC(num_thresholds=200)

    optimizer = build_optimizer()

    two_tower_model = build_two_tower_model()

    train_ds = load_dataset(sys.argv[1], 50)
    validation_ds = load_dataset(sys.argv[2], 200)
    postive_item_dict = load_postive_item_dict(sys.argv[3])
    two_tower_model.summary()

    for epoch in range(EPOCHS):
        # Reset the metrics at the start of the next epoch
        train_loss_metric.reset_states()
        train_accuracy_metric.reset_states()
        validation_loss_metric.reset_states()
        validation_accuracy_metric.reset_states()
        train_auc_metric.reset_states()
        validation_auc_metric.reset_states()
        while True:
            try:
                for i in range(100):
                    train_batch = train_ds.get_next()
                    ad_id = tf.sparse.to_dense(train_batch['ad_id']).numpy()
                    item_seq = tf.sparse.to_dense(train_batch['item_seq'])
                    item_target = tf.sparse.to_dense(
                        train_batch['item_target'])
                    labels = tf.sparse.to_dense(train_batch['label'])

                    item_range = 20000

                    # positive
                    pos_logits, pos_logits_sigmoid, loss = train_step(two_tower_model, [
                        item_seq, item_target], tf.ones_like(labels), optimizer, train_loss_metric, train_accuracy_metric)
                    train_auc_metric.update_state(
                        tf.ones_like(labels), pos_logits_sigmoid)
                    # print("positive", logits_sigmoid.numpy())

                    # negative
                    for neg_i in range(20):
                        item_negative = negtive_sample(
                            ad_id, postive_item_dict, item_range)
                        neg_logits, neg_logits_sigmoid, loss = train_step(two_tower_model, [
                            item_seq, item_negative], tf.zeros_like(labels), optimizer, train_loss_metric, train_accuracy_metric)
                        if neg_i == 0:
                            train_auc_metric.update_state(
                                tf.zeros_like(labels), neg_logits_sigmoid)
                        #print("negative", logits_sigmoid.numpy())
                    if i == 99:
                        validate_performance(validation_ds, two_tower_model, validation_auc_metric,
                                             validation_loss_metric, validation_accuracy_metric)
                    if i % 10 == 0:
                        print("train auc_metric: ",
                              train_auc_metric.result().numpy())
            except tf.errors.OutOfRangeError:
                break
        while True:
            try:
                validate_performance(validation_ds, two_tower_model, validation_auc_metric,
                                     validation_loss_metric, validation_accuracy_metric)
            except tf.errors.OutOfRangeError:
                break
        template = 'Epoch {}, Loss: {}, Accuracy: {}, Test Loss: {}, Test Accuracy: {}'
        print(template.format(epoch + 1,
                              train_loss_metric.result(),
                              train_accuracy_metric.result(),
                              validation_loss_metric.result(),
                              validation_accuracy_metric.result()))

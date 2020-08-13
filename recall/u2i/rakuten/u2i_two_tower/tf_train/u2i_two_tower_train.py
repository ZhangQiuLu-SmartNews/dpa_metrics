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
import gc


if __name__ == '__main__':

    def build_two_tower_model():
        def item_embedding_layer():
            return layers.Embedding(input_dim=25000,
                                    output_dim=256,
                                    mask_zero=False,
                                    embeddings_regularizer=tf.keras.regularizers.l1_l2(),
                                    embeddings_initializer=tf.keras.initializers.GlorotNormal)

        item_embedding = item_embedding_layer()

        def user_tower_model():
            item_seq_input = keras.Input(shape=(None,), name="item_seq")
            embedding = item_embedding(item_seq_input)
            item_seq_pooling = layers.GlobalAveragePooling1D()(embedding)

            item_car_input = keras.Input(shape=(None,), name="item_car")
            embedding = item_embedding(item_car_input)
            item_car_pooling = layers.GlobalAveragePooling1D()(embedding)

            item_purchase_input = keras.Input(
                shape=(None,), name="item_purchase")
            embedding = item_embedding(item_purchase_input)
            item_purchase_pooling = layers.GlobalAveragePooling1D()(embedding)

            concate_pooling = tf.concat(
                [item_seq_pooling, item_car_pooling, item_purchase_pooling], axis=1)
            concate_pooling = tf.where(tf.math.is_nan(concate_pooling), tf.zeros_like(
                concate_pooling), concate_pooling)
            output = layers.Dense(
                128,
                activation=tf.nn.tanh,
                kernel_regularizer=tf.keras.regularizers.l1_l2(),
                kernel_initializer=tf.keras.initializers.GlorotNormal)(concate_pooling)
            return [item_seq_input, item_car_input, item_purchase_input], output

        def item_tower_model():
            input = keras.Input(shape=(1,), name="item_input")
            embedding = item_embedding(input)
            output = layers.Dense(
                128,
                activation=tf.nn.tanh,
                kernel_regularizer=tf.keras.regularizers.l1_l2(),
                kernel_initializer=tf.keras.initializers.GlorotNormal)(embedding)
            output = tf.reshape(output, [-1, 128])
            return [input], output

        user_tower_input, user_tower_output = user_tower_model()
        item_tower_input, item_tower_output = item_tower_model()

        dot = tf.multiply(user_tower_output,
                          item_tower_output, name='user_item_dot')
        dot_reduce = tf.reduce_sum(dot, axis=1, keepdims=True)
        dot_sigmoid = tf.math.sigmoid(dot_reduce)
        return keras.Model(
            inputs=user_tower_input + item_tower_input,
            outputs=[dot_sigmoid, dot_reduce,
                     user_tower_output, item_tower_output]
        )

    def convert_to_prob(labels, logits):
        labels_prob = tf.concat(
            [labels, tf.ones_like(labels) - labels], axis=1)
        logits_prob = tf.concat(
            [logits, tf.ones_like(logits) - logits], axis=1)
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

    @ tf.function
    def train_step(model, input, labels, optimizer):
        with tf.GradientTape() as tape:
            model_out = model(input)
            logits_sigmoid = model_out[0]
            logits = model_out[1]
            labels_prob, logits_prob = convert_to_prob(labels, logits_sigmoid)
            loss = build_loss(labels_prob, logits_prob)
            # apply_metrics(labels_prob, logits_prob, loss, loss_metric, accuracy_metric)
        gradients = tape.gradient(loss, model.trainable_variables)
        optimizer.apply_gradients(zip(gradients, model.trainable_variables))
        return logits, logits_sigmoid, loss

    @ tf.function
    def test_step(model, input, labels):
        model_out = model(input)
        logits_sigmoid = model_out[0]
        logits = model_out[1]
        labels_prob, logits_prob = convert_to_prob(labels, logits_sigmoid)
        loss = build_loss(labels_prob, logits_prob)
        # apply_metrics(labels_prob, logits_prob, loss, loss_metric, accuracy_metric)
        return logits, logits_sigmoid, loss

    def build_optimizer():
        return tf.keras.optimizers.Adam(learning_rate=0.00002)

    def _parse_function(example_proto):
        keys_to_features = {
            'ad_id': tf.io.VarLenFeature(tf.string),
            'item_seq': tf.io.VarLenFeature(tf.int64),
            'behavior_seq': tf.io.VarLenFeature(tf.int64),
            'item_car': tf.io.VarLenFeature(tf.int64),
            'item_purchase': tf.io.VarLenFeature(tf.int64),
            'item_category': tf.io.VarLenFeature(tf.int64),
            'item_target': tf.io.VarLenFeature(tf.int64),
            'label': tf.io.VarLenFeature(tf.int64)
        }
        parsed_features = tf.io.parse_single_example(
            example_proto, keys_to_features)
        return parsed_features

    def get_next_batch(dataset):
        dataset_batch = dataset.get_next()
        ad_id = tf.sparse.to_dense(
            dataset_batch['ad_id']).numpy()
        item_seq = tf.sparse.to_dense(dataset_batch['item_seq'])
        item_car = tf.sparse.to_dense(dataset_batch['item_car'])
        item_purchase = tf.sparse.to_dense(dataset_batch['item_purchase'])
        item_target = tf.sparse.to_dense(
            dataset_batch['item_target'])
        label = tf.sparse.to_dense(dataset_batch['label'])
        nn_input = [item_seq, item_car, item_purchase]
        return ad_id, nn_input, item_target, label

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

    def validate_performance(validation_ds, model, validation_auc_metric):
        ad_id, nn_input, item_target, label = get_next_batch(validation_ds)
        logits, logits_sigmoid, loss = test_step(model, [nn_input, item_target], tf.ones_like(
            label))
        validation_auc_metric.update_state(
            tf.ones_like(label), logits_sigmoid)
        item_negative = negtive_sample(
            ad_id, postive_item_dict, item_range)
        neg_logits, neg_logits_sigmoid, loss = test_step(model, [nn_input, item_negative], tf.zeros_like(
            label))
        validation_auc_metric.update_state(
            tf.zeros_like(label), neg_logits_sigmoid)

        logits_sigmoid_array = logits_sigmoid.numpy()
        neg_logits_sigmoid_array = neg_logits_sigmoid.numpy()
        print("val loss: ", loss)
        print("validation auc_metric: {}".format(
            validation_auc_metric.result().numpy()))
        print("validation rank: positive sum: {}, negative sum: {}".format(
            np.mean(logits_sigmoid_array), np.mean(neg_logits_sigmoid_array)))

    def validation_hr_rate(validation_hr_ds, model, batch):
        hr_1 = 0
        hr_5 = 0
        hr_10 = 0
        hr_20 = 0
        hr_50 = 0
        hr_100 = 0
        hr_500 = 0
        i = 0
        try:
            while True:
                ad_id, nn_input, item_target, label = get_next_batch(
                    validation_hr_ds)
                validation_hr_batch = validation_hr_ds.get_next()
                cancidate_item = [[i + 1] for i in range(item_range)]
                nn_input_repeat = [tf.repeat(
                    _input, len(cancidate_item), axis=0) for _input in nn_input]
                cancidate_item = tf.convert_to_tensor(cancidate_item)
                output = model([nn_input_repeat, cancidate_item])
                ranked = np.argsort(np.reshape(output[0].numpy(), [1, -1]))[0]
                ranked = ranked[::-1]
                item_target = item_target.numpy()[0]
                hr_1 += 1 if item_target == ranked[0] else 0
                hr_5 += 1 if item_target in ranked[:5] else 0
                hr_10 += 1 if item_target in ranked[:10] else 0
                hr_20 += 1 if item_target in ranked[:20] else 0
                hr_50 += 1 if item_target in ranked[:50] else 0
                hr_100 += 1 if item_target in ranked[:100] else 0
                hr_500 += 1 if item_target in ranked[:500] else 0
                i += 1
                if batch > 0 and i == batch:
                    break
        finally:
            gc.collect()
            return "HR@N record numbers: {} \nhr@1: {} , hr@5: {}, hr@10: {}, hr@20: {}, hr@50: {},  hr@100: {}, hr@500: {}".format(i, hr_1 / i, hr_5 / i, hr_10 / i, hr_20 / i, hr_50 / i, hr_100 / i, hr_500 / i)

    EPOCHS = 5
    train_auc_metric = tf.keras.metrics.AUC(num_thresholds=200)
    validation_auc_metric = tf.keras.metrics.AUC(num_thresholds=200)

    optimizer = build_optimizer()

    two_tower_model = build_two_tower_model()

    train_step_batch = 20
    validation_step_batch = 200
    negative_sample_iter = 10
    # candidate item count
    item_range = 20000

    train_ds = load_dataset(sys.argv[1], train_step_batch)
    validation_ds = load_dataset(sys.argv[2], validation_step_batch)
    validation_hr_ds = load_dataset(sys.argv[2], 1)
    postive_item_dict = load_postive_item_dict(sys.argv[3])
    two_tower_model.summary()

    global_step = tf.Variable(0, dtype=tf.int64)
    ckpt = tf.train.Checkpoint(step=global_step, optimizer=optimizer, net=two_tower_model, iterator=train_ds)
    manager = tf.train.CheckpointManager(ckpt, './tf_ckpts', max_to_keep=3)
    ckpt.restore(manager.latest_checkpoint)
    if manager.latest_checkpoint:
      print("Restored from {}".format(manager.latest_checkpoint))
    else:
      print("Initializing from scratch.")

    for epoch in range(EPOCHS):
        # Reset the metrics at the start of the next epoch
        train_auc_metric.reset_states()
        validation_auc_metric.reset_states()
        while True:
            try:
                for i in range(200):
                    train_batch = train_ds.get_next()
                    ad_id, nn_input, item_target, label = get_next_batch(
                        train_ds)

                    # positive
                    pos_logits, pos_logits_sigmoid, loss = train_step(two_tower_model, [nn_input, item_target], tf.ones_like(
                        label), optimizer)
                    print(two_tower_model.layers[3].get_weights())
                    exit()
                    train_auc_metric.update_state(
                        tf.ones_like(label), pos_logits_sigmoid)
                    global_step.assign_add(train_step_batch)

                    # negative
                    for neg_i in range(negative_sample_iter):
                        item_negative = negtive_sample(
                            ad_id, postive_item_dict, item_range)
                        neg_logits, neg_logits_sigmoid, loss = train_step(two_tower_model, [nn_input, item_negative], tf.zeros_like(
                            label), optimizer)
                        if neg_i == 0:
                            train_auc_metric.update_state(
                                tf.zeros_like(label), neg_logits_sigmoid)
                        global_step.assign_add(train_step_batch)

                    global_step_num = global_step.numpy()
                    if i == 0:
                        save_path = manager.save()
                        print("=" * 15)
                        print("global step: {}, validation: ".format(
                            global_step_num))
                        validate_performance(
                            validation_ds, two_tower_model, validation_auc_metric)
                        validation_hr = validation_hr_rate(
                            validation_hr_ds, two_tower_model, 100)
                        print("global step: {}, {}".format(
                            global_step_num, validation_hr))
                        print("=" * 15)
                    if i % 20 == 0:
                        print("global step: {}, train auc_metric: {}".format(
                            global_step_num, train_auc_metric.result().numpy()))
            except tf.errors.OutOfRangeError:
                break
        while True:
            try:
                global_step_num = global_step.numpy()
                print("Epoch {} , global step {} validation:".format(
                    epoch, global_step_num))
                validate_performance(
                    validation_ds, two_tower_model, validation_auc_metric)
                validation_hr = validation_hr_rate(
                    validation_hr_ds, two_tower_model, 0)
                print(validation_hr)
            except tf.errors.OutOfRangeError:
                break
        template = 'Epoch {}, AUC: {}'
        print(template.format(epoch + 1,train_accuracy_metric.result()))

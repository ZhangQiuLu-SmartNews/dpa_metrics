import tensorflow as tf
import numpy as np
import pandas as pd
import base64
import sys
from tensorflow import keras
from tensorflow.keras import layers


def read_csv_file(file_name):
    return pd.read_csv(file_name, sep='\001', chunksize=500000,
                       names='ad_id,item_seq,behavior_seq,item_car,item_purchase,item_category,item_target,label'.split(','))


def feature_preprocess(feature_df):
    featue_column = feature_config().keys()
    return feature_df[featue_column]


def serialize_tfexample(feature_row, feature_conf):
    features = {}
    for feature in feature_conf:
        feature_wrapper = feature_conf[feature]
        f_val = getattr(feature_row, feature)
        features.update({feature: feature_wrapper(f_val)})
    example_proto = tf.train.Example(
        features=tf.train.Features(feature=features))
    return example_proto.SerializeToString()


def feature_config():
    def _bytes_feature(value):
        if isinstance(value, type(tf.constant(0))):
            value = value.numpy()
        return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))

    def _float_feature(value):
        return tf.train.Feature(float_list=tf.train.FloatList(value=[value]))

    def _int64_feature(value):
        return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))

    def _int64_list_feature(value):
        return tf.train.Feature(int64_list=tf.train.Int64List(value=value))

    def _float_list_feature(value):
        # value is a list
        return tf.train.Feature(float_list=tf.train.FloatList(value=value))

    feature_conf_write = {
        'ad_id': _bytes_feature,
        'item_seq': _int64_list_feature,
        'behavior_seq': _int64_list_feature,
        'item_car': _int64_list_feature,
        'item_purchase': _int64_list_feature,
        'item_category': _int64_list_feature,
        'item_target': _int64_feature,
        'label': _int64_feature
    }
    return feature_conf_write


def feature_extracton(feature_df):
    feature_conf = feature_config()
    feature_df['ad_id'] = feature_df['ad_id'].str.encode('utf-8')

    feature_df['item_seq'].replace('\\N', '', inplace=True)
    feature_df['item_seq'] = feature_df['item_seq'].str.split(
        ',').apply(lambda val_array: [int(val) for val in val_array] if val_array[0] != '' else [])

    feature_df['behavior_seq'].replace('\\N', '', inplace=True)
    feature_df['behavior_seq'] = feature_df['behavior_seq'].str.split(
        ',').apply(lambda val_array: [int(val) for val in val_array] if val_array[0] != '' else [])

    feature_df['item_car'].replace('\\N', '', inplace=True)
    feature_df['item_car'] = feature_df['item_car'].str.split(
        ',').apply(lambda val_array: [int(val) for val in val_array] if val_array[0] != '' else [])

    feature_df['item_purchase'].replace('\\N', '', inplace=True)
    feature_df['item_purchase'] = feature_df['item_purchase'].str.split(
        ',').apply(lambda val_array: [int(val) for val in val_array] if val_array[0] != '' else [])

    feature_df['item_category'].replace('\\N', '', inplace=True)
    feature_df['item_category'] = feature_df['item_category'].str.split(
        ',').apply(lambda val_array: [int(val) for val in val_array] if val_array[0] != '' else [])

    return feature_df[feature_conf.keys()]


def feature_extracton_unique(feature_df):
    feature_conf = feature_config()
    feature_df['ad_id'] = feature_df['ad_id'].str.encode('utf-8')

    feature_df['item_seq'].replace('\\N', '', inplace=True)
    feature_df['item_seq'] = feature_df['item_seq'].str.split(
        ',').apply(lambda val_array: np.unique([int(val) for val in val_array]) if val_array[0] != '' else [])

    feature_df['behavior_seq'].replace('\\N', '', inplace=True)
    feature_df['behavior_seq'] = feature_df['behavior_seq'].str.split(
        ',').apply(lambda val_array: np.unique([int(val) for val in val_array]) if val_array[0] != '' else [])

    feature_df['item_car'].replace('\\N', '', inplace=True)
    feature_df['item_car'] = feature_df['item_car'].str.split(
        ',').apply(lambda val_array: np.unique([int(val) for val in val_array]) if val_array[0] != '' else [])

    feature_df['item_purchase'].replace('\\N', '', inplace=True)
    feature_df['item_purchase'] = feature_df['item_purchase'].str.split(
        ',').apply(lambda val_array: np.unique([int(val) for val in val_array]) if val_array[0] != '' else [])

    feature_df['item_category'].replace('\\N', '', inplace=True)
    feature_df['item_category'] = feature_df['item_category'].str.split(
        ',').apply(lambda val_array: np.unique([int(val) for val in val_array]) if val_array[0] != '' else [])

    feature_df = feature_df[feature_df['item_seq'].map(len) > 5]

    return feature_df[feature_conf.keys()]

def generate_tfrecord(feature_df, tfrecord_file):
    feature_conf = feature_config()
    writer = tf.io.TFRecordWriter(tfrecord_file)
    for row in feature_df.itertuples():
        row_example = serialize_tfexample(row, feature_conf)
        writer.write(row_example)
    writer.close()


if __name__ == '__main__':
    if len(sys.argv) == 2:

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

        dataset = tf.data.TFRecordDataset(sys.argv[1])
        dataset = dataset.map(_parse_function)

        feature_dt = dataset.batch(10)
        # Create a one-shot iterator
        data = iter(feature_dt).get_next()
        dense_ad_id = tf.sparse.to_dense(data['ad_id']).numpy()

        dense_data = tf.sparse.to_dense(data['item_car'])

        inputs = keras.Input(shape=(None,), dtype="int32")
        x = layers.Embedding(input_dim=24000, output_dim=10,
                             mask_zero=True)(inputs)
        x = tf.where(tf.math.is_nan(x), tf.zeros_like(x), x)
        outputs = layers.GlobalAveragePooling1D()(x)
        model = keras.Model(
            inputs=inputs,
            outputs=outputs
        )
        print(model(dense_data))

    elif len(sys.argv) == 3:
        csv_file = sys.argv[1]
        tfrecord_file = sys.argv[2]

        file_index = 0

        for feature_df in read_csv_file(csv_file):
            feature_df = feature_preprocess(feature_df)
            feature_df = feature_extracton_unique(feature_df)
            generate_tfrecord(feature_df, "{}.{}".format(tfrecord_file, file_index))
            file_index += 1

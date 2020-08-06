import json
import logging
import os
import random
import sys
from collections import OrderedDict, defaultdict
from functools import partial

import tensorflow as tf

_logger = logging.getLogger(__name__)


SEP = "\t"
READ_MODE = "r"
FEATURE_CONFIG_PATH = "u2i_dnn_feature_config.json"
FILE_NAME = "".join(random.sample("zyxwvutsrqponmlkjihgfedcba1234567890", 10))
DENSE = "dense"
SPARSE = "sparse"
EMBEDDING = "embedding"
WEIGHTED_EMBEDDING = "weighted_embedding"
LABELS = "labels"


class FeatureConfig:
    def __init__(self, config_path):
        try:
            with open(config_path, "r") as f:
                config = json.load(
                    f, object_pairs_hook=partial(defaultdict, lambda: [])
                )
                self._sparse = set(config[SPARSE])
                self._dense = set(config[DENSE])
                self._embedding = set(config[EMBEDDING])
                self._weighted_embedding = set(config[WEIGHTED_EMBEDDING])
                self._labels = set(config[LABELS])
        except Exception as e:
            print("open config file exception: {}".format(e))

    @property
    def sparse(self):
        return self._sparse

    @property
    def dense(self):
        return self._dense

    @property
    def embedding(self):
        return self._embedding

    @property
    def weighted_embedding(self):
        return self._weighted_embedding

    @property
    def labels(self):
        return self._labels


class FeatureMeta:
    def __init__(self, value, feature_type, label):
        if feature_type not in [DENSE, SPARSE, EMBEDDING, WEIGHTED_EMBEDDING]:
            raise Exception("{} type not supported yes".format(feature_type))

        self.type = feature_type
        self.positive = 0
        self.count = 0
        self.categories = defaultdict(int)
        if self.type == DENSE:
            self.min, self.max, self.mean = value, value, value
            self.square_mean = value ** 2
        self.update(value, label)

    def update(self, value, label):
        if self.type == WEIGHTED_EMBEDDING:
            self._update_weighted_categories(value, label)
        elif self.type == DENSE and self.count > 0:
            self.min = min(self.min, value)
            self.max = max(self.max, value)
            self.mean = (value + self.mean * self.count) / (self.count + 1)
            self.square_mean = (value ** 2 + self.square_mean * self.count) / (
                self.count + 1
            )
        elif self.type != DENSE:
            self._update_categories(value)

        self.positive += label
        self.count += 1

    def _update_categories(self, categories):
        if type(categories) == list:
            for category in categories:
                self.categories[category] += 1
        else:
            self.categories[categories] += 1

    def _update_weighted_categories(self, categories, label):
        for key, value in categories.items():
            if key not in self.categories:
                self.categories[key] = FeatureMeta(float(value), DENSE, label)
            else:
                self.categories[key].update(float(value), label)

    def export(self):
        export_data = self.__dict__
        if self.type == DENSE:
            export_data["variance"] = self.square_mean - self.mean ** 2
        elif self.type in [SPARSE, EMBEDDING]:
            self.categories = dict(
                sorted(self.categories.items(), key=lambda item: item[1], reverse=True)
            )
        elif self.type == WEIGHTED_EMBEDDING:
            self.categories = {
                key: value.export() for key, value in self.categories.items()
            }

        return export_data


def read_input():
    for line in sys.stdin:
        dt, features, label, *cvr_label = line.rstrip().split(SEP)
        feature_dict = json.loads(features)
        feature_dict["label"] = label
        feature_dict["ctr_label"] = label
        if cvr_label:
            feature_dict["cvr_label"] = cvr_label[0]
        yield dt, feature_dict, float(label)


def read_input_from_file(file_path):
    with open(file_path, READ_MODE) as f:
        for line in f.readlines():
            dt, features, label = line.rstrip().split(SEP)
            feature_dict = json.loads(features)
            yield dt, feature_dict, float(label)


def _bytes_feature(value):
    if isinstance(value, type(tf.constant(0))):
        # BytesList won't unpack a string from an EagerTensor.
        value = value.numpy()
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=list(value)))


def _float_feature(value):
    return tf.train.Feature(float_list=tf.train.FloatList(value=list(value)))


def _int64_feature(value):
    return tf.train.Feature(int64_list=tf.train.Int64List(value=list(value)))


def _update_feature_meta(meta_map, feature, value, label, type):
    if feature not in meta_map:
        meta_map[feature] = FeatureMeta(value, type, label)
    else:
        meta_map[feature].update(value, label)


def dump_meta(date, meta_map):
    data = {key: value.export() for key, value in meta_map.items()}
    data = OrderedDict(sorted(data.items(), key=lambda item: item[0]))
    print(date + "\t" + json.dumps(data))


def generate_records():
    feature_config = FeatureConfig(FEATURE_CONFIG_PATH)
    _logger.info("file_name: {}".format(FILE_NAME))
    with tf.io.TFRecordWriter(FILE_NAME + ".tfrecord", "GZIP") as writer:
        meta_map = dict()
        for dt, features_dict, label in read_input():
            tf_features = dict()
            # sparse features
            for feature in feature_config.sparse:
                if feature in features_dict and features_dict[feature] is not None:
                    value = features_dict[feature]
                    if type(value) == int:
                        value = str(value)
                    tf_features[feature] = _bytes_feature([value.encode()])
                    _update_feature_meta(meta_map, feature, value, label, SPARSE)
            # dense features
            for feature in feature_config.dense:
                if feature in features_dict and features_dict[feature] is not None:
                    value = features_dict[feature]
                    if type(value) not in [int, float, str]:
                        continue
                    if type(value) == str:
                        value = float(value)
                    tf_features[feature] = _float_feature([value])
                    _update_feature_meta(meta_map, feature, value, label, DENSE)
            # embedding features
            for feature in feature_config.embedding:
                if feature in features_dict:
                    value = features_dict[feature]
                    if not value or len(value) == 0:
                        continue
                    if type(value[0]) == int:
                        tf_features[feature] = _bytes_feature(
                            [str(i).encode() for i in value]
                        )
                    elif type(value[0]) == str:
                        tf_features[feature] = _bytes_feature(
                            [s.encode() for s in value]
                        )
                    _update_feature_meta(meta_map, feature, value, label, EMBEDDING)
            # weighted_embedding features
            for feature in feature_config.weighted_embedding:
                if feature in features_dict:
                    value = features_dict[feature]
                    if not value or len(value) == 0:
                        continue
                    tf_features[feature + "_keys"] = _bytes_feature(
                        [s.encode() for s in value.keys()]
                    )
                    tf_features[feature + "_values"] = _float_feature(
                        [float(v) for v in value.values()]
                    )
                    _update_feature_meta(
                        meta_map, feature, value, label, WEIGHTED_EMBEDDING
                    )
            # multi-task labels:
            for feature in feature_config.labels:
                if feature in features_dict and feature not in tf_features:
                    value = features_dict[feature]
                    if type(value) not in [int, float, str]:
                        continue
                    if type(value) == str:
                        value = float(value)
                    tf_features[feature] = _float_feature([value])
            # build tf Example
            example_proto = tf.train.Example(
                features=tf.train.Features(feature=tf_features)
            )
            serialized_example = example_proto.SerializeToString()
            writer.write(serialized_example)
    # save tf record file
    os.system(
        "aws s3 cp {}.tfrecord s3://smartad-dmp/warehouse/user/yukimura/dpa/u2i/data/dt={}/ > /dev/null".format(
            FILE_NAME, dt
        )
    )
    # save feature meta
    dump_meta(dt, meta_map)


if __name__ == "__main__":
    generate_records()

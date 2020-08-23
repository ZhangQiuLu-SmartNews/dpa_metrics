from sklearn.cluster import KMeans
import numpy as np
import pandas as pd
import argparse
from collections import Counter


def read_user_item_behavior(user_item_file, item_category_file):
    user_item_df = pd.read_csv(user_item_file, names=['aid', 'item_seq', 'category_seq'], dtype={'aid': str, 'item_seq': str, 'category_seq': str}, sep='\001')
    item_category_df = pd.read_csv(item_category_file, names=['item_category', 'counts'], sep='\001')
    item_category_map = get_item_category_map(item_category_df)
    cluster_vec = fill_cluster_vec(user_item_df, item_category_map)
    return user_item_df, cluster_vec


def get_item_category_map(item_category_df):
    item_category_df = item_category_df[item_category_df.counts > 100]
    item_category_df = item_category_df.sort_values(by=['item_category'])
    item_category_map = {category: i for i, category in enumerate(item_category_df['item_category'].values)}
    return item_category_map


def fill_cluster_vec(item_category_df, item_category_map):
    def build_vec(seq, i_map):
        vec = [0 for _ in range(len(i_map.keys()))]
        for category in seq:
            if category in i_map:
                vec[i_map[category]] = 1
        return np.asarray(vec)

    # item_category_df['cluster_vec'] = item_category_df.category_seq.apply(lambda seq: build_vec(str(seq), item_category_map))

    vecs = []
    for i, cate_seq in enumerate(item_category_df['category_seq'].values):
        vecs.append(build_vec(str(cate_seq).split(','), item_category_map))
    return vecs


def main(args):
    user_item_behavior_df, cluster_vec = read_user_item_behavior(args.user_item_file, args.item_category_file)
    kmeans = KMeans(n_clusters=5, random_state=0).fit(cluster_vec)
    counter = Counter(kmeans.labels_)
    print(counter)
    labels = [label for label in kmeans.labels_]
    user_item_behavior_df['cluster_label'] = np.reshape(labels, [-1, 1])
    user_item_behavior_df.to_csv(args.output_file, header=False, index=False, sep='\001')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="fuck i2i",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--user_item', type=str, default='', dest='user_item_file')
    parser.add_argument('--item_category', type=str, default='', dest='item_category_file')
    parser.add_argument('--output', type=str, default='user_item_behavior_df.csv', dest='output_file')
    args = parser.parse_args()
    print(args)
    main(args)
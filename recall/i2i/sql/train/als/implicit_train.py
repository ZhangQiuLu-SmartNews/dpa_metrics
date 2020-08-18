
from __future__ import print_function

import argparse
import codecs
import logging
import time
import pandas as pd
import scipy

import numpy as np
import tqdm

from implicit.als import AlternatingLeastSquares
from implicit.bpr import BayesianPersonalizedRanking
from implicit.datasets.movielens import get_movielens
from implicit.datasets.lastfm import get_lastfm
from implicit.lmf import LogisticMatrixFactorization
from implicit.nearest_neighbours import (BM25Recommender, CosineRecommender,
                                         TFIDFRecommender, bm25_weight)

log = logging.getLogger("implicit")


def read_user_item_data(file_name):
    return pd.read_csv(file_name, sep='\001', names='ad_id,item_id,user_index,item_index,score'.split(','))


def get_user_item_sparse_data_csv(user_item_df):
    unique_user = np.sort(np.unique(user_item_df['user_id']))
    unique_item = np.sort(np.unique(user_item_df['item_id']))
    unique_user_df = pd.DataFrame(
        {'user_index': [i for i in range(len(unique_user))], 'user_id': unique_user})
    unique_item_df = pd.DataFrame(
        {'item_index': [i for i in range(len(unique_item))], 'item_id': unique_item})
    user_item_df = user_item_df.join(
        unique_item_df.set_index('item_id'), on='item_id')
    user_item_df = user_item_df.join(
        unique_user_df.set_index('user_id'), on='user_id')
    return unique_user, unique_item, user_item_df


def get_user_item_sparse_data_presto(user_item_df):
    user_item_df['user_index'] -= 1
    user_item_df['item_index'] -= 1
    unique_user = user_item_df[['user_index', 'ad_id']].drop_duplicates().sort_values(by=['user_index'])
    unique_item = user_item_df[['item_index', 'item_id']].drop_duplicates().sort_values(by=['item_index'])
    assert(np.max(unique_item['item_index']) + 1 == len(unique_item['item_index']))
    assert(np.max(unique_user['user_index']) + 1 == len(unique_user['user_index']))
    return unique_user, unique_item, user_item_df[['user_index', 'item_index', 'score']]


def similar_to_csv(model, k, user_item_ratings, unique_item):
    similar_df = pd.DataFrame(data={'item_id': unique_item['item_id'], 'item_index': unique_item['item_index']})

    def get_topk(_item, _k):
        similar_arary = []
        if user_item_ratings.indptr[_item] != user_item_ratings.indptr[_item + 1]:
            candidate_score = model.similar_items(_item, k + 1)
            first_dot = 1
            for index, (candidate, score) in enumerate(candidate_score):
                if index == 0:
                    first_dot = score
                    continue
                similar_arary.append('{}={}'.format(unique_item['item_id'].values[candidate], score / first_dot))
        return similar_arary

    iterations = 1000
    i = 0
    while i * iterations < len(similar_df):
        similar_df_slice = similar_df.iloc[i * iterations: (i + 1) * iterations]
        similar_df_slice['topk'] = similar_df_slice['item_index'].apply(lambda _item_index: get_topk(_item_index, k))
        i += 1
        yield similar_df_slice


def calculate_similar_movies(input_filename,
                             output_filename,
                             model_name="als", min_rating=4.0,
                             variant='20m'):
    # read in the input data file
    start = time.time()
    # titles, ratings = get_movielens(variant)

    user_item_df = read_user_item_data(input_filename)
    print(user_item_df)
    unique_user, unique_item, user_item_df = get_user_item_sparse_data_presto(
        user_item_df)

    #user_item_df = user_item_df.sort_values(by=['user_index','item_index'])
    user_item_ratings = scipy.sparse.csr_matrix(
        (user_item_df['score'], (user_item_df['item_index'], user_item_df['user_index'])))
    print(user_item_ratings)
    '''
    # remove things < min_rating, and convert to implicit dataset
    # by considering ratings as a binary preference only
    ratings.data[ratings.data < min_rating] = 0
    ratings.eliminate_zeros()
    ratings.data = np.ones(len(ratings.data))
    '''

    log.info("read data file in %s", time.time() - start)

    # generate a recommender model based off the input params
    if model_name == "als":
        model = AlternatingLeastSquares(
            factors=10, regularization=0.01, use_native=True, iterations=1, calculate_training_loss=True)

        # lets weight these models by bm25weight.
        log.debug("weighting matrix by bm25_weight")
        # ratings = (bm25_weight(ratings, B=0.9) * 5).tocsr()

    elif model_name == "bpr":
        model = BayesianPersonalizedRanking()

    elif model_name == "lmf":
        model = LogisticMatrixFactorization()

    elif model_name == "tfidf":
        model = TFIDFRecommender()

    elif model_name == "cosine":
        model = CosineRecommender()

    elif model_name == "bm25":
        model = BM25Recommender(B=0.2)

    else:
        raise NotImplementedError("TODO: model %s" % model_name)

    # train the model
    log.debug("training model %s", model_name)
    start = time.time()
    model.fit(user_item_ratings)
    log.debug("trained model '%s' in %s", model_name, time.time() - start)
    log.debug("calculating top movies")

    similar_df_gen = similar_to_csv(model, 10, user_item_ratings, unique_item)
    for similar_df_slice in similar_df_gen:
        similar_df_slice.to_csv(args.outputfile, mode='a', header=False, index=False)


    '''
    user_count = np.ediff1d(user_item_ratings.indptr)
    to_generate = sorted(np.arange(len(unique_item)),
                         key=lambda x: -user_count[x])

    log.debug("calculating similar movies")

    with tqdm.tqdm(total=len(to_generate)) as progress:
        with codecs.open(output_filename, "w", "utf8") as o:
            for item_id in to_generate:
                # if this movie has no ratings, skip over (for instance 'Graffiti Bridge' has
                # no ratings > 4 meaning we've filtered out all data for it.
                if user_item_ratings.indptr[item_id] != user_item_ratings.indptr[item_id + 1]:
                    title = unique_item[item_id]
                    for other, score in model.similar_items(item_id, 11):
                        o.write("%s\t%s\t%s\n" %
                                (title, unique_item[other], score))
                progress.update(1)
    '''

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates related movies from the MovieLens 20M "
                                     "dataset (https://grouplens.org/datasets/movielens/20m/)",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--input', type=str, default='test.csv',
                        dest='inputfile', help='input file name')
    parser.add_argument('--output', type=str, default='similar-movies.tsv',
                        dest='outputfile', help='output file name')
    parser.add_argument('--model', type=str, default='als',
                        dest='model', help='model to calculate (als/bm25/tfidf/cosine)')
    parser.add_argument('--variant', type=str, default='20m', dest='variant',
                        help='Whether to use the 20m, 10m, 1m or 100k movielens dataset')
    parser.add_argument('--min_rating', type=float, default=4.0, dest='min_rating',
                        help='Minimum rating to assume that a rating is positive')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    calculate_similar_movies(input_filename=args.inputfile,
                             output_filename=args.outputfile,
                             model_name=args.model,
                             min_rating=args.min_rating, variant=args.variant)

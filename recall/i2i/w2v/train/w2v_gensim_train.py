import tqdm
import argparse
import pandas as pd
import numpy as np
from gensim.similarities.index import AnnoyIndexer
from gensim.models.callbacks import CallbackAny2Vec
from gensim.models.fasttext import FastText
from fasttext import train_unsupervised
import gensim.models


def item_session(file_name, chunk_size):
    for chunk in pd.read_csv(file_name, chunksize=chunk_size):
        yield chunk.values


def get_similarity(model, args):
    indexer = AnnoyIndexer(model, 10)
    similarity = {}
    with tqdm.tqdm(desc="get_similarity", total=len(model.wv.vectors)) as progress:
        for word in model.wv.vocab:
            similarity[word] = ['{}={}'.format(cscore, cword) for cscore, cword in model.wv.most_similar(word, topn=args.k, indexer=indexer)]
            progress.update(1)
    return similarity


def save_csv(similarity, args):
    topk_df = pd.DataFrame({'item': np.sort(list(similarity.keys()))})
    with tqdm.tqdm(desc='save_csv', total=len(similarity.keys()) // args.save_one_time) as progress:
        for i in range(0, len(similarity.keys()), args.save_one_time):
            topk_df['topk'] = topk_df.iloc[i: i+args.save_one_time]['item'].apply(lambda x: similarity[x])
            topk_df.to_csv(args.output_file, mode='a', header=False, index=False)
            progress.update(1)


class callback(CallbackAny2Vec):
    '''Callback to print loss after each epoch.'''

    def __init__(self, args):
        self.epoch = 0
        self.args = args

    def on_epoch_end(self, model):
        loss = model.get_latest_training_loss()
        model.save('{}.{}'.format(self.args.model_file, str(self.epoch)))
        print('Loss after epoch {}: {}'.format(self.epoch, loss))
        self.epoch += 1


def main(args):

    #model = gensim.models.Word2Vec(corpus_file=args.input_file, iter=5, window=10, alpha=0.001, min_alpha=0.0001, sg=1, hs=1, compute_loss=True, workers=25, callbacks=[callback(args)])
    model = train_unsupervised(
        input=args.input_file,
        model='skipgram',
    )
    model.save_model(args.model_file)
    model= gensim.models.fasttext.load_facebook_model(args.model_file)
    similarity = get_similarity(model, args)
    save_csv(similarity, args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fuck i2i",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--input', type=str, default='', dest='input_file')
    parser.add_argument('--model_output', type=str, default='gensim_w2v.model', dest='model_file')
    parser.add_argument('--output', type=str, default='gensim_output.csv', dest='output_file')
    parser.add_argument('--top_k', type=int, default=10, dest='k')
    parser.add_argument('--chunk_size', type=int, default=1000000, dest='chunk_size')
    parser.add_argument('--save_one_time', type=int, default=2000, dest='save_one_time')
    args = parser.parse_args()

    main(args)
from fasttext import train_unsupervised
import tqdm
import argparse
import pandas as pd
import numpy as np


def get_similarity(model, args):
    similarity = {}
    with tqdm.tqdm(desc="get_similarity", total=len(model.words)) as progress:
        for word in model.words:
            similarity[word] = ['{}={}'.format(cword, cscore) for cscore, cword in model.get_nearest_neighbors(word, k=args.k)]
            progress.update(1)
    return similarity


def save_csv(similarity, args):
    topk_df = pd.DataFrame({'item': np.sort(list(similarity.keys()))})
    with tqdm.tqdm(desc='save_csv', total=len(similarity.keys()) // args.save_one_time) as progress:
        for i in range(0, len(similarity.keys()), args.save_one_time):
            topk_df['topk'] = topk_df.iloc[i: i+args.save_one_time]['item'].apply(lambda x: similarity[x])
            topk_df.to_csv(args.outputfile, mode='a', header=False, index=False)
            progress.update(1)


def main(args):
    model = train_unsupervised(
        input=args.input_file,
        model='skipgram',
    )

    model.save_model(args.model_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fuck i2i",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--input', type=str, default='', dest='input_file')
    parser.add_argument('--model_output', type=str, default='gensim_w2v.model.vec', dest='model_file')
    parser.add_argument('--output', type=str, default='', dest='output_file')
    parser.add_argument('--topk', type=int, default=10, dest='k')
    parser.add_argument('--save_one_time', type=int, default=2000, dest='save_one_time')
    args = parser.parse_args()

    main(args)
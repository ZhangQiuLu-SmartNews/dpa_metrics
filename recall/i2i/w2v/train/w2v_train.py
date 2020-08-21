from fasttext import train_unsupervised
import tqdm
import argparse


def get_similarity(model, args):
    similarity = {}
    with tqdm.tqdm(total=len(model.words)) as progress:
        for word in model.words:
            similarity[word] = ['{}={}'.format(cscore, cword) for cscore, cword in model.get_nearest_neighbors(word, k=args.k)]
            progress.update(1)
    return similarity

def main(args):
    model = train_unsupervised(
        input=args.inputfile,
        model='skipgram',
    )

    similarity_dict = get_similarity(model, args)
    print(similarity_dict)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fuck i2i",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--input', type=str, default='', dest='inputfile')
    parser.add_argument('--topk', type=int, default=10, dest='k')
    args = parser.parse_args()

    main(args)
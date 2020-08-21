from fasttext import train_unsupervised
import os
import argparse


def main(args):
    model = train_unsupervised(
        input=args.inputfile,
        model='skipgram',
    )
    print(model.words)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fuck i2i",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--input', type=str, default='test.csv', dest='inputfile')
    args = parser.parse_args()

    main(args)
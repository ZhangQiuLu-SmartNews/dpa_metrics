from fasttext import train_unsupervised
import os


if __name__ == "__main__":
    model = train_unsupervised(
        input=os.path.join(os.getenv("DATADIR", ''), 'fil9'),
        model='skipgram',
    )
    print(model.words)
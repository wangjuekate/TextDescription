
from utils1 import create_input_files, train_word2vec_model

if __name__ == '__main__':
    create_input_files(csv_folder='/Users/jjw6286/Downloads/Machinelearning/a-PyTorch-Tutorial-to-Text-Classification/datasettotrain',
                       output_folder='/Users/jjw6286/Downloads/Machinelearning/a-PyTorch-Tutorial-to-Text-Classification',
                       sentence_limit=15,
                       word_limit=20,
                       min_word_count=5)

    train_word2vec_model(data_folder='/Users/jjw6286/Downloads/Machinelearning/a-PyTorch-Tutorial-to-Text-Classification',
                         algorithm='skipgram')

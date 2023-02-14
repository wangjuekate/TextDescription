
from utils1 import create_input_files, train_word2vec_model

if __name__ == '__main__':
    create_input_files(csv_folder='~/TextDescription/IdentifySub_Com/datatraining',
                       output_folder='/home/wangjuekate/TextDescription/IdentifySub_Com',
                       sentence_limit=15,
                       word_limit=20,
                       min_word_count=5)

    train_word2vec_model(data_folder='/home/wangjuekate/TextDescription/IdentifySub_Com',
                         algorithm='skipgram')

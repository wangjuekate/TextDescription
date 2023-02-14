import torch
from torch import nn
from utils1 import preprocess, rev_label_map
import json
import os
from nltk.tokenize import PunktSentenceTokenizer, TreebankWordTokenizer
import pymongo
import pandas as pd
from json import loads
import numpy as np
import math
import json
import time
from pymongo import MongoClient

import tqdm                                                                                                   
import numpy as np
import pandas as pd

import concurrent.futures
import multiprocessing
num_processes = multiprocessing.cpu_count()


device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# Load model
checkpoint = 'checkpoint_han.pth.tar'
checkpoint = torch.load(checkpoint)
model = checkpoint['model']
model = model.to(device)
model.eval()

# Pad limits, can use any high-enough value since our model does not compute over the pads
sentence_limit = 15
word_limit = 20

# Word map to encode with
data_folder = ''
with open(os.path.join(data_folder, 'word_map.json'), 'r') as j:
    word_map = json.load(j)

# Tokenizers
sent_tokenizer = PunktSentenceTokenizer()
word_tokenizer = TreebankWordTokenizer()


def classify(commentx, commenty,desx,desy):
    document = commentx+commenty+desx+desy
    """
    Classify a document with the Hierarchial Attention Network (HAN).

    :param document: a document in text form
    :return: pre-processed tokenized document, class scores, attention weights for words, attention weights for sentences, sentence lengths
    """
    # A list to store the document tokenized into words
    doc = list()

    # Tokenize document into sentences
    sentences = list()
    for paragraph in preprocess(document).splitlines():
        sentences.extend([s for s in sent_tokenizer.tokenize(paragraph)])

    # Tokenize sentences into words
    for s in sentences[:sentence_limit]:
        w = word_tokenizer.tokenize(s)[:word_limit]
        if len(w) == 0:
            continue
        doc.append(w)

    # Number of sentences in the document
    sentences_in_doc = len(doc)
    sentences_in_doc = torch.LongTensor([sentences_in_doc]).to(device)  # (1)

    # Number of words in each sentence
    words_in_each_sentence = list(map(lambda s: len(s), doc))
    words_in_each_sentence = torch.LongTensor(words_in_each_sentence).unsqueeze(0).to(device)  # (1, n_sentences)

    # Encode document with indices from the word map
    encoded_doc = list(
        map(lambda s: list(map(lambda w: word_map.get(w, word_map['<unk>']), s)) + [0] * (word_limit - len(s)),
            doc)) + [[0] * word_limit] * (sentence_limit - len(doc))
    encoded_doc = torch.LongTensor(encoded_doc).unsqueeze(0).to(device)

    # Apply the HAN model
    scores, word_alphas, sentence_alphas = model(encoded_doc, sentences_in_doc,
                                                 words_in_each_sentence)  # (1, n_classes), (1, n_sentences, max_sent_len_in_document), (1, n_sentences)
    scores = scores.squeeze(0)  # (n_classes)
    scores = nn.functional.softmax(scores, dim=0)  # (n_classes)
    word_alphas = word_alphas.squeeze(0)  # (n_sentences, max_sent_len_in_document)
    sentence_alphas = sentence_alphas.squeeze(0)  # (n_sentences)
    words_in_each_sentence = words_in_each_sentence.squeeze(0)  # (n_sentences)
    score, prediction = scores.max(dim=0)
    output = [prediction.item(),score.item()]
    return(output)   



if __name__ == '__main__':    
    alltoclassify = pd.read_csv("~/TextDescription/IdentifySub_Com/datatraining/Alltoclassify_des.csv",sep=",")
    alltoclassify =alltoclassify.head(10)
    alltoclassify =alltoclassify.head(10).fillna('') 
    with concurrent.futures.ProcessPoolExecutor(num_processes) as pool:
        alltoclassify['label'], alltoclassify['score'] = list(tqdm.tqdm(pool.map(classify, alltoclassify['comments_x'],alltoclassify['comments_y'],alltoclassify['description_x'],alltoclassify['description_y'], chunksize=10), total=alltoclassify.shape[0]))
    alltoclassify.to_csv("~/TextDescription/IdentifySub_Com/classifiedfile.csv",sep=",", index= False)                     
                     
                          

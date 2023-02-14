import torch
from torch import nn
from utils1 import preprocess, rev_label_map
import json
import os
from nltk.tokenize import PunktSentenceTokenizer, TreebankWordTokenizer

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


def classify(document):
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

    return doc, scores, word_alphas, sentence_alphas, words_in_each_sentence


if __name__ == '__main__':
    document = 'How do computers work? I have a CPU I want to use. But my keyboard and motherboard do not help.\n\n You can just google how computers work. Honestly, its easy.'
    document = 'But think about it! It\'s so cool. Physics is really all about math. what feynman said, hehe'
    document = "I think I'm falling sick. There was some indigestion at first. But now a fever is beginning to take hold."
    document = "I want to tell you something important. Get into the stock market and investment funds. Make some money so you can buy yourself some yogurt."
   # document = "You know what's wrong with this country? republicans and democrats. always at each other's throats\n There's no respect, no bipartisanship."
    output = classify(document)
    print(output)

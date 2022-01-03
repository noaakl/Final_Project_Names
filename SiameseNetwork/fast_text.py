from _csv import writer
import pandas as pd
import fasttext.util

#
# TODO: add competitor corpus names
# df = pd.read_csv("knn_suggestions_according_sound_pandas_imp_sorted_by_ed.csv")
# orgs = df['Original'].drop_duplicates()
# candidates = df['Candidate'].drop_duplicates()
#
# with open('./name_vectors_dataset.csv', 'a') as f_object:
#     writer_object = writer(f_object, delimiter =' ')
#     writer_object.writerow(orgs)
#     writer_object.writerow(candidates)
#     f_object.close()
#
# model = fasttext.train_unsupervised("name_vectors_dataset.csv", model = 'skipgram', minCount  = 1)
# model.save_model("name2vec_model.bin")
import torch

model = fasttext.load_model("name2vec_model.bin")

# print(model['king'])
# print(model.get_word_vector('king'))
print(torch.Tensor(model.get_word_vector('king')))
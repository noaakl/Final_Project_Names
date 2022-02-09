from _csv import writer
import pandas as pd
import fasttext.util

#
# TODO: add competitor corpus names
# TODO: MAKE A LIST OF ALL ZEROS AND ADD ALL AT ONCE
# df = pd.read_csv("knn_suggestions_according_sound_pandas_imp_sorted_by_ed.csv")
missing_names = pd.read_csv("missing_names.csv")
# orgs = df['Original'].drop_duplicates()
# candidates = df['Candidate'].drop_duplicates()
#
# with open('./name_vectors_dataset.csv', 'a') as f_object:
#     writer_object = writer(f_object, delimiter =' ')
#     writer_object.writerow(orgs)
#     writer_object.writerow(candidates)
#     f_object.close()
#
# model = fasttext.train_unsupervised("name_vectors_dataset_two.csv", model = 'skipgram', minCount  = 1)
# check: wordNgrams = 3
# model.save_model("name2vec_model.bin")
# import torch
# print(missing_names)
names = missing_names['S h e r l y n n']
s = pd.Series(['S h e r l y n n'])
x = names.append(s)
# print(x)
# names = missing_names.columns.split(' ')
names = []
for name in x:
    name = name.replace(" ", "")
    names.append(name)
#
model = fasttext.load_model("name2vec_model.bin")
# # # # print(model['king'])
import pickle
for name in names:
  vec = model.get_word_vector(name)
  with open("missing_fasttext_vecs8/"+name+".pkl", 'wb') as f:
    pickle.dump(vec, f)


# fasttext loading vector trial
# import pickle
# with open("Aaafje.pkl", 'rb') as f:
#     word_vec = pickle.load(f)
# print(word_vec)
# print(model.get_word_vector('king'))
# print(torch.Tensor(model.get_word_vector('king')))
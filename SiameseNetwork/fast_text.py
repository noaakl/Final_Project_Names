# from _csv import writer
# import pandas as pd
# import fasttext.util
#
# #
# # TODO: add competitor corpus names
# # TODO: MAKE A LIST OF ALL ZEROS AND ADD ALL AT ONCE
# # df = pd.read_csv("knn_suggestions_according_sound_pandas_imp_sorted_by_ed.csv")
# missing_names = pd.read_csv("missing_names.csv")
# # orgs = df['Original'].drop_duplicates()
# # candidates = df['Candidate'].drop_duplicates()
# #
# # with open('./name_vectors_dataset.csv', 'a') as f_object:
# #     writer_object = writer(f_object, delimiter =' ')
# #     writer_object.writerow(orgs)
# #     writer_object.writerow(candidates)
# #     f_object.close()
# #
# # model = fasttext.train_unsupervised("name_vectors_dataset_two.csv", model = 'skipgram', minCount  = 1)
# # check: wordNgrams = 3
# # model.save_model("name2vec_model.bin")
# # import torch
# # print(missing_names)
# names = missing_names['S h e r l y n n']
# s = pd.Series(['S h e r l y n n'])
# x = names.append(s)
# # print(x)
# # names = missing_names.columns.split(' ')
# names = []
# for name in x:
#     name = name.replace(" ", "")
#     names.append(name)
# #
# model = fasttext.load_model("name2vec_model.bin")
# # # # # print(model['king'])
# import pickle
# for name in names:
#   vec = model.get_word_vector(name)
#   with open("missing_fasttext_vecs8/"+name+".pkl", 'wb') as f:
#     pickle.dump(vec, f)
#
#
# # fasttext loading vector trial
# # import pickle
# # with open("Aaafje.pkl", 'rb') as f:
# #     word_vec = pickle.load(f)
# # print(word_vec)
# # print(model.get_word_vector('king'))
# # print(torch.Tensor(model.get_word_vector('king')))



### concat datasets
import pandas as pd

# path = "./Siamese_Datasets/Competitor_Results/"
# data_type = "Nysiis"
# df0 = pd.read_csv(path + data_type + "/top_ten_suggestions_for_gt_by_" + data_type + "_0_with_gt.csv")
# df1 = pd.read_csv(path + data_type + "/top_ten_suggestions_for_gt_by_" + data_type + "_1_with_gt.csv")
# df2 = pd.read_csv(path + data_type + "/top_ten_suggestions_for_gt_by_" + data_type + "_2_with_gt.csv")
# df3 = pd.read_csv(path + data_type + "/top_ten_suggestions_for_gt_by_" + data_type + "_3_with_gt.csv")
# df4 = pd.read_csv(path + data_type + "/top_ten_suggestions_for_gt_by_" + data_type + "_4_with_gt.csv")
# dfs = [df0,df1, df2, df3, df4]
# united_df = pd.concat(dfs)
# united_df = united_df.sort_values(by='Original')
# united_df.to_csv(path + data_type + "/" + data_type + "_names.csv")


path = "./Siamese_Datasets/Competitor_Results/"
data_types = ["Double_Metaphone","Metaphone","Matching_Rating_Codex","Nysiis"]
dfs = []
for i in range(len(data_types)):
    df = pd.read_csv(path + data_types[i] +"/"+data_types[i] + "_names.csv")
    dfs.append(df)
united_df = pd.concat(dfs)
united_df = united_df.sort_values(by='Original')
united_df.to_csv(path+"./negative_example_names.csv")



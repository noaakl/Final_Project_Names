import pickle
import matplotlib.pylab as plt
import torch
from _csv import writer


def upload_gram_frequencies_dict1():
    with open('gram_frequencies_dictionary_1.pkl', 'rb') as f:
        loaded_dict = pickle.load(f)
    return loaded_dict


def upload_gram_frequencies_dict2():
    with open('gram_frequencies_dictionary_2.pkl', 'rb') as f:
        loaded_dict = pickle.load(f)
    return loaded_dict


def upload_gram_frequencies_dict3():
    with open('gram_frequencies_dictionary_3.pkl', 'rb') as f:
        loaded_dict = pickle.load(f)
    return loaded_dict


one_gram_dict = {}
two_gram_dict = {}
three_gram_dict = {}


def sort_by_grams(grams_dict):
    # dict = three_gram_dict
    for key, value in grams_dict.items():
        if len(key) == 1:  # 1 gram
            dict = one_gram_dict
        elif len(key) == 2:  # 2 gram
            dict = two_gram_dict
        else:
            dict = three_gram_dict

        if key not in dict:
            dict[key] = value
        else:
            dict[key] += value


# gram_frequencies3 = upload_gram_frequencies_dict3()
# # print(gram_frequencies3)
# # sort_by_grams(gram_frequencies2)
# # # # sort grams by number of appearances
# # # grams_1 = sorted(one_gram_dict.items(), key=lambda k: k[1], reverse=True)
# # grams_2 = sorted(two_gram_dict, key=lambda k: two_gram_dict[k], reverse=True)
# grams_3 = sorted(gram_frequencies3.items(), key=lambda k: k[1], reverse=True)
# # # #
# # # print(grams_1)
# # # print(grams_2)
# print(grams_3)
# # #
# # # x1, y1 = zip(*grams_1)  # unpack a list of pairs into two tuples
# # # x2, y2 = zip(*grams_2)  # unpack a list of pairs into two tuples
# x3, y3 = zip(*grams_3)  # unpack a list of pairs into two tuples
# #
# print(x3)
# # print(y3)
# # from csv import writer
# # # #
# with open('./all_top_grams.csv', 'a') as f_object:
#     # Pass this file object to csv.writer()
#     # and get a writer object
#     writer_object = writer(f_object)
# #     # for word in y1:
# #     #     writer_object.writerow([word])
# #     # Pass the list as an argument into
# #     # the writerow()
#     writer_object.writerow(x3)
# #     # Close the file object
#     f_object.close()

# plt.xlabel("Gram")
# plt.ylabel("Frequency")
# plt.plot(x1, y1)
# plt.show()

# plt.xlabel("Gram")
# plt.ylabel("Frequency")
# plt.plot(x2, y2)
# plt.show()

# plt.xlabel("Gram")
# plt.ylabel("Frequency")
# plt.plot(x3, y3)
# plt.show()


import pandas as pd
all_top_grams_csv = pd.read_csv("all_top_grams.csv").columns

f = all_top_grams_csv[:50]
name_rep = [0] * 50
print(name_rep)
print(f)
print(len(f))
print(len(name_rep))
import pickle
import matplotlib.pylab as plt
import torch


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


gram_frequencies3 = upload_gram_frequencies_dict3()
# print(gram_frequencies3)
sort_by_grams(gram_frequencies3)
# # sort grams by number of appearances
# grams_1 = sorted(one_gram_dict.items(), key=lambda k: k[1], reverse=True)
# grams_2 = sorted(two_gram_dict.items(), key=lambda k: k[1], reverse=True)
grams_3 = sorted(three_gram_dict.items(), key=lambda k: k[1], reverse=True)
# #
# print(grams_1)
# print(grams_2)
print(grams_3)
#
# x1, y1 = zip(*grams_1)  # unpack a list of pairs into two tuples
# x2, y2 = zip(*grams_2)  # unpack a list of pairs into two tuples
x3, y3 = zip(*grams_3)  # unpack a list of pairs into two tuples

# print(x1)
# print(y1)
from csv import writer
#
with open('./grams3.csv', 'a') as f_object:
    # Pass this file object to csv.writer()
    # and get a writer object
    writer_object = writer(f_object)
    # for word in y1:
    #     writer_object.writerow([word])
    # Pass the list as an argument into
    # the writerow()
    writer_object.writerow(x3)
    writer_object.writerow(y3)
    # Close the file object
    f_object.close()

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


# # Convert names to sparse
# def word2sparse(name, name_letter_split):
#     name = name.lower()
#     # name_split = [name[i:i + name_letter_split] for i in range(len(name) - (name_letter_split - 1))]
#     # calculate_gram_frequency(gram_frequencies_dict, name_split)
#     name = [26] + [ord(c) - ord('a') for c in name] + [27]
#     name = [name[i:i + name_letter_split] for i in range(len(name) - (name_letter_split - 1))]
#     print(name)
#     nameidx = None
#     if name_letter_split ==1:
#         nameidx = torch.tensor([[0, pair[0]] for pair in name])
#     elif name_letter_split ==2:
#         nameidx = torch.tensor([[0, pair[0] * 28 + pair[1]] for pair in name])
#     elif name_letter_split ==3:
#         nameidx = torch.tensor([[0, pair[0] * (28**2) + pair[1] * 28 + pair[2]] for pair in name])
#
#     values = torch.ones(len(name))
#     s = torch.sparse_coo_tensor(nameidx.t(), values, (1, 28 ** 2))
#     return s, 28 ** 2
#
# print(word2sparse('hila',1))
# print(word2sparse('hila',2))
# print(word2sparse('hila',3))

import pickle
import matplotlib.pylab as plt


def upload_gram_frequencies_dict():
    with open('gram_frequencies_dictionary.pkl', 'rb') as f:
        loaded_dict = pickle.load(f)
    return loaded_dict


one_gram_dict = {}
two_gram_dict = {}
three_gram_dict = {}


def sort_by_grams(grams_dict):
    dict = three_gram_dict
    for key, value in grams_dict.items():
        if len(key) == 1:  # 1 gram
            dict = one_gram_dict
        elif len(key) == 2:  # 2 gram
            dict = one_gram_dict

        if key not in dict:
            dict[key] = value
        else:
            dict[key] += value


gram_frequencies = upload_gram_frequencies_dict()
sort_by_grams(gram_frequencies)
# sort grams by number of appearances
grams_1 = sorted(one_gram_dict.items(), key=lambda k: k[1], reverse=True)
grams_2 = sorted(two_gram_dict.items(), key=lambda k: k[1], reverse=True)
grams_3 = sorted(three_gram_dict.items(), key=lambda k: k[1], reverse=True)

print(grams_1)
print(grams_2)
print(grams_3)

x1, y1 = zip(*grams_1)  # unpack a list of pairs into two tuples
x2, y2 = zip(*grams_1)  # unpack a list of pairs into two tuples
x3, y3 = zip(*grams_3)  # unpack a list of pairs into two tuples

plt.xlabel("Gram")
plt.ylabel("Frequency")
plt.plot(x1, y1)
plt.show()

plt.xlabel("Gram")
plt.ylabel("Frequency")
plt.plot(x2, y2)
plt.show()

plt.xlabel("Gram")
plt.ylabel("Frequency")
plt.plot(x3, y3)
plt.show()

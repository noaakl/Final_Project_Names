import pickle
import matplotlib.pylab as plt


def upload_gram_frequencies_dict():
    with open('saved_dictionary.pkl', 'rb') as f:
        loaded_dict = pickle.load(f)
    return loaded_dict


gram_frequencies = upload_gram_frequencies_dict()


lists = sorted(gram_frequencies.items()) # sorted by key, return a list of tuples

x, y = zip(*lists) # unpack a list of pairs into two tuples

plt.plot(x, y)
plt.show()
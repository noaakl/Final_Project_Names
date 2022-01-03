import string
import torch
import itertools
import random
import fasttext

"""**Creating Vectors**

In this notebook we use an embedding method inspired by FastText sub-word generation. For a word, we generate charecter b-grams in it.


*   We take a name and add angular brackets to denote the beginning and end of a word. E.g **Marc** --> **\<marc>**
*   Then, we generate charecter b-grams. For exaple: **\<marc>** --> **[<m, ma, ar, rc, c>]**
*   Then, we take each pair of letter as a number in base 28 and convert them to base 10. ('<' = 26, '>' = 27)

We have two ways to represent the vectors:
1. Dense - A list of zeros and ones. We put 1 in the indexes that represent the b-grams. For exaple, the list **[<a, aa, a>]** will have ones in 728, 0 and 27.
2. Sparse- Pytorch has a special object that helps represent big vectors with a lot of zeros in more efficient way. You can read about sparse [here](https://pytorch.org/docs/stable/sparse.html).

"""


def calculate_gram_frequency(dict, name_split):
    """
    The function is used to build the gram dictionary for calculating frequency
    """
    for gram in name_split:
        if gram in dict:
            dict[gram] += 1
        else:
            dict[gram] = 1


# Convert names to sparse
def word2sparse(name, name_letter_split, gram_frequencies_dict = {}):
    '''
    The function creates a vector as type of bag of words where in each coordinate a 1 is place representing
    its existence of the value
    torch.sparse_coo_tensor receives the matrix of indices (transposed), the values to place (ones)
    and the vector size
    '''
    name = name.lower()
    # name_split = [name[i:i + name_letter_split] for i in range(len(name) - (name_letter_split - 1))]
    # calculate_gram_frequency(gram_frequencies_dict, name_split)
    name = [26] + [ord(c) - ord('a') for c in name] + [27] # start + end (26 letters)
    name = [name[i:i + name_letter_split] for i in range(len(name) - (name_letter_split - 1))]
    nameidx = None
    if name_letter_split == 1:
        nameidx = torch.tensor([[0, pair[0]] for pair in name])
    elif name_letter_split == 2:
        nameidx = torch.tensor([[0, pair[0] * 28 + pair[1]] for pair in name])
    elif name_letter_split == 3:
        nameidx = torch.tensor([[0, pair[0] * (28 ** 2) + pair[1] * 28 + pair[2]] for pair in name])
    values = torch.ones(len(name))
    s = torch.sparse_coo_tensor(nameidx.t(), values, (1, 28 ** name_letter_split))
    # nnz -> how many are filled , tensor dimensions
    return s, 28 ** name_letter_split # 28 ** 2


# Convert names to dense
def word2dense(name, name_letter_split):
    name = name.lower()
    name = [26] + [ord(c) - ord('a') for c in name] + [27]
    name = [name[i:i + name_letter_split] for i in range(len(name) - 1)]
    nameidx = [pair[0] * 28 + pair[1] for pair in name]
    dense = torch.zeros(28 ** 2)
    for idx in nameidx:
        dense[idx] = 1
    return dense, 28 ** 2


def create_grams_dict(number_of_grams):
    """
    The function creates a gram dictionary using the number_of_grams input given in order to create a representation
    for the letter combinations
    """
    alphabet = list(string.ascii_lowercase) # + ['<'] + ['>'] # alphabet & start + end chars
    if number_of_grams == 1:
        alph_dict = dict((alphabet[indx], indx) for indx in range(len(alphabet)))
    else:
        combos = list(itertools.product(alphabet, repeat=number_of_grams))
        random.shuffle(combos)
        letter_combinations = [''.join(tup) for tup in combos]
        alph_dict = dict((letter_combinations[idx], idx) for idx in range(len(letter_combinations)))
    return alph_dict, len(alph_dict)


def word_ngrams(name, number_of_grams):
    """
    The function splits the name into the given amount of chars in 'number_of_grams' and return a vector representation using
    'create_grams_dict'
    """
    alph_dict, vector_size = create_grams_dict(number_of_grams)
    name = name.lower()
    # first num is grams second num is grams-1 for split
    name_split = [name[i:i + number_of_grams] for i in range(len(name) - (number_of_grams - 1))]
    name_vec = torch.tensor([[0, alph_dict[chars]] for chars in name_split])
    values = torch.zeros(len(name_vec))
    s = torch.sparse_coo_tensor(name_vec.t(), values, (1, vector_size))
    return s, vector_size


# convert names to vec representation by using char2vec
# def name2vec(name,name_letter_split):
#   name = name.lower()
#   c2v_model = chars2vec.load_model('eng_50')
#   word_embeddings = c2v_model.vectorize_words([name])
#   return word_embeddings.__getitem__(0)

def name2vec_addition(name, name_letter_split):
    """
    The function converts name to vector representation by using addition
    """
    name = name.lower()
    name = [26] + [ord(c) - ord('a') for c in name] + [27]
    name = [name[i:i + 2] for i in range(len(name) - 1)]
    sum_pair = [0, 0]
    for pair in name:
        sum_pair[0] += pair[0]
        sum_pair[1] += pair[1]
    return sum_pair


def nam2vec_fasttext(name, name_letter_split = 2):
    """
    The function uses a trained fasttext model (by the ground truth names corpus) to create a vector
    representing the given word
    """
    model = fasttext.load_model("name2vec_model.bin")
    word_vec = torch.Tensor(model.get_word_vector(name))
    return word_vec, len(word_vec)
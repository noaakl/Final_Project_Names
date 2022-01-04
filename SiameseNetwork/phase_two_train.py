import pandas as pd
import random
import siamese_network_bgrams_updated

'''
Phase to add Negative Examples from methods of name representation as vectors (can switch with larger file later)
'''


def second_phase_negative_examples(short_file_path, dataset, filtered_df):
    """
    The method reads the vector representation csv files and creates a df of the names the
    algorithms' received distance was large as negative examples
    """
    # The three methods to represent the names as vectors
    turicreate_df = pd.read_csv(short_file_path + dataset)
    turicreate_df['Method'] = 'turicreate'
    wav2vec_df = pd.read_csv(short_file_path + "knn_suggestions_according_sound_pandas_imp_wav2vec.csv")
    wav2vec_df['Method'] = 'wav2vec'
    pyAudioAnalysis_df = pd.read_csv(short_file_path + "knn_suggestions_according_sound_pandas_imp_pyAudioAnalysis.csv")
    pyAudioAnalysis_df['Method'] = 'pyAudioAnalysis'
    # add negative examples from vector representation methods
    # turicrete mistakes have larger distance from other methods
    turicreate_df = turicreate_df[turicreate_df['Distance'] >= 4]  # add distance to parameters?
    dfs = [turicreate_df, wav2vec_df, pyAudioAnalysis_df]
    methods_df = pd.concat(dfs)
    methods_df = methods_df[methods_df['Distance'] >= 1]  # add distance to parameters?
    methods_df = methods_df.rename(columns={'Candidate': 'Negative'})
    filtered_df = filtered_df.rename(columns={'Candidate': 'Positive',
                                              'Distance': 'Distance_Spoken_Name',
                                              'Edit_Distance': 'Edit_Distance_Spoken_Name'})
    # merge the vector representation df with the filtered df
    sn_gt_df = pd.merge(methods_df, filtered_df, how="inner", left_on='Original', right_on='Original')
    # trim the df
    triplets_df = sn_gt_df[['Original', 'Positive', 'Negative']]
    # Drop duplicate (original + positive) pairs
    unique_df = triplets_df.drop_duplicates(subset=['Original', 'Positive'])
    return unique_df


# !!!!!!!!!!!!!!!! NEEDED? CHECK
# CHECK IF NEEDED AFTER DUPLICATE REMOVE FUNCTION
# # find unique pairs(original, positive)
# originals = triplets_df['Original'].tolist()
# positives = triplets_df['Positive'].tolist()
# negatives = triplets_df['Negative'].tolist()

# pairs = []
# for i, org in enumerate(originals):
#   pairs += [org + positives[i]]

# pairs_df = pd.DataFrame({'Pairs': pairs, 'originals': originals,
#                          'Positive': positives, 'Negative': negatives})

# TODO: check if needed
def add_random_negatives_phase_two(unique_df):
    # create triplets of data ###################check if needed (easy negative examples)
    originals = unique_df['Original'].tolist()
    positives = unique_df['Positive'].tolist()
    negatives = unique_df['Negative'].tolist()
    trios = []
    for i, org in enumerate(originals):
        trios += [[org, positives[i], negatives[i]]]
        random_negative = siamese_network_bgrams_updated.negative(originals, i)
        trios += [[originals[i], positives[i], random_negative]]

    random.shuffle(trios)

    print(trios[0])
    print(len(trios))
    return trios


def train_test_split_phase_two(train_data, test_data, trios):
    # l = int(0.66 * len(unique_df))
    # train_data = unique_df[:l]
    # test_data = unique_df[l:]
    # test_data
    ######################### DELETE IF NOT NEEDED TO ADD RANDOM NEGATIVES (use above)
    l = int(0.66 * len(trios))
    train_data += trios[:l]
    test_data += trios[l:]
    return train_data, test_data
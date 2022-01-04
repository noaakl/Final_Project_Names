import pandas as pd
from tqdm import tqdm
import random
import csv
import siamese_network_bgrams_updated

'''
Phase to add Negative Examples from competitors
'''


def first_phase_negative_examples(long_file_path, competitor_dataset):
    """
    The method reads the competitor csv files and creates a df of the names the
    algorithm did not agree with 'Behind The Name' as negative examples
    """
    # filtered_df -> positive (by us, sound2vec) , methods_df -> negative (synonym 0 by behind the name)
    # Original -> Name appeared on our df and on competitors' negatives
    # Candidate_x -> competitors' incorrect suggestion for name in 'Original' (synonym 0)
    # Candidate_y -> our correct suggestion for name in 'Original'

    if not competitor_dataset:  # the 2 datasets
        # Suggestion of the competitors
        dm_df = pd.read_csv(long_file_path + 'top_ten_suggestions_for_gt_by_Double_Metaphone_with_gt.csv')
        mrc_df = pd.read_csv(long_file_path + 'top_ten_suggestions_for_gt_by_Matching_Rating_Codex_with_gt.csv')
        metaphone_df = pd.read_csv(long_file_path + 'top_ten_suggestions_for_gt_by_Metaphone_with_gt.csv')
        nysiis_df = pd.read_csv(long_file_path + 'top_ten_suggestions_for_gt_by_Nysiis_with_gt.csv')
        soundex_df = pd.read_csv(long_file_path + 'top_ten_suggestions_for_gt_by_Soundex_with_gt.csv')
        # Suggestion of the competitors version 2
        dm_v2_df = pd.read_csv(long_file_path + 'v2_top_ten_suggestions_for_gt_by_Double_Metaphone_with_gt.csv')
        mrc_v2_df = pd.read_csv(long_file_path + 'v2_top_ten_suggestions_for_gt_by_Matching_Rating_Codex_with_gt.csv')
        metaphone_v2_df = pd.read_csv(long_file_path + 'v2_top_ten_suggestions_for_gt_by_Metaphone_with_gt.csv')
        nysiis_v2_df = pd.read_csv(long_file_path + 'v2_top_ten_suggestions_for_gt_by_Nysiis_with_gt.csv')
        soundex_v2_df = pd.read_csv(long_file_path + 'v2_top_ten_suggestions_for_gt_by_Soundex_with_gt.csv')
        # Adding the name of the method as a column
        dm_v2_df['Method'] = 'Double Metaphone'
        mrc_v2_df['Method'] = 'Matching Rating Codex'
        metaphone_v2_df['Method'] = 'Metaphone'
        nysiis_v2_df['Method'] = 'Nysiis'
        soundex_v2_df['Method'] = 'Soundex'
    else:  # use the new competitor dataset
        # Suggestion of the competitors
        # path = "/content/drive/MyDrive/Siamese_Datasets/Competitor_Results/"
        path = './Siamese_Datasets/Competitor_Results/'
        dm_df = pd.read_csv(path + 'Double_Metaphone/Double_Metaphone_names.csv')
        mrc_df = pd.read_csv(path + 'Matching_Rating_Codex/Matching_Rating_Codex_names.csv')
        metaphone_df = pd.read_csv(path + 'Metaphone/Metaphone_names.csv')
        nysiis_df = pd.read_csv(path + 'Nysiis/Nysiis_names.csv')
        soundex_df = pd.read_csv(path + 'Soundex/Soundex_names.csv')

    # Adding the name of the method as a column
    dm_df['Method'] = 'Double Metaphone'
    mrc_df['Method'] = 'Matching Rating Codex'
    metaphone_df['Method'] = 'Metaphone'
    nysiis_df['Method'] = 'Nysiis'
    soundex_df['Method'] = 'Soundex'
    if competitor_dataset:
        # Concat the datasets
        dfs = [dm_df, mrc_df, metaphone_df, nysiis_df, soundex_df]
    else:
        # Concat the datasets
        dfs = [dm_df, mrc_df, metaphone_df, nysiis_df, soundex_df, dm_v2_df, mrc_v2_df,
               metaphone_v2_df, nysiis_v2_df, soundex_v2_df]
    methods_df = pd.concat(dfs)
    # Find the ones the algorithms got wrong
    # FOR HARD NEGATIVE EXAMPLES OF COMPETITORS
    methods_df = methods_df[methods_df['Is_Original_Synonym'] == 0]
    return methods_df


"""We take the mistakes of the competitors and use the as negative samples"""


def create_train_df(methods_df, filtered_df):
    """
    The method merges the positive and negative dfs and creates the train df with the candidates
    """
    originals_df = pd.merge(methods_df, filtered_df, how="inner", left_on='Original', right_on='Original')
    train_df = originals_df[['Original', 'Candidate_y', 'Candidate_x']]
    train_df = train_df.rename(columns={'Candidate_y': 'Positive',
                                        'Candidate_x': 'Negative'})
    return train_df


# TODO: CHECK IF NEEDED
def helper2():
    # ADDING MIXED NEGATIVE EXAMPLES.. (not complicated negatives)

    # Samples From ground truth we created from turicreate dataset
    ground_truth_df = pd.read_csv("./spokenName2Vec_ground_truth.csv")
    # For each name we mark the letter it starts with
    ground_truth_df["Start"] = ground_truth_df["Original"].apply(
        lambda x: x[0])
    originals = ground_truth_df['Original']
    negatives = []
    # Creating random negatives
    for i in tqdm(range(len(originals))):
        negatives += [siamese_network_bgrams_updated.negative(originals, i)]

    # Updating the ground truth
    ground_truth_df['Negative'] = negatives
    # ground_truth_df
    ground_truth_df.to_csv('spokenName2Vec_ground_truth.csv', index=False)
    # df = ground_truth_df[ground_truth_df["Start"] == "A"]


# TODO: CHECK IF NEEDED
def helper():
    helper2()
    #ADDING MIXED NEGATIVE EXAMPLES.. (not complicated negatives)
    with open('./spokenName2Vec_ground_truth.csv', newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
    triplets = []
    for row in tqdm(data):
        triplets = triplets + [[row[2], row[3], row[8]]]
    triplets = triplets[1:]
    random.shuffle(triplets)
    return triplets


def train_test_split(train_df):
    # Split to train data and test data
    ### ADD EXTRA RANDOM NEGATIVES (adding the triplets..)
    triplets = helper()
    originals = train_df['Original']
    pos = list(train_df['Positive'])
    for row in tqdm(train_df.itertuples()):
        triplets = triplets + [[row[1], row[2], row[3]]]
    triplets = triplets[1:]

    l = int(0.66 * len(triplets))
    train_data = triplets[:l]
    test_data = triplets[l:]
    # test_data
    ### WITHOUT THE EXTRA RANDOM NEGATIVES USE THIS BELOW
    # l = int(0.66 * len(train_df))
    # train_data = train_df[:l]
    # test_data = train_df[l:]
    # test_data
    return train_data, test_data


# TODO: CHECK IF NEEDED
def add_random_negatives(train_df, train_data):
    ## (used to add more random negatives) -> random negative for every unique positive
    # Samples from competitors
    """
    The method adds random 'easy' negative examples to the train data
    """
    train_array = train_df['Positive'].unique()
    competitors = []
    for pos in tqdm(train_array):
        pos_df = train_df[train_df['Positive'] == pos]
        org = random.choice(pos_df['Original'].to_list())
        postv = random.choice(pos_df['Positive'].to_list())
        negtv = random.choice(pos_df['Negative'].to_list())
        competitors += [[org, postv, negtv]]
    train_data = train_data + competitors
    return train_data
import pandas as pd
import numpy as np

path = "./Siamese_Datasets/Competitor_Results/"
data_types = ["Double_Metaphone", "Metaphone", "Matching_Rating_Codex", "Nysiis"]
df_vecs = pd.read_csv(path + "/" + "name_sound_features.csv")


def create_files():
    """
    Create the ground truth files from the competitor results
    """
    for i in range(len(data_types)):
        df = pd.read_csv(path + data_types[i] +"/"+data_types[i] + "_names.csv")
        df = df[df['Is_Original_Synonym'] == 0]
        df = df[['Original','Candidate']]
        df['Candidate']= np.random.permutation(df['Candidate'].values)
        df.to_csv(path + data_types[i] +"/"+data_types[i]+"_gt.csv")


def calculate_distances(vec1,vec2):
    a = np.array(vec1)
    b = np.array(vec2)
    # calc the distance
    dist = np.linalg.norm(a - b)
    return dist


def find_name():
    """
    The function calculates the distance between each pair of names from the competitor dataset and ground truth
    """
    for i in range(len(data_types)):
        # read csv names
        df_names = pd.read_csv(path + data_types[i] + "/" + data_types[i] + "_gt.csv")

        # read csv feature vectors and calculate distances
        df_names['Distance'] = df_names.apply(helper_func,1)
        print("DONE:  "+str(data_types[i]))
        df_names.to_csv(path + data_types[i] + "/" + data_types[i] + "_distances.csv")
        print("CREATED")
    df_names = pd.read_csv("./Siamese_Datasets/ground_truth_constructed_based_on_all_first_names_behindthename_filtered_wt_V2.csv")

    df_names['Distance'] = df_names.apply(helper_func,1)
    print("DONE:  last")
    df_names.to_csv("./Siamese_Datasets/ground_truth_constructed_based_on_all_first_names_behindthename_filtered_wt_V2.csv")
    print("CREATED last")


def helper_func(row):
    """
    The function extracts the given row's corresponding vector for the name and the candidate
    and calculates the distance between them
    """
    name_vec = df_vecs[df_vecs['name']==row['Original']].iloc[:,:138]
    candidate_vec = df_vecs[df_vecs['name']==row['Candidate']].iloc[:,:138]
    if name_vec.empty or candidate_vec.empty:
        return -1
    distance = calculate_distances(name_vec,candidate_vec)
    print(str(row['Original'])+"  dist:  "+str(distance))
    return distance


# apply the main function for the distribution experiment
find_name()
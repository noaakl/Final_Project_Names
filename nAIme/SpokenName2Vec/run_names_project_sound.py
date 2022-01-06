from pydub import AudioSegment
import os
import turicreate as tc
from tqdm import tqdm
import pandas as pd
import editdistance
from sklearn.metrics import precision_score, accuracy_score, f1_score
from name2mp3 import convert_name_to_mp3
from nAIme.SpokenName2Vec.splitDir import split_dir

__author__ = "Aviad Elyashar"


def calculate_edit_distance(name1, name2):
    if not name1 or not name2:
        return -1

    name1 = name1.lower()
    name2 = name2.lower()

    edit_dist = editdistance.eval(name1, name2)
    return edit_dist


def compare_suggestion(original_name, candidate, ground_truth_df):
    result_df = ground_truth_df[(ground_truth_df["Name"] == original_name) & (ground_truth_df["Synonym"] == candidate)]
    print("original_name:{0}, candidate:{1}".format(original_name, candidate))
    if result_df.empty:
        return 0
    return 1


def convert_mp3s_to_wavs(out_path, name=None):
    print("convert_mp3s_to_wavs - Start")
    mp3_files_path = './mp3s/'
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    if name is not None:
        mp3_name_path = name + ".mp3"
        mp3s = [mp3_name_path]
    else:
        mp3s = os.listdir(mp3_files_path)
        mp3s = [mp3 for mp3 in mp3s if "mp3" in mp3]
        mp3s = sorted(mp3s)

    for i, mp3 in tqdm(enumerate(mp3s)):
        print("Name:{0} {1}/{2}".format(mp3, i, len(mp3s)))
        file_name_parts = mp3.split(".")
        name = file_name_parts[0]
        name_path = mp3_files_path + mp3

        sound = AudioSegment.from_mp3(name_path)
        sound.export("{0}/{1}.wav".format(out_path, name), format="wav")

    print("convert_mp3s_to_wavs - Done")


def create_sound_features_for_wavs(wav_file_input_path, out_path=None, number=0):
    print("create_sound_features_for_wavs")
    # load audio files - should be wav
    data = tc.load_audio(wav_file_input_path)
    # extract the names
    data['name'] = data['path'].apply(lambda p: p.split("/")[-1].replace('.wav', ''))
    # extract sound features
    try:
        data['deep_features'] = tc.sound_classifier.get_deep_features(data['audio'])
        # remove the names with empty features.
        data['deep_array'] = data['deep_features'].apply(lambda l: list(l[0]) if len(l) > 0 else None)
        data = data.dropna()
        data = data.select_columns(['name', 'deep_array'])
        if out_path is not None:
            if not os.path.exists(out_path + '/sound_features'):
                os.makedirs(out_path + '/sound_features')
            data.export_csv(out_path + '/sound_features/name_sound_features_{0}.csv'.format(number))
        print("create_sound_features_for_wavs - Done")
        return data
    except:
        return None


def split_dict(row):
    print()
    data1 = tc.SFrame(row['audio']['data'])
    data1.export_csv("./mis3.csv")


def create_knn_classifier(data_path, out_path=None, data_to_test=None):
    print("create_knn_classifier - Start")
    data = tc.SFrame.read_csv(data_path)
    if data_to_test is None:
        data_to_test = data
        data.export_csv('./RelevantFiles/name_sound_features.csv')
        data_names = data.select_columns(['name'])
        data_names.export_csv('./RelevantFiles/name_sound_features_only_names.csv')
    model = tc.nearest_neighbors.create(data, features=['deep_array'])
    # calculate KNN for all names
    knn = model.query(data, k=11)
    # remove yourself
    sf = knn[knn['query_label'] != knn['reference_label']]
    if out_path is not None:
        sf.export_csv(out_path + '/knn_results_with_indexes.csv')
    print("create_knn_classifier - Done")
    return sf.to_dataframe()


def extract_sound_features_and_use_knn_to_predict_for_suggestion():
    # create data features for name
    print("extract_sound_features_and_use_knn_to_predict - Start")
    wav_file_input_path = './wavs_query/'
    sound_features = create_sound_features_for_wavs(wav_file_input_path)
    sf = None
    if sound_features is not None:
        sf = create_knn_classifier('RelevantFiles/name_sound_features.csv', data_to_test=sound_features)
    return sf


def extract_sound_features_and_use_knn_to_predict(out_path):
    print("extract_sound_features_and_use_knn_to_predict - Start")
    # create full data features with batches
    count = len([f for f in os.listdir('') if f.startswith('wavs') and os.path.isdir(os.path.join('', f))]) - 1
    if count > 0:
        for number in range(0, count):
            print("wavs {0}".format(number))
            wav_file_input_path = './wavs{0}/'.format(number)
            create_sound_features_for_wavs(wav_file_input_path, out_path, number)
        create_knn_classifier(out_path+'/sound_features/', out_path=out_path)
    print("extract_sound_features_and_use_knn_to_predict - Done")


def convert_knn_suggestion_indexes_to_names(out_path=None, knn_results_with_indexes_df=None, name=None):
    print("convert_knn_suggestion_indexes_to_names - Start")
    if knn_results_with_indexes_df is None:
        knn_results_with_indexes_df = pd.read_csv(out_path + "/knn_results_with_indexes.csv")
    name_sound_features_df = pd.read_csv("./RelevantFiles/name_sound_features_only_names.csv")
    results = []

    for index, row in knn_results_with_indexes_df.iterrows():
        if name is not None:
            original_name = name
        else:
            original_name_index = row[0]
            original_name = name_sound_features_df.loc[original_name_index]["name"]

        candidate_name_index = row[1]
        distance = row[2]
        rank = row[3]

        candidate_name = name_sound_features_df.loc[candidate_name_index]["name"]

        result = (original_name, candidate_name, distance, rank)

        print("Convering from indexes to names for: {0} {1} {2}/{3}".format(original_name, candidate_name, index,
                                                                            knn_results_with_indexes_df.shape[0]))
        results.append(result)

    results_df = pd.DataFrame(results, columns=['Original', 'Candidate', 'Distance', 'Rank'])
    results_df = results_df.sort_values(by=['Original', 'Rank'], ascending=True)

    if out_path is not None:
        results_df.to_csv(out_path + "/knn_suggestions_according_sound_pandas_imp.csv", index=False)
    print("convert_knn_suggestion_indexes_to_names - Done")
    return results_df


def remove_names_not_in_ground_truth(results_df=None):
    print('remove_names_not_in_ground_truth - Start')
    if results_df is None:
        results_df = pd.read_csv("./results/knn_suggestions_according_sound_pandas_imp.csv")
    # for first names
    ground_truth_df = pd.read_csv("./RelevantFiles/ground_truth_constructed_based_on_all_first_names_behindthename.csv")

    original_names_series = ground_truth_df["Name"]
    original_names = original_names_series.tolist()

    original_names_set = set(original_names)
    sorted_results = []
    for index, row in results_df.iterrows():
        original_name = row[0]
        print("Name: {0} {1}/{2}".format(original_name, index, results_df.shape[0]))
        if original_name in original_names_set:
            sorted_results.append(row)

    sorted_results_df = pd.DataFrame(sorted_results, columns=['Original', 'Candidate', 'Distance', 'Rank'])
    sorted_results_df = sorted_results_df.sort_values(by=['Original', 'Rank'], ascending=True)

    sorted_results_df.to_csv("./results/knn_suggestions_according_sound_only_names_in_ground_truth.csv", index=False)
    print("remove_names_not_in_ground_truth - Done")
    return sorted_results_df


def sort_results_by_edit_distance(knn_suggestions_df=None, out_path=None):
    print("sort_results_by_edit_distance - Start")
    if knn_suggestions_df is None:
        knn_suggestions_sf = tc.SFrame.read_csv(out_path+'/knn_suggestions_according_sound_pandas_imp.csv')
    else:
        knn_suggestions_sf = tc.SFrame(data=knn_suggestions_df)
    knn_suggestions_sf['Edit_Distance'] = knn_suggestions_sf.apply(
        lambda x: calculate_edit_distance(x["Original"], x["Candidate"]))
    knn_suggestions_sf_by_edit_distance_sf = knn_suggestions_sf.sort(['Original', 'Edit_Distance'], ascending=True)
    if out_path is not None:
        knn_suggestions_sf_by_edit_distance_sf.export_csv(out_path+"/knn_suggestions_according_sound"
                                                                   "_only_names_in_ground_truth_ranked_by_ED.csv")
    print("sort_results_by_edit_distance - Done")
    return knn_suggestions_sf_by_edit_distance_sf.to_dataframe()


def remove_suggestions_by_threshold(knn_suggestions_ranked_by_ED_df=None, out_path=None):
    print("remove_suggestions_by_threshold - Start")
    if knn_suggestions_ranked_by_ED_df is None:
        knn_suggestions_ranked_by_ED_df = pd.read_csv(out_path+"/knn_suggestions_according_sound_only_names"
                                                               "_in_ground_truth_ranked_by_ED.csv")

    threshold = 1
    results = []
    for index, row in knn_suggestions_ranked_by_ED_df.iterrows():
        original_name = row[0]
        candidate_name = row[1]
        distance = row[2]
        print("Name: {0} Candidate:{1} {2}/{3}".format(original_name, candidate_name, index,
                                                       knn_suggestions_ranked_by_ED_df.shape[0]))

        # in case  the features were extracted using TuriCreate
        if distance < threshold:
            results.append(row)


    sorted_results_df = pd.DataFrame(results, columns=['Original', 'Candidate', 'Distance', 'Rank', "Edit_Distance"])
    sorted_results_df = sorted_results_df.sort_values(by=['Original', 'Distance', 'Edit_Distance'], ascending=True)

    if out_path is not None:
        sorted_results_df.to_csv("./RelevantFiles/knn_suggestions_according_sound_only_names_in_ground_truth_threshold_lower_than_1.csv", index=False)
    print("remove_suggestions_by_threshold - Done")
    return sorted_results_df


def compare_suggestions_with_ground_truth():
    print("compare_suggestions_with_ground_truth - Start")
    suggestions_df = pd.read_csv(
        './results/knn_suggestions_according_sound_only_names_in_ground_truth_threshold_lower_than_1.csv')
    # in case first names
    ground_truth_df = pd.read_csv('./RelevantFiles/ground_truth_constructed_based_on_all_first_names_behindthename.csv')
    suggestions_with_ground_truth_df = compare_suggestions_with_ground_truth_by_provided_dfs(suggestions_df,
                                                                                             ground_truth_df)
    print("compare_suggestions_with_ground_truth - Done")


def compare_suggestions_with_ground_truth_by_provided_dfs(suggestions_df, ground_truth_df):
    suggestions_df['Is_Original_Synonym'] = suggestions_df.apply(
        lambda x: compare_suggestion(x["Original"], x["Candidate"], ground_truth_df), axis=1)
    suggestions_df.to_csv(
        './results/knn_suggestions_according_sound_only_names_in_ground_truth_threshold_lower_than_1_with_ground_truth.csv',
        index=False)
    return suggestions_df


def calculate_performance_for_suggestions():
    print("calculate_performance_for_suggestions...")
    suggestions_df = pd.read_csv(
        './results/knn_suggestions_according_sound_only_names_in_ground_truth_threshold_lower_than_1_with_ground_truth.csv')

    # in case first names
    ground_truth_df = pd.read_csv('./RelevantFiles/ground_truth_constructed_based_on_all_first_names_behindthename.csv')
    calculate_performance(suggestions_df, ground_truth_df)


def calculate_performance(suggestions_df, ground_truth_df):
    print("calculate_performance")
    source_names_series = suggestions_df["Original"]
    # source_names_series = suggestions_df["Source_Name"]
    source_names = source_names_series.tolist()
    source_names = list(set(source_names))
    source_names = sorted(source_names)

    final_results = []
    for i, source_name in enumerate(source_names):
        print("First Name: {0} {1}/{2}".format(source_name, i, len(source_names)))
        source_name_results_df = suggestions_df[suggestions_df["Original"] == source_name]
        predictions = source_name_results_df["Is_Original_Synonym"]

        num_of_rows = source_name_results_df.shape[0]
        actual = [1] * num_of_rows

        accuracy = accuracy_score(actual, predictions)
        predictions_10 = predictions[0:10]
        actual_10 = actual[0:10]
        accuracy_10 = accuracy_score(actual_10, predictions_10)

        f1 = f1_score(actual, predictions)
        predictions_10 = predictions[0:10]
        actual_10 = actual[0:10]
        f1_10 = f1_score(actual_10, predictions_10)

        precison = precision_score(actual, predictions, average='micro')

        precison_1, precison_2, precison_3, precison_5, precision_10 = calculte_precision_at(actual, predictions)

        source_name_ground_truth_df = ground_truth_df[ground_truth_df["Name"] == source_name]
        source_name_num_of_relevant_synonyms = source_name_ground_truth_df.shape[0]

        num_of_relevant_retrieved_at_10 = predictions_10.sum()
        num_of_retrieved_at_10 = predictions_10.count()

        num_of_relevant_retrieved = predictions.sum()
        num_of_retrieved = predictions.count()

        recall_related_to_ground_truth = -1
        if source_name_num_of_relevant_synonyms > 0:
            recall_related_to_ground_truth = num_of_relevant_retrieved / float(source_name_num_of_relevant_synonyms)

            recall_1, recall_2, recall_3, recall_5, recall_10 = calculate_recall_at(predictions,
                                                                                    source_name_num_of_relevant_synonyms)

            # precision_related_to_ground_truth = num_of_relevant_retrieved / float(num_of_retrieved)

            # recall = recall_score(actual, predictions)

            result_tuple = (source_name, num_of_relevant_retrieved, num_of_retrieved, num_of_relevant_retrieved_at_10,
                            num_of_retrieved_at_10, source_name_num_of_relevant_synonyms,
                            accuracy, accuracy_10, f1, f1_10, precison_1, precison_2, precison_3, precison_5,
                            precision_10, precison, recall_1, recall_2, recall_3, recall_5, recall_10,
                            recall_related_to_ground_truth)
            final_results.append(result_tuple)

    final_results_df = pd.DataFrame(final_results,
                                    columns=['Source_Name', 'Num of Relevant Retrieved', 'Num of Retrieved',
                                             'Num of Relevant Retrieved@10', 'Num of Retrieved@10',
                                             'Total Num of Relevant in Ground Truth', 'Accuracy', 'Accuracy@10', 'F1',
                                             'F1@10', 'Precision@1', 'Precision@2', 'Precision@3', 'Precision@5',
                                             'Precision@10',
                                             'Precision', 'Recall@1', 'Recall@2', 'Recall@3', 'Recall@5', 'Recall@10',
                                             'Recall'])

    final_results_df.to_csv(
        './results/knn_suggestions_according_sound_only_names_in_ground_truth_threshold_lower_than_1_with_ground_truth_comparision_results_performance.csv',
        index=False)


def calculte_precision_at(actual, predictions):
    predictions_1 = predictions[0:1]
    actual_1 = actual[0:1]
    precison_1 = precision_score(actual_1, predictions_1, average='micro')

    predictions_2 = predictions[0:2]
    actual_2 = actual[0:2]
    precison_2 = precision_score(actual_2, predictions_2, average='micro')

    predictions_3 = predictions[0:3]
    actual_3 = actual[0:3]
    precison_3 = precision_score(actual_3, predictions_3, average='micro')

    predictions_5 = predictions[0:5]
    actual_5 = actual[0:5]
    precison_5 = precision_score(actual_5, predictions_5, average='micro')

    predictions_10 = predictions[0:10]
    actual_10 = actual[0:10]
    precison_10 = precision_score(actual_10, predictions_10, average='micro')

    return precison_1, precison_2, precison_3, precison_5, precison_10


def calculate_recall_at(predictions, source_name_num_of_relevant_synonyms):
    num_of_relevant_retrieved_1 = predictions[0:1].sum()
    recall_1 = num_of_relevant_retrieved_1 / float(source_name_num_of_relevant_synonyms)

    num_of_relevant_retrieved_2 = predictions[0:2].sum()
    recall_2 = num_of_relevant_retrieved_2 / float(source_name_num_of_relevant_synonyms)

    num_of_relevant_retrieved_3 = predictions[0:3].sum()
    recall_3 = num_of_relevant_retrieved_3 / float(source_name_num_of_relevant_synonyms)

    num_of_relevant_retrieved_5 = predictions[0:5].sum()
    recall_5 = num_of_relevant_retrieved_5 / float(source_name_num_of_relevant_synonyms)

    num_of_relevant_retrieved_10 = predictions[0:10].sum()
    recall_10 = num_of_relevant_retrieved_10 / float(source_name_num_of_relevant_synonyms)

    return recall_1, recall_2, recall_3, recall_5, recall_10


def top_suggestions(name, suggestion_df=None):
    print("top_suggestions - Start")
    if suggestion_df is None:
        suggestion_df = pd.read_csv(
            "RelevantFiles/knn_suggestions_according_sound_only_names_in_ground_truth_threshold_lower_than_1.csv")
    filtered_suggestion_df = suggestion_df[suggestion_df['Original'] == name]
    candidate_list = filtered_suggestion_df['Candidate'].to_list()
    return candidate_list


def get_suggestion(name):
    wavs_path = './wavs_query'
    name = name.capitalize()
    names_dataset = pd.read_csv("RelevantFiles/all_distinct_names_length_higher_than_2_characters.csv")
    filtered_names_dataset = names_dataset[names_dataset['Name'] == name]
    if filtered_names_dataset.empty:
        convert_name_to_mp3(name)
        convert_mp3s_to_wavs(wavs_path)
        sf = extract_sound_features_and_use_knn_to_predict_for_suggestion()
        if sf is None:
           return []
        knn_suggestion_with_names = convert_knn_suggestion_indexes_to_names(knn_results_with_indexes_df=sf, name=name)
        knn_suggestions_ranked_by_ed_df = sort_results_by_edit_distance(knn_suggestions_df=knn_suggestion_with_names)
        suggestion_df = remove_suggestions_by_threshold(knn_suggestions_ranked_by_ED_df=knn_suggestions_ranked_by_ed_df)
        return top_suggestions(name, suggestion_df)
    else:
        return top_suggestions(name)


def spoken_name_2_vec_full_process():
    out_path = './results'
    wavs_path = './wavs'
    convert_name_to_mp3()
    convert_mp3s_to_wavs(wavs_path)
    split_dir()
    extract_sound_features_and_use_knn_to_predict(out_path=out_path)
    convert_knn_suggestion_indexes_to_names(out_path=out_path)
    sort_results_by_edit_distance(out_path=out_path)
    remove_suggestions_by_threshold(out_path=out_path)
    print("done")


def main():
    print(get_suggestion("Aaberg"))
    print("Done!!")


if __name__ == "__main__":
    main()

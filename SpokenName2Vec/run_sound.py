import os
from pydub import AudioSegment
import pandas as pd
from pyAudioAnalysis import MidTermFeatures as mtf
from tqdm import tqdm
#from splitDir import split_dir
import turicreate as tc
import editdistance


__author__ = "Aviad Elyashar"


def calculate_edit_distance(name1, name2):
    if not name1 or not name2:
        return -1

    name1 = name1.lower()
    name2 = name2.lower()

    edit_dist = editdistance.eval(name1, name2)
    return edit_dist


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


def extract_audio_features(wav_path):
    print('extract_audio_features...')
    mid_term_features, wav_files, mid_feature_names = mtf.directory_feature_extraction(wav_path, 1, 1, 0.02, 0.02)
    names = [wav_file.split("/")[-1].replace(".wav", '') for wav_file in wav_files]
    if len(names) > 1:
        df = pd.DataFrame(data=mid_term_features)
    else:
        df = pd.DataFrame(data=[mid_term_features])
    df['name'] = names
    sf = tc.SFrame(data=df)
    return sf

def extract_sound_features_for_all(out_path):
    print("extract_sound_features_and_use_knn_to_predict - Start")

    if not os.path.exists("./{0}/sound_features/".format(out_path)):
        os.makedirs("./{0}/sound_features/".format(out_path))

    # create full data features with batches
    count = len([f for f in os.listdir('') if f.startswith('wavs') and os.path.isdir(os.path.join('', f))]) - 1
    if count > 0:
        for number in range(19, count):
            print("wavs {0}".format(number))
            wav_file_input_path = './wavs{0}/'.format(number)
            features = extract_audio_features(wav_file_input_path)
            features.export_csv("./{0}/sound_features/features{1}.csv".format(out_path, number))
        data = tc.SFrame.read_csv("./{0}/sound_features/".format(out_path))
        data.export_csv('./{0}/name_sound_features.csv'.format(out_path))
    print("extract_sound_features_for_all - Done")


def extract_sound_features_for_suggestion(name):
    # create data features for name
    print("extract_sound_features_and_use_knn_to_predict - Start")
    wav_file_input_path = './wavs_query/'
    features = extract_audio_features(wav_file_input_path)
    print("extract_sound_features_for_suggestion - Done")
    return features


def create_knn_classifier(data_to_test=None, name=None, out_path=None):
    print("create_knn_classifier - Start")
    with pkg_resources.path(RelevantFiles, "name_sound_features.csv") as p:
        package_path = p
    data = tc.SFrame.read_csv(str(package_path))
    #data = tc.SFrame.read_csv('./RelevantFiles/name_sound_features.csv')
    if data_to_test is None:
        data_to_test = data
    else:
        data_to_test = data_to_test.filter_by(name, 'name')
    features = list(range(0, 138))
    features = list(map(str, features))

    model = tc.nearest_neighbors.create(data, features=features)
    knn = model.query(data_to_test, k=11)
    sf = knn[knn['query_label'] != knn['reference_label']]
    if out_path is not None:
        sf.export_csv('./{0}/knn_results_with_indexes.csv'.format(out_path))
    print("create_knn_classifier - Done")
    return sf.to_dataframe()


def convert_knn_suggestion_indexes_to_names(out_path=None, knn_results_with_indexes_df=None, name=None):
    print("convert_knn_suggestion_indexes_to_names - Start")
    if knn_results_with_indexes_df is None:
        knn_results_with_indexes_df = pd.read_csv("./{0}/knn_results_with_indexes.csv".format(out_path))
    with pkg_resources.path(RelevantFiles, "name_sound_features.csv") as p:
        package_path = p
    name_sound_features_df = pd.read_csv(str(package_path))
    #name_sound_features_df = pd.read_csv("RelevantFiles/name_sound_features.csv")
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
        if original_name != candidate_name:
            results.append(result)

    results_df = pd.DataFrame(results, columns=['Original', 'Candidate', 'Distance', 'Rank'])
    results_df = results_df.sort_values(by=['Original', 'Rank'], ascending=True)

    if out_path is not None:
        results_df.to_csv("./{0}/knn_suggestions_according_sound_pandas_imp.csv".format(out_path), index=False)
    print("convert_knn_suggestion_indexes_to_names - Done")
    return results_df


def remove_suggestions_by_threshold(knn_suggestions_ranked_by_ED_df=None, out_path=None):
    print("remove_suggestions_by_threshold - Start")
    if knn_suggestions_ranked_by_ED_df is None:
        knn_suggestions_ranked_by_ED_df = pd.read_csv("./{0}/knn_suggestions_according_sound_only_names"
                                                               "_in_ground_truth_ranked_by_ED.csv".format(out_path))

    threshold = 0.3
    results = []
    for index, row in knn_suggestions_ranked_by_ED_df.iterrows():
        original_name = row[0]
        candidate_name = row[1]
        distance = row[2]
        print("Name: {0} Candidate:{1} {2}/{3}".format(original_name, candidate_name, index,
                                                       knn_suggestions_ranked_by_ED_df.shape[0]))

        # in case  the features were extracted using TuriCreate
        if distance <= threshold:
            results.append(row)


    sorted_results_df = pd.DataFrame(results, columns=['Original', 'Candidate', 'Distance', 'Rank', "Edit_Distance"])
    sorted_results_df = sorted_results_df.sort_values(by=['Original', 'Distance', 'Edit_Distance'], ascending=True)

    if out_path is not None:
        sorted_results_df.to_csv("./{0}/knn_suggestions_according_sound_only_"
                                 "names_in_ground_truth_threshold_lower_than_11.csv".format(out_path), index=False)
    print("remove_suggestions_by_threshold - Done")
    return sorted_results_df


def sort_results_by_edit_distance(knn_suggestions_df=None, out_path=None):
    print("sort_results_by_edit_distance - Start")
    if knn_suggestions_df is None:
        knn_suggestions_sf = tc.SFrame.read_csv('./{0}/knn_suggestions_according_sound_pandas_imp.csv'.format(out_path))
    else:
        knn_suggestions_sf = tc.SFrame(data=knn_suggestions_df)
    print(knn_suggestions_sf)
    knn_suggestions_sf['Edit_Distance'] = knn_suggestions_sf.apply(
        lambda x: calculate_edit_distance(x["Original"], x["Candidate"]))
    knn_suggestions_sf_by_edit_distance_sf = knn_suggestions_sf.sort(['Original', 'Edit_Distance'], ascending=True)
    if out_path is not None:
        knn_suggestions_sf_by_edit_distance_sf.export_csv("./{0}/knn_suggestions_according_sound"
                                                                   "_only_names_in_ground_truth_ranked_by_ED.csv".format(out_path))
    print("sort_results_by_edit_distance - Done")
    return knn_suggestions_sf_by_edit_distance_sf.to_dataframe()


def top_suggestions(name, suggestion_df=None):
    #print("top_suggestions - Start")
    if suggestion_df is None:
        with pkg_resources.path(RelevantFiles, "knn_suggestions_according_sound_only_names"
                                               "_in_ground_truth_threshold_lower_than_1.csv") as p:
            package_path = p
        suggestion_df = pd.read_csv(str(package_path))
        #suggestion_df = pd.read_csv(
         #   "RelevantFiles/knn_suggestions_according_sound_only_names_in_ground_truth_threshold_lower_than_1.csv")
    filtered_suggestion_df = suggestion_df[suggestion_df['Original'] == name]
    candidate_list = filtered_suggestion_df['Candidate'].to_list()
    return candidate_list


def get_suggestion(name):
    # with pkg_resources.path(RelevantFiles, "RelevantFiles.7z") as p:
    #     package_path = p
    # pth = Path(package_path)
    # pth_dir = pth.parent
    # pth = str(pth)
    # #Archive(package_path).extractall(pth)
    # all_files = os.listdir(pth_dir)
    # csv_files = list(filter(lambda f: f.endswith('.csv'), all_files))
    # if len(csv_files) == 0:
    #     shutil.register_unpack_format('7zip', ['.7z'], unpack_7zarchive)
    #     shutil.unpack_archive(pth, pth_dir)

    wavs_path = './wavs_query'
    name = name.capitalize()
    with pkg_resources.path(RelevantFiles, "all_distinct_names_length_higher_than_2_characters.csv") as p:
        package_path = p
    names_dataset = pd.read_csv(str(package_path))
    #names_dataset = pd.read_csv("RelevantFiles/all_distinct_names_length_higher_than_2_characters.csv")
    filtered_names_dataset = names_dataset[names_dataset['Name'] == name]
    if filtered_names_dataset.empty:
        convert_name_to_mp3(name)
        convert_mp3s_to_wavs(wavs_path, name)
        df = extract_sound_features_for_suggestion(name)
        knn = create_knn_classifier(data_to_test=df, name=name)
        knn_with_names = convert_knn_suggestion_indexes_to_names(knn_results_with_indexes_df=knn, name=name)
        print(knn_with_names)
        df_sorted = sort_results_by_edit_distance(knn_suggestions_df=knn_with_names)
        df = remove_suggestions_by_threshold(knn_suggestions_ranked_by_ED_df=df_sorted)
        return top_suggestions(name, df)
    else:
        return top_suggestions(name)


def full_process():
    out_path = 'results1'
    wavs_path = './wavs'
    convert_name_to_mp3()
    convert_mp3s_to_wavs(wavs_path)
    #split_dir()
    extract_sound_features_for_all(out_path=out_path)
    create_knn_classifier(out_path=out_path)
    convert_knn_suggestion_indexes_to_names(out_path=out_path)
    sort_results_by_edit_distance(out_path=out_path)
    remove_suggestions_by_threshold(out_path=out_path)
    print("done")


def main():
    print(get_suggestion("noa"))
    print(get_suggestion("noaa"))

    print("Done!!")




if __name__ == "__main__":
    main()

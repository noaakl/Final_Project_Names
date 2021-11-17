import os

import editdistance
import pandas as pd
from tqdm import tqdm


def is_nan(x):
    return (x != x)


def calculate_edit_distance(name1, name2):
    if not name1 or not name2:
        return -1

    name1 = name1.lower()
    name2 = name2.lower()

    edit_dist = editdistance.eval(name1, name2)
    return edit_dist


def create_suggestions(last_name, df, targeted_field_name): 
    if not df.empty:
        suggestions_df = df[[targeted_field_name]]
        suggestions_df["Original"] = last_name
        
        suggestions_df = suggestions_df.rename(columns={targeted_field_name: "Candidate"})

        suggestions_df = suggestions_df[["Original", "Candidate"]]

        suggestions_df['Edit_Distance'] = suggestions_df.apply(lambda x: calculate_edit_distance(x["Original"], x["Candidate"]), axis=1)
        suggestions_df_sorted_by_ED = suggestions_df.sort_values(['Original', 'Edit_Distance'], ascending=True)
        top_10_suggestions_df = suggestions_df_sorted_by_ED.head(10)
        return top_10_suggestions_df
    else:
        return pd.DataFrame()


def compare_suggestion(original_name, candidate, ground_truth_df):
    result_df = ground_truth_df[(ground_truth_df["Name"] == original_name)]
    if result_df.empty:
        return -1
    result_df = ground_truth_df[(ground_truth_df["Name"] == original_name) & (ground_truth_df["Synonym"] == candidate)]
    print("original_name:{0}, candidate:{1}".format(original_name, candidate))
    if result_df.empty:
        return 0
    return 1


def compare_suggestions_with_ground_truth_by_provided_dfs(suggestions_df, ground_truth_df):
    suggestions_df['Is_Original_Synonym'] = suggestions_df.apply(lambda x: compare_suggestion(x["Original"], x["Candidate"], ground_truth_df),axis=1)
    return suggestions_df


def create_top_10_suggestion_csv_file():
    ground_truth_df_1 = pd.read_csv('./RelevantFiles/ground_truth_constructed_based_on_all_first_names_behindthename.csv')
    output_path = './'
    target_field_name = "First Name"
    targeted_field_name = target_field_name.replace(" ", "_")
    name_phonetic_algorithm_df = pd.read_csv('./RelevantFiles/wt_First_Name_phonetic_algorithm_codes.csv')
    #phonetic_algorithms = ['Soundex', 'Metaphone', 'Double_Metaphone', 'Nysiis', 'Matching_Rating_Codex']
    phonetic_algorithms = ['Soundex', 'Metaphone']
    ground_truth_df = pd.read_csv('./RelevantFiles/all_distinct_First_Name_wikitree.csv')
    original_names = ground_truth_df["Child_First_Name"].unique().tolist()
    original_names = sorted(original_names)
    nums = {}
    for phonetic_algorithm in tqdm(phonetic_algorithms):
        num = 0
        count = 0
        dfs = []
        if not os.path.exists('./{0}'.format(phonetic_algorithm)):
            os.makedirs('./{0}'.format(phonetic_algorithm))
            os.makedirs('./{0}/{1}'.format(phonetic_algorithm, 'results'))
        for i, original_name in enumerate(original_names):
            print("Name: {0} {1}/{2}".format(original_name, i, len(original_names)))
            selected_name_phonetic_algorithms_df = name_phonetic_algorithm_df[name_phonetic_algorithm_df[targeted_field_name] == original_name]
            if selected_name_phonetic_algorithms_df.empty:
                continue
            res = selected_name_phonetic_algorithms_df[phonetic_algorithm].array[0]
            if is_nan(res) or res == '["",""]':
                continue
            index = selected_name_phonetic_algorithms_df.index

            if phonetic_algorithm == "Double_Metaphone":
                dmf_df = selected_name_phonetic_algorithms_df["Double_Metaphone_Primary"]
                if not dmf_df.empty:
                    dmp_score = dmf_df.tolist()[0]
                    selected_name_same_dmp_score_df = name_phonetic_algorithm_df[name_phonetic_algorithm_df["Double_Metaphone_Primary"] == dmp_score]
                    selected_name_same_dmp_score_df = selected_name_same_dmp_score_df.drop(index)
            
                dms_df = selected_name_phonetic_algorithms_df["Double_Metaphone_Secondary"]
                if not dms_df.empty:
                    dmp_score = dms_df.tolist()[0]
                    selected_name_same_dms_score_df = name_phonetic_algorithm_df[name_phonetic_algorithm_df["Double_Metaphone_Secondary"] == dmp_score]

                if index[0] in selected_name_same_dms_score_df.index.tolist():
                    selected_name_same_dms_score_df = selected_name_same_dms_score_df.drop(index)

                selected_name_same_measure_score_df = pd.concat([selected_name_same_dmp_score_df, selected_name_same_dms_score_df])

            else:
                df = selected_name_phonetic_algorithms_df[phonetic_algorithm]
                if not df.empty:
                    sound_measure_score = df.tolist()[0]

                selected_name_same_measure_score_df = name_phonetic_algorithm_df[name_phonetic_algorithm_df[phonetic_algorithm] == sound_measure_score]
                selected_name_same_measure_score_df = selected_name_same_measure_score_df.drop(index)

            top_10_synonyms_df = create_suggestions(original_name, selected_name_same_measure_score_df, targeted_field_name)
            if not top_10_synonyms_df.empty:
                dfs.append(top_10_synonyms_df)
            if count == 30:
                results_df = pd.concat(dfs)
                results_df.to_csv(output_path + "{0}/results/v2_top_ten_suggestions_for_gt_by_{0}_{1}.csv".format(
                    phonetic_algorithm, num), index=False)
                create_copmare_file_to_gt(phonetic_algorithm, num, ground_truth_df_1)
                num = num + 1
                count = 0
                dfs = []
            count = count + 1

        results_df = pd.concat(dfs)
        results_df.to_csv(output_path + "{0}/results/v2_top_ten_suggestions_for_gt_by_{0}_{1}.csv".format(phonetic_algorithm,num), index=False)
        create_copmare_file_to_gt(phonetic_algorithm, num, ground_truth_df_1)


def create_copmare_file_to_gt(phonetic_algorithm, num, ground_truth_df):
    output_path = "./"
    suggestions_by_measure_name_df = pd.read_csv(output_path + "{0}/results/v2_top_ten_suggestions_for_gt_by_{0}_{1}.csv".format(phonetic_algorithm, num))
    suggestions_with_gt_df = compare_suggestions_with_ground_truth_by_provided_dfs(suggestions_by_measure_name_df, ground_truth_df)
    suggestions_with_gt_df.to_csv(output_path + "{0}/results/top_ten_suggestions_for_gt_by_{0}_{1}_with_gt.csv".format(phonetic_algorithm, num), index=False)


def main():
     num = create_top_10_suggestion_csv_file()
     print("Done!!")


if __name__ == "__main__":
    main()


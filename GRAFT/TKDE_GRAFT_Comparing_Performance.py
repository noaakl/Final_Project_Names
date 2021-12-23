#!/usr/bin/env python
# coding: utf-8

# # Performance 

# In[40]:


import turicreate as tc
import turicreate.aggregate as agg
import re
import editdistance
import time
import os
import networkx as nx
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import phonetics
from sklearn.metrics import precision_score, accuracy_score, recall_score, precision_recall_fscore_support, f1_score

# original_path = "/Users/noaakless/Desktop/final_project/Names_Students_Project/"
original_path = "/home/user/project_py_3/Family_Trees_TKDE/"


# In[41]:


def get_full_path_suggestions(target_field_name, parental_relation_type, min_chars_count, max_edit_distance,
                              min_occurance,
                              neighbors_count, ranking_function):
    targeted_field_name = target_field_name.replace(" ", "_")

    full_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_geq_{0}_chars_ED_1_{1}_geq_{4}_occur_{5}_{6}_neighb.csv".format(
        min_chars_count, max_edit_distance, targeted_field_name, parental_relation_type, min_occurance,
        ranking_function, neighbors_count)
    return full_path


def compare_suggestion(original_name, candidate, ground_truth_df):
    print("original_name: {0}, candidate: {1}".format(original_name, candidate))
    result_df = ground_truth_df[
        (ground_truth_df["Name"] == original_name) &
        (ground_truth_df["Synonym"] == candidate)]

    if result_df.empty:
        return 0
    return 1


def compare_suggestion_with_ground_truth_by_provided_dfs(suggestions_df, ground_truth_df, ranking_function,
                                                         full_path_suggestions_file_no_prefix):
    print('compare_suggestion_with_ground_truth_by_provided_dfs')
    suggestions_df['Is_Original_Synonym'] = suggestions_df.apply(
        lambda x: compare_suggestion(x["Original"], x["Candidate"], ground_truth_df), axis=1)

    suggestions_df.to_csv(full_path_suggestions_file_no_prefix + "_with_gt_V2.csv", index=False)
    return suggestions_df


def calculate_performance_for_suggestions():
    print("calculate_performance_for_suggestions...")
    for ranking_function in ranking_functions:
        suggestions_df = pd.read_csv(
            output_path + "{0}_{1}_suggest_with_gt_V2.csv".format(graph_type, ranking_function))

        # in case first names
        # ground_truth_df = pd.read_csv(original_path + '/results/ground_truth_constructed_based_on_all_first_names_behindthename.csv')

        # in case last names from Name2Vec
        # ground_truth_df = pd.read_csv(original_path + "/results/records25k_data_surnames.tsv", sep="\t")
        calculate_performance(suggestions_df, ground_truth_df, ranking_function)


def calculate_performance(suggestions_df, ground_truth_df, ranking_function, full_path_suggestions_file_no_prefix):
    print("calculate_performance")
    source_names_series = suggestions_df["Original"]
    # source_names_series = suggestions_df["Source_Name"]
    source_names = source_names_series.tolist()
    source_names = list(set(source_names))
    source_names = sorted(source_names)

    final_results = []
    for i, source_name in enumerate(source_names):
        print(
            "Ranking Function: {0} First Name: {1} {2}/{3}".format(ranking_function, source_name, i, len(source_names)))
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

    average_results = []
    final_results_columns = final_results_df.columns
    average_performance_tuple = ("AVERAGE",)
    for column in final_results_columns:

        if column == "Source_Name":
            continue
        average_score = final_results_df[column].mean()
        average_performance_tuple = average_performance_tuple + (average_score,)

    # print("len of average_performance_tuple is:{0}".format(len(average_performance_tuple)))
    # print("len of final_results_columns is:{0}".format(len(final_results_columns)))

    df = pd.DataFrame([average_performance_tuple], columns=final_results_columns)

    df.reset_index(drop=True, inplace=True)
    final_results_df.reset_index(drop=True, inplace=True)

    final_results_with_performance_df = pd.concat([final_results_df, df])

    final_results_df.to_csv(full_path_suggestions_file_no_prefix + "_with_gt_perf_res_V2.csv", index=False)

    final_results_with_performance_df.to_csv(full_path_suggestions_file_no_prefix + "_with_gt_perf_res_V2_avg.csv",
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


target_field_names = ["First Name"]
# target_field_names = ["FN_LN"]
# output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/V2/First_Names/"
# output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/V2/First_and_Last_Names/"
# output_path = original_path + 'Family_Trees_TKDE/V2/First_Names2/'
output_path = original_path + 'Family_Trees_TKDE/V2/First_Names/'

ground_truth_df = pd.read_csv(
    original_path + '/Family_Trees_TKDE/results/ground_truth_constructed_based_on_all_first_names_behindthename.csv')

original_names = ground_truth_df["Name"].unique().tolist()
original_names = sorted(original_names)

# if not os.path.exists(output_path):
#     os.makedirs(output_path)


# parental_relation_types = ['Child_Father', 'Child_Grandfather', 'Child_GreatGrandfather', 'Child_All_Ancestors']
parental_relation_types = ['Child_Father']
# parental_relation_types = ['Child_Grandfather']
# parental_relation_types = ['Child_All_Ancestors']
# max_edit_distances = [3]
max_edit_distances = [2, 3, 4, 5, 100]
# min_chars_counts = [2]
min_chars_counts = [2, 3]
# min_occurances = [5]
# min_occurances = [10]
min_occurances = [5, 10]
neighbors_counts = [2]
ranking_functions = ['ED_and_order',
                     'order_2_and_ED',
                     'min_ED_of_DM',
                     'ED_and_order_and_ED_of_DM']

results = []
for target_field_name in tqdm(target_field_names):
    for parental_relation_type in tqdm(parental_relation_types):
        for min_chars_count in min_chars_counts:
            for max_edit_distance in tqdm(max_edit_distances):
                for min_occurance in tqdm(min_occurances):
                    for neighbors_count in neighbors_counts:
                        for i, ranking_function in tqdm(enumerate(ranking_functions)):
                            full_path_suggestions_file = get_full_path_suggestions(target_field_name,
                                                                                   parental_relation_type,
                                                                                   min_chars_count, max_edit_distance,
                                                                                   min_occurance, neighbors_count,
                                                                                   ranking_function)

                            full_path_suggestions_file_no_prefix = full_path_suggestions_file.split(".csv")[0]

                            suggestions_df = pd.read_csv(full_path_suggestions_file)

                            suggestions_with_ground_truth_df = compare_suggestion_with_ground_truth_by_provided_dfs(
                                suggestions_df, ground_truth_df, ranking_function,
                                full_path_suggestions_file_no_prefix)

                            calculate_performance(suggestions_with_ground_truth_df, ground_truth_df, ranking_function,
                                                  full_path_suggestions_file_no_prefix)

print("Done!")

# In[ ]:





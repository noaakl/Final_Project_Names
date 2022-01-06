#!/usr/bin/env python
# coding: utf-8

import turicreate as tc
import editdistance
import time
import os
import networkx as nx
import pandas as pd
from tqdm import tqdm
import phonetics
from nAIme.GRAFT import RelevantFiles
import importlib_resources as pkg_resources
import zipfile
from pathlib import Path


def get_child_father_full_path(target_field_name, min_chars_count, max_edit_distance, min_occurance, output_path):
    targeted_field_name = target_field_name.replace(" ", "_")
    parental_relation_type = 'Child_Father'
    full_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_ancestors_geq_{4}_occur.csv".format(
        min_chars_count, max_edit_distance, targeted_field_name, parental_relation_type, min_occurance)
    return full_path


def get_child_gandfather_full_path(target_field_name, min_chars_count, max_edit_distance, min_occurance, output_path):
    targeted_field_name = target_field_name.replace(" ", "_")
    parental_relation_type = 'Child_Grandfather'
    full_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_ancestors_geq_{4}_occur.csv".format(
        min_chars_count, max_edit_distance, targeted_field_name, parental_relation_type, min_occurance)
    return full_path


def get_child_greatgandfather_full_path(target_field_name, min_chars_count, max_edit_distance, min_occurance,
                                        output_path):
    targeted_field_name = target_field_name.replace(" ", "_")
    parental_relation_type = 'Child_GreatGrandfather'
    full_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_ancestors_geq_{4}_occur.csv".format(
        min_chars_count, max_edit_distance, targeted_field_name, parental_relation_type, min_occurance)
    return full_path


def get_child_ancestors_path(target_field_name, parental_relation_type, min_chars_count, max_edit_distance,
                             min_occurance, output_path):
    new_path = output_path + parental_relation_type
    if not os.path.exists(new_path):
        os.makedirs(new_path)

    new_path = output_path + parental_relation_type + "/geq_{0}_chars/".format(min_chars_count)
    if not os.path.exists(new_path):
        os.makedirs(new_path)

    new_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/".format(min_chars_count,
                                                                                        max_edit_distance)
    # print(new_path)
    if not os.path.exists(new_path):
        os.makedirs(new_path)

    return new_path


def get_child_ancestors_results_file_name(target_field_name,
                                          parental_relation_type,
                                          min_chars_count,
                                          max_edit_distance,
                                          min_occurance):
    targeted_field_name = target_field_name.replace(" ", "_")

    full_path = "wt_{0}_{1}_stacked_no_prefix_ed_geq_{2}_chars_ED_1_{3}_child_ancestors_geq_{4}_occur.csv".format(
        targeted_field_name, parental_relation_type, min_chars_count, max_edit_distance, min_occurance)
    return full_path


def create_parental_relation_types_csv(target_field_names, min_chars_counts, max_edit_distances, min_occurances,
                                       output_path, parental_relation_types):
    for target_field_name in target_field_names:
        for min_chars_count in min_chars_counts:
            for max_edit_distance in max_edit_distances:
                for min_occurance in min_occurances:
                    child_father_full_path = get_child_father_full_path(target_field_name, min_chars_count,
                                                                        max_edit_distance, min_occurance, output_path)
                    with pkg_resources.path(RelevantFiles, "wt_First_Name_Child_Father_stacked_no_prefix_ed_geq_2_chars"
                                                           "_ED_1_2_child_ancestors_geq_10_occur.csv") as p:
                        package_path = p
                    child_father_edges_df = pd.read_csv(str(package_path))
                    # print(child_father_full_path)

                    child_grandfather_full_path = get_child_gandfather_full_path(target_field_name, min_chars_count,
                                                                                 max_edit_distance, min_occurance,
                                                                                 output_path)
                    with pkg_resources.path(RelevantFiles, "wt_First_Name_Child_GreatGrandfather_stacked_no_"
                                                           "prefix_ed_geq_2_chars_ED_1_2_child_ancestors_geq_10_occur.csv") as p:
                        package_path = p
                    child_grandfather_edges_df = pd.read_csv(str(package_path))  # TODO: #


                    child_greatgrandfather_full_path = get_child_greatgandfather_full_path(target_field_name,
                                                                                           min_chars_count,
                                                                                           max_edit_distance,
                                                                                           min_occurance, output_path)

                    with pkg_resources.path(RelevantFiles, "wt_First_Name_Child_GreatGrandfather_stacked_no_"
                                                           "prefix_ed_geq_2_chars_ED_1_2_child_ancestors_geq_10_occur.csv") as p:
                        package_path = p
                    child_greatgrandfather_edges_df = pd.read_csv(str(package_path))  # TODO: #
                    # print(child_greatgrandfather_full_path)

                    df = pd.concat(
                        [child_father_edges_df, child_grandfather_edges_df, child_greatgrandfather_edges_df])  # TODO: #
                    # df = pd.concat([child_father_edges_df])  # TODO: !#
                    updated_df = df.groupby(['Child_Name', 'Ancestor_Name', 'Edit_Distance'])['sum'].sum().reset_index()
                    updated_df = updated_df.sort_values('sum', ascending=False)

                    for parental_relation_type in parental_relation_types:
                        all_ancestors_output_path = get_child_ancestors_path(target_field_name, parental_relation_type,
                                                                             min_chars_count, max_edit_distance,
                                                                             min_occurance, output_path)
                        results_file_name = get_child_ancestors_results_file_name(target_field_name,
                                                                                  parental_relation_type,
                                                                                  min_chars_count,
                                                                                  max_edit_distance,
                                                                                  min_occurance)
                        updated_df.to_csv(all_ancestors_output_path + results_file_name, index=False)


def get_full_path(target_field_name, parental_relation_type, min_chars_count, max_edit_distance, min_occurance,
                  output_path):
    targeted_field_name = target_field_name.replace(" ", "_")

    full_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_ancestors_geq_{4}_occur.csv".format(
        min_chars_count, max_edit_distance, targeted_field_name, parental_relation_type, min_occurance)
    return full_path


def get_results_full_path(target_field_name,
                          parental_relation_type,
                          min_chars_count,
                          max_edit_distance,
                          min_occurance,
                          neighbors_count,
                          ranking_function,
                          output_path):
    targeted_field_name = target_field_name.replace(" ", "_")
    full_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_geq_{0}_chars_ED_1_{1}_geq_{4}_occur_{5}_{6}_neighb.csv".format(
        min_chars_count,
        max_edit_distance, targeted_field_name, parental_relation_type, min_occurance, ranking_function,
        neighbors_count)
    return full_path


def calculate_edit_distance(name1, name2):
    if not name1 or not name2:
        return -1
    name1 = name1.lower()
    name2 = name2.lower()
    edit_dist = editdistance.eval(name1, name2)
    return edit_dist


def calculate_shortest_path(original_name, candidate, graph):
    shortest_path = nx.shortest_path_length(graph, source=original_name, target=candidate)
    return shortest_path


def rank_candidate(edit_distance_result, order, shortest_path):
    rank = edit_distance_result * order * shortest_path
    return rank


def get_phonetics_double_metaphone(name):
    # if name is not None and name is not 'None' and name is not '':
    #     # name = unicode(name)
    result = phonetics.dmetaphone(name)
    return result[0], result[1]


def find_positive_min_value(value1, value2, value3, value4):
    array = [value1, value2, value3, value4]
    positive_values = [i for i in array if i >= 0]
    if len(positive_values) > 0:
        min_value = min(positive_values)
        return min_value
    else:
        return 100


def rank_candidate_ED_and_order(edit_distance_result, order):
    rank = edit_distance_result * order
    return rank


def ED_and_order_and_ED_of_DM(edit_distance_result, order, min_edit_distance_of_DM):
    rank = edit_distance_result * order * (min_edit_distance_of_DM + 1)
    return rank


class OrderingFunctions:
    def __init__(self):
        pass

    #
    # Order^2 *  Edit Distance
    #

    @staticmethod
    def order_2_and_ED(name_graph, original_name, neighbors_count):
        nodes = nx.single_source_shortest_path_length(name_graph, original_name, neighbors_count)
        if len(nodes) > 1:
            nodes = list(nodes.items())

            original_name_series = [original_name] * len(nodes)
            candidates_df = pd.DataFrame(nodes, columns=['Candidate', 'Order'])
            candidates_df['Original'] = original_name_series
            candidates_df = candidates_df[['Original', 'Candidate', 'Order']]

            candidates_df = candidates_df[candidates_df["Order"] != 0]

            candidates_df['Edit_Distance'] = candidates_df.apply(
                lambda x: calculate_edit_distance(x["Original"], x["Candidate"]), axis=1)
            candidates_df['Shortest_Path'] = candidates_df.apply(
                lambda x: calculate_shortest_path(x["Original"], x["Candidate"], name_graph), axis=1)
            candidates_df['Rank'] = candidates_df.apply(
                lambda x: rank_candidate(x["Edit_Distance"], x["Order"], x["Shortest_Path"]),
                axis=1)

            candidates_df = candidates_df.sort_values(by='Order')
            candidates_df = candidates_df.sort_values(by='Rank')
            head_candidates_df = candidates_df.head(10)
            return head_candidates_df
        # return candidates_df
        return None

    # Order * Edit Distance
    @staticmethod
    def ED_and_order(name_graph, original_name, neighbors_count):
        nodes = nx.single_source_shortest_path_length(name_graph, original_name, neighbors_count)
        if len(nodes) > 1:
            nodes = list(nodes.items())

            original_name_series = [original_name] * len(nodes)
            candidates_df = pd.DataFrame(nodes, columns=['Candidate', 'Order'])
            candidates_df['Original'] = original_name_series
            candidates_df = candidates_df[['Original', 'Candidate', 'Order']]

            candidates_df = candidates_df[candidates_df["Order"] != 0]

            candidates_df['Edit_Distance'] = candidates_df.apply(
                lambda x: calculate_edit_distance(x["Original"], x["Candidate"]), axis=1)
            candidates_df['Rank'] = candidates_df.apply(
                lambda x: rank_candidate_ED_and_order(x["Edit_Distance"], x["Order"]), axis=1)

            candidates_df = candidates_df.sort_values(by='Rank')
            head_candidates_df = candidates_df.head(10)
            return head_candidates_df
        return None
        # return candidates_df

        # Order * Edit Distance * ED (matahpone)

    @staticmethod
    def ED_and_order_and_ED_of_DM(name_graph, original_name, neighbors_count):
        nodes = nx.single_source_shortest_path_length(name_graph, original_name, neighbors_count)
        if len(nodes) > 1:
            nodes = list(nodes.items())

            original_name_series = [original_name] * len(nodes)
            candidates_df = pd.DataFrame(nodes, columns=['Candidate', 'Order'])
            candidates_df['Original'] = original_name_series
            candidates_df = candidates_df[['Original', 'Candidate', 'Order']]

            candidates_df = candidates_df[candidates_df["Order"] != 0]

            candidates_df['Edit_Distance'] = candidates_df.apply(
                lambda x: calculate_edit_distance(x["Original"], x["Candidate"]),
                axis=1)

            candidates_df['Double_Metaphone_Primary_Original_Name'], candidates_df[
                'Double_Metaphone_Secondary_Original_Name'] = zip(*candidates_df.apply(
                lambda x: get_phonetics_double_metaphone(x["Original"]),
                axis=1))
            candidates_df['Double_Metaphone_Primary_Candidate'], candidates_df[
                'Double_Metaphone_Secondary_Candidate'] = zip(*candidates_df.apply(
                lambda x: get_phonetics_double_metaphone(x["Candidate"]),
                axis=1))
            candidates_df["Edit_Distance_Primary_DM_Original_Candidate"] = candidates_df.apply(
                lambda x: calculate_edit_distance(x["Double_Metaphone_Primary_Original_Name"],
                                                  x["Double_Metaphone_Primary_Candidate"]),
                axis=1)
            candidates_df["Edit_Distance_Secondary_DM_Original_Candidate"] = candidates_df.apply(
                lambda x: calculate_edit_distance(x["Double_Metaphone_Secondary_Original_Name"],
                                                  x["Double_Metaphone_Secondary_Candidate"]),
                axis=1)

            candidates_df["Edit_Distance_Primary_DM_Original_Secondary_Candidate"] = candidates_df.apply(
                lambda x: calculate_edit_distance(x["Double_Metaphone_Primary_Original_Name"],
                                                  x["Double_Metaphone_Secondary_Candidate"]),
                axis=1)
            candidates_df["Edit_Distance_Secondary_DM_Original_Primary_Candidate"] = candidates_df.apply(
                lambda x: calculate_edit_distance(x["Double_Metaphone_Secondary_Original_Name"],
                                                  x["Double_Metaphone_Primary_Candidate"]),
                axis=1)
            # candidates_df.to_csv(self._output_directory_path + "Metaphone_Edit_distance_graph.csv")
            candidates_df["Min_Edit_Distance_of_DM"] = candidates_df.apply(
                lambda x: find_positive_min_value(x["Edit_Distance_Primary_DM_Original_Candidate"],
                                                  x["Edit_Distance_Secondary_DM_Original_Candidate"],
                                                  x["Edit_Distance_Primary_DM_Original_Secondary_Candidate"],
                                                  x["Edit_Distance_Secondary_DM_Original_Primary_Candidate"]),
                axis=1)

            candidates_df['Rank'] = candidates_df.apply(
                lambda x: ED_and_order_and_ED_of_DM(x["Edit_Distance"], x["Order"], x["Min_Edit_Distance_of_DM"]),
                axis=1)

            candidates_df = candidates_df.sort_values(by='Rank')
            head_candidates_df = candidates_df.head(10)
            return head_candidates_df
        return None

    #
    # Recieve the graph of father and son edit distance 1 until 3.
    # The ranking is according to double metaphone from the original name with edit distance.
    #

    @staticmethod
    def min_ED_of_DM(name_graph, original_name, neighbors_count):
        nodes = nx.single_source_shortest_path_length(name_graph, original_name, neighbors_count)

        if len(nodes) > 1:
            nodes = list(nodes.items())

            original_name_series = [original_name] * len(nodes)
            candidates_df = pd.DataFrame(nodes, columns=['Candidate', 'Order'])
            candidates_df['Original'] = original_name_series
            candidates_df = candidates_df[['Original', 'Candidate', 'Order']]

            candidates_df = candidates_df[candidates_df["Order"] != 0]
            candidates_df['Double_Metaphone_Primary_Original_Name'], candidates_df[
                'Double_Metaphone_Secondary_Original_Name'] = zip(*candidates_df.apply(
                lambda x: get_phonetics_double_metaphone(x["Original"]),
                axis=1))
            candidates_df['Double_Metaphone_Primary_Candidate'], candidates_df[
                'Double_Metaphone_Secondary_Candidate'] = zip(*candidates_df.apply(
                lambda x: get_phonetics_double_metaphone(x["Candidate"]),
                axis=1))
            candidates_df["Edit_Distance_Primary_DM_Original_Candidate"] = candidates_df.apply(
                lambda x: calculate_edit_distance(x["Double_Metaphone_Primary_Original_Name"],
                                                  x["Double_Metaphone_Primary_Candidate"]),
                axis=1)
            candidates_df["Edit_Distance_Secondary_DM_Original_Candidate"] = candidates_df.apply(
                lambda x: calculate_edit_distance(x["Double_Metaphone_Secondary_Original_Name"],
                                                  x["Double_Metaphone_Secondary_Candidate"]),
                axis=1)

            candidates_df["Edit_Distance_Primary_DM_Original_Secondary_Candidate"] = candidates_df.apply(
                lambda x: calculate_edit_distance(x["Double_Metaphone_Primary_Original_Name"],
                                                  x["Double_Metaphone_Secondary_Candidate"]),
                axis=1)
            candidates_df["Edit_Distance_Secondary_DM_Original_Primary_Candidate"] = candidates_df.apply(
                lambda x: calculate_edit_distance(x["Double_Metaphone_Secondary_Original_Name"],
                                                  x["Double_Metaphone_Primary_Candidate"]),
                axis=1)
            # candidates_df.to_csv(self._output_directory_path + "Metaphone_Edit_distance_graph.csv")
            candidates_df["Min_Edit_Distance_of_DM"] = candidates_df.apply(
                lambda x: find_positive_min_value(x["Edit_Distance_Primary_DM_Original_Candidate"],
                                                  x["Edit_Distance_Secondary_DM_Original_Candidate"],
                                                  x["Edit_Distance_Primary_DM_Original_Secondary_Candidate"],
                                                  x["Edit_Distance_Secondary_DM_Original_Primary_Candidate"]),
                axis=1)

            candidates_df["Rank"] = candidates_df["Min_Edit_Distance_of_DM"]
            # candidates_df = candidates_df.sort_values(by='Min_Edit_Distance_of_DM')
            candidates_df = candidates_df.sort_values(by='Rank')
            head_candidates_df = candidates_df.head(10)
            return head_candidates_df
        return None


def get_graph_info(g):
    node_count = g.number_of_nodes()
    edge_count = g.number_of_edges()
    avg_in_degree = sum(d for n, d in g.in_degree()) / float(node_count)
    avg_out_degree = sum(d for n, d in g.out_degree()) / float(node_count)
    return node_count, edge_count, avg_in_degree, avg_out_degree


def create_results_csv(target_field_names, parental_relation_types, min_chars_counts, max_edit_distances,
                       min_occurances, output_path, neighbors_counts, ranking_functions, original_names):
    name_graph = None
    results = []
    for target_field_name in target_field_names:
        for parental_relation_type in parental_relation_types:
            for min_chars_count in min_chars_counts:
                for max_edit_distance in max_edit_distances:
                    for min_occurance in min_occurances:

                        if parental_relation_types == 'Child_Father':
                            with pkg_resources.path(RelevantFiles, "wt_First_Name_Child_Father_stacked_no_prefix_ed_"
                                                                   "geq_2_chars_ED_1_2_child_ancestors_geq_10_occur.csv") as p:
                                package_path = p
                            edges_sf = tc.SFrame.read_csv(str(package_path))
                        else:
                            with pkg_resources.path(RelevantFiles, "wt_First_Name_Child_GreatGrandfather_"
                                                                   "stacked_no_prefix_ed_geq_2_chars_ED_1_2_child_ancestors_geq_10_occur.csv") as p1:
                                package_path = p1
                            edges_sf = tc.SFrame.read_csv(str(package_path))

                        start_time = time.time()

                        name_graph = nx.DiGraph()  # Creating Undirected Graph
                        # # adding all nodes and vertices at once
                        name_graph.add_weighted_edges_from(
                            [(r['Ancestor_Name'], r['Child_Name'], r['sum']) for r in edges_sf])

                        for neighbors_count in neighbors_counts:
                            for i, ranking_function in tqdm(enumerate(ranking_functions)):
                                dfs = []
                                for j, original_name in enumerate(original_names):
                                    if name_graph.has_node(original_name):
                                        candidates_df = getattr(OrderingFunctions, ranking_function)(name_graph,
                                                                                                     original_name,
                                                                                                     neighbors_count)
    return name_graph


# # Performance
def get_full_path_suggestions(target_field_name, parental_relation_type, min_chars_count, max_edit_distance,
                              min_occurance,
                              ranking_function, neighbors_count, output_path):
    targeted_field_name = target_field_name.replace(" ", "_")

    full_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_geq_{0}_chars_ED_1_{1}_geq_{4}_occur_{5}_{6}_neighb.csv".format(
        min_chars_count,
        max_edit_distance, targeted_field_name, parental_relation_type, min_occurance, ranking_function,
        neighbors_count)
    return full_path


def min_ED_of_DM2(name_graph, original_name):
    nodes = nx.single_source_shortest_path_length(name_graph, original_name, 3)
    # print(nodes)
    if len(nodes) > 1:
        nodes = list(nodes.items())

        original_name_series = [original_name] * len(nodes)
        candidates_df = pd.DataFrame(nodes, columns=['Candidate', 'Order'])
        candidates_df['Original'] = original_name_series
        candidates_df = candidates_df[['Original', 'Candidate', 'Order']]

        candidates_df = candidates_df[candidates_df["Order"] != 0]
        candidates_df['Double_Metaphone_Primary_Original_Name'], candidates_df[
            'Double_Metaphone_Secondary_Original_Name'] = zip(*candidates_df.apply(
            lambda x: get_phonetics_double_metaphone(x["Original"]),
            axis=1))
        candidates_df['Double_Metaphone_Primary_Candidate'], candidates_df[
            'Double_Metaphone_Secondary_Candidate'] = zip(*candidates_df.apply(
            lambda x: get_phonetics_double_metaphone(x["Candidate"]),
            axis=1))
        candidates_df["Edit_Distance_Primary_DM_Original_Candidate"] = candidates_df.apply(
            lambda x: calculate_edit_distance(x["Double_Metaphone_Primary_Original_Name"],
                                              x["Double_Metaphone_Primary_Candidate"]),
            axis=1)
        candidates_df["Edit_Distance_Secondary_DM_Original_Candidate"] = candidates_df.apply(
            lambda x: calculate_edit_distance(x["Double_Metaphone_Secondary_Original_Name"],
                                              x["Double_Metaphone_Secondary_Candidate"]),
            axis=1)

        candidates_df["Edit_Distance_Primary_DM_Original_Secondary_Candidate"] = candidates_df.apply(
            lambda x: calculate_edit_distance(x["Double_Metaphone_Primary_Original_Name"],
                                              x["Double_Metaphone_Secondary_Candidate"]),
            axis=1)
        candidates_df["Edit_Distance_Secondary_DM_Original_Primary_Candidate"] = candidates_df.apply(
            lambda x: calculate_edit_distance(x["Double_Metaphone_Secondary_Original_Name"],
                                              x["Double_Metaphone_Primary_Candidate"]),
            axis=1)

        # candidates_df.to_csv(self._output_directory_path + "Metaphone_Edit_distance_graph.csv")

        candidates_df["Min_Edit_Distance_of_DM"] = candidates_df.apply(
            lambda x: find_positive_min_value(x["Edit_Distance_Primary_DM_Original_Candidate"],
                                              x["Edit_Distance_Secondary_DM_Original_Candidate"],
                                              x["Edit_Distance_Primary_DM_Original_Secondary_Candidate"],
                                              x["Edit_Distance_Secondary_DM_Original_Primary_Candidate"]),
            axis=1)

        candidates_df["Rank"] = candidates_df["Min_Edit_Distance_of_DM"]
        # candidates_df = candidates_df.sort_values(by='Min_Edit_Distance_of_DM')
        candidates_df = candidates_df.sort_values(by='Order')
        candidates_df = candidates_df.sort_values(by='Rank')
        # head_candidates_df = candidates_df.head(10)
        return candidates_df
    return None


def get_suggestion(original_name):
    with pkg_resources.path(RelevantFiles, "RelevantFiles.zip") as p:
        package_path = p
    pth = Path(package_path)
    pth = pth.parent
    with zipfile.ZipFile(package_path, 'r') as zip_ref:
        zip_ref.extractall(pth)

    original_name = original_name.capitalize()
    target_field_names = ["First Name"]
    output_path = "../../Family_Trees_TKDE/Family_Trees_TKDE/V2/First_Names/"
    parental_relation_types = ['Child_Father', 'Child_Grandfather']
    max_edit_distances = [2]
    min_chars_counts = [2]
    min_occurances = [10]

    with pkg_resources.path(RelevantFiles, "ground_truth_constructed_based_on_all_first_names_behindthename.csv") as p:
        package_path = p
    ground_truth_df = pd.read_csv(str(package_path))

    original_names = ground_truth_df["Name"].unique().tolist()
    original_names = sorted(original_names)

    neighbors_counts = [2]
    ranking_functions = ['ED_and_order',
                         'order_2_and_ED',
                         'min_ED_of_DM',
                         'ED_and_order_and_ED_of_DM']
    name_graph = create_results_csv(target_field_names, parental_relation_types, min_chars_counts, max_edit_distances,
                                    min_occurances, output_path, neighbors_counts, ranking_functions, original_names)
    head_candidates_df = min_ED_of_DM2(name_graph, original_name)
    result = head_candidates_df.head(10)["Candidate"]
    result = result.tolist()
    return result


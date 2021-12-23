#!/usr/bin/env python
# coding: utf-8

# In[10]:


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


# # Create child-all ancestors graph before suggesting 

parental_relation_type = ''


def get_child_father_full_path(target_field_name, min_chars_count, max_edit_distance, min_occurance):
    targeted_field_name = target_field_name.replace(" ", "_")
    parental_relation_type = 'Child_Father'
    full_path = output_path + parental_relation_type  + "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_ancestors_geq_{4}_occur.csv".format(min_chars_count, max_edit_distance, targeted_field_name, parental_relation_type, min_occurance)
    return full_path


def get_child_gandfather_full_path(target_field_name, min_chars_count, max_edit_distance, min_occurance):
    targeted_field_name = target_field_name.replace(" ", "_")
    parental_relation_type = 'Child_Grandfather'
    full_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_ancestors_geq_{4}_occur.csv".format(min_chars_count, max_edit_distance, targeted_field_name, parental_relation_type, min_occurance)
    return full_path


def get_child_greatgandfather_full_path(target_field_name, min_chars_count, max_edit_distance, min_occurance):
    targeted_field_name = target_field_name.replace(" ", "_")
    parental_relation_type = 'Child_GreatGrandfather'
    full_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_ancestors_geq_{4}_occur.csv".format(min_chars_count, max_edit_distance, targeted_field_name, parental_relation_type, min_occurance)
    return full_path


# In[12]:

def get_child_ancestors_path(target_field_name, parental_relation_type, min_chars_count, max_edit_distance, min_occurance):
    
    new_path = output_path + parental_relation_type
    if not os.path.exists(new_path):
        os.makedirs(new_path)
        
    new_path = output_path + parental_relation_type + "/geq_{0}_chars/".format(min_chars_count)
    if not os.path.exists(new_path):
        os.makedirs(new_path)
    
    new_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/".format(min_chars_count, max_edit_distance)
    print(new_path)
    if not os.path.exists(new_path):
        os.makedirs(new_path)
        
    return new_path


# In[13]:


def get_child_ancestors_results_file_name(target_field_name, 
                                          parental_relation_type, 
                                          min_chars_count, 
                                          max_edit_distance, 
                                          min_occurance):
    targeted_field_name = target_field_name.replace(" ", "_")
    
    full_path = "wt_{0}_{1}_stacked_no_prefix_ed_geq_{2}_chars_ED_1_{3}_child_ancestors_geq_{4}_occur.csv".format(
        targeted_field_name, parental_relation_type, min_chars_count, max_edit_distance, min_occurance)
    return full_path


# In[16]:


target_field_names = ["First Name"]
# target_field_names = ["Last Name Current"]
#output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/V2/First_Names/"
# output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/V2/Last_Names/"
# output_path = "/home/user/project_py_3/Family_Trees_TKDE/Family_Trees_TKDE/V2/First_Names2/"
output_path = "/home/user/project_py_3/Family_Trees_TKDE/Family_Trees_TKDE/V2/First_Names/"

parental_relation_types = ['Child_Father', 'Child_Grandfather', 'Child_GreatGrandfather']
# parental_relation_types = ['Child_Father']
# parental_relation_type = 'Child_Father'
# parental_relation_type = "Child_All_Ancestors"
# max_edit_distances = [2, 3, 4, 5]
# max_edit_distances = [3]
max_edit_distances = [2, 3, 4, 5, 100]
# min_chars_counts = [2]
min_chars_counts = [2, 3]
# min_occurances = [10]
min_occurances = [5, 10]


for target_field_name in target_field_names:
    for min_chars_count in min_chars_counts:
        for max_edit_distance in max_edit_distances:                                                             
            for min_occurance in min_occurances:
                child_father_full_path = get_child_father_full_path(target_field_name, min_chars_count, max_edit_distance, min_occurance)
                child_father_edges_df = pd.read_csv(child_father_full_path)

                child_grandfather_full_path = get_child_gandfather_full_path(target_field_name, min_chars_count, max_edit_distance, min_occurance)
                # child_grandfather_edges_df = pd.read_csv(child_grandfather_full_path)

                child_greatgrandfather_full_path = get_child_greatgandfather_full_path(target_field_name, min_chars_count, max_edit_distance, min_occurance)
                # child_greatgrandfather_edges_df = pd.read_csv(child_greatgrandfather_full_path)

                # df = pd.concat([child_father_edges_df, child_grandfather_edges_df, child_greatgrandfather_edges_df])
                df = pd.concat([child_father_edges_df])
                updated_df = df.groupby(['Child_Name','Ancestor_Name', 'Edit_Distance'])['sum'].sum().reset_index()
                updated_df = updated_df.sort_values('sum', ascending=False)
                
                all_ancestors_output_path = get_child_ancestors_path(target_field_name, parental_relation_type, min_chars_count, max_edit_distance, min_occurance)
                results_file_name = get_child_ancestors_results_file_name(target_field_name, 
                                          parental_relation_type, 
                                          min_chars_count, 
                                          max_edit_distance, 
                                          min_occurance)
                updated_df.to_csv(all_ancestors_output_path + results_file_name, index=False)
                
print("Done!")

# In[20]:


def get_full_path(target_field_name, parental_relation_type, min_chars_count, max_edit_distance, min_occurance):
    targeted_field_name = target_field_name.replace(" ", "_")
    
    full_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_ancestors_geq_{4}_occur.csv".format(min_chars_count, max_edit_distance, targeted_field_name, parental_relation_type, min_occurance)
    return full_path


def get_results_full_path(target_field_name, 
                          parental_relation_type, 
                          min_chars_count, 
                          max_edit_distance, 
                          min_occurance,
                          neighbors_count, 
                          ranking_function):
    
    targeted_field_name = target_field_name.replace(" ", "_")
    
    #full_path_without_extension = full_path.split(".")[0]
    #updated_path =  full_path_without_extension + "_{0}_2_neighbors_suggestions.csv".format(ranking_function)
    full_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_geq_{0}_chars_ED_1_{1}_geq_{4}_occur_{5}_{6}_neighb.csv".format(min_chars_count, 
    max_edit_distance, targeted_field_name, parental_relation_type, min_occurance, ranking_function, neighbors_count)
    
    return full_path
    

# In[21]:


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


# In[22]:


class OrderingFunctions:
    def __init__(self):
      pass
    
    #
    # Order^2 *  Edit Distance
    #

    def order_2_and_ED(name_graph, original_name, neighbors_count):
        nodes = nx.single_source_shortest_path_length(name_graph, original_name, neighbors_count)
        if len(nodes) > 1:
            nodes = list(nodes.items())

            original_name_series = [original_name] * len(nodes)
            candidates_df = pd.DataFrame(nodes, columns=['Candidate', 'Order'])
            candidates_df['Original'] = original_name_series
            candidates_df = candidates_df[['Original', 'Candidate', 'Order']]

            candidates_df = candidates_df[candidates_df["Order"] != 0]

            candidates_df['Edit_Distance'] = candidates_df.apply(lambda x: calculate_edit_distance(x["Original"], x["Candidate"]), axis=1)
            candidates_df['Shortest_Path'] = candidates_df.apply(lambda x: calculate_shortest_path(x["Original"], x["Candidate"], name_graph), axis=1)
            candidates_df['Rank'] = candidates_df.apply(lambda x: rank_candidate(x["Edit_Distance"], x["Order"], x["Shortest_Path"]),
                                              axis=1)

            candidates_df = candidates_df.sort_values(by='Rank')
            head_candidates_df = candidates_df.head(10)
            return head_candidates_df
        #return candidates_df
        return None
        

   # Order * Edit Distance
    def ED_and_order(name_graph, original_name, neighbors_count):
        nodes = nx.single_source_shortest_path_length(name_graph, original_name, neighbors_count)
        if len(nodes) > 1:
            nodes = list(nodes.items())

            original_name_series = [original_name] * len(nodes)
            candidates_df = pd.DataFrame(nodes, columns=['Candidate', 'Order'])
            candidates_df['Original'] = original_name_series
            candidates_df = candidates_df[['Original', 'Candidate', 'Order']]

            candidates_df = candidates_df[candidates_df["Order"] != 0]

            candidates_df['Edit_Distance'] = candidates_df.apply(lambda x: calculate_edit_distance(x["Original"], x["Candidate"]),axis=1)
            candidates_df['Rank'] = candidates_df.apply(lambda x: rank_candidate_ED_and_order(x["Edit_Distance"], x["Order"]), axis=1)

            candidates_df = candidates_df.sort_values(by='Rank')
            head_candidates_df = candidates_df.head(10)
            return head_candidates_df
        return None
        #return candidates_df


        # Order * Edit Distance * ED (matahpone)
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

            candidates_df['Double_Metaphone_Primary_Original_Name'],             candidates_df['Double_Metaphone_Secondary_Original_Name'] = zip(*candidates_df.apply(
                lambda x: get_phonetics_double_metaphone(x["Original"]),
                axis=1))
            candidates_df['Double_Metaphone_Primary_Candidate'], candidates_df['Double_Metaphone_Secondary_Candidate'] = zip(*candidates_df.apply(
                lambda x: get_phonetics_double_metaphone(x["Candidate"]),
                axis=1))
            candidates_df["Edit_Distance_Primary_DM_Original_Candidate"] =                 candidates_df.apply(lambda x: calculate_edit_distance(x["Double_Metaphone_Primary_Original_Name"], x["Double_Metaphone_Primary_Candidate"]),
                axis=1)
            candidates_df["Edit_Distance_Secondary_DM_Original_Candidate"] =                 candidates_df.apply(lambda x: calculate_edit_distance(x["Double_Metaphone_Secondary_Original_Name"], x["Double_Metaphone_Secondary_Candidate"]),
                                    axis=1)

            candidates_df["Edit_Distance_Primary_DM_Original_Secondary_Candidate"] =                 candidates_df.apply(lambda x: calculate_edit_distance(x["Double_Metaphone_Primary_Original_Name"],
                                                                      x["Double_Metaphone_Secondary_Candidate"]),
                                    axis=1)
            candidates_df["Edit_Distance_Secondary_DM_Original_Primary_Candidate"] =                 candidates_df.apply(lambda x: calculate_edit_distance(x["Double_Metaphone_Secondary_Original_Name"],
                                                                      x["Double_Metaphone_Primary_Candidate"]),
                                    axis=1)
            #candidates_df.to_csv(self._output_directory_path + "Metaphone_Edit_distance_graph.csv")
            candidates_df["Min_Edit_Distance_of_DM"] =                 candidates_df.apply(lambda x: find_positive_min_value(x["Edit_Distance_Primary_DM_Original_Candidate"],
                                                                      x["Edit_Distance_Secondary_DM_Original_Candidate"],
                                                                      x["Edit_Distance_Primary_DM_Original_Secondary_Candidate"],
                                                                      x["Edit_Distance_Secondary_DM_Original_Primary_Candidate"]),
                                    axis=1)


            candidates_df['Rank'] = candidates_df.apply(lambda x: ED_and_order_and_ED_of_DM(x["Edit_Distance"], x["Order"], x["Min_Edit_Distance_of_DM"]), axis=1)

            candidates_df = candidates_df.sort_values(by='Rank')
            head_candidates_df = candidates_df.head(10)
            return head_candidates_df
        return None
        
        
    #
    # Recieve the graph of father and son edit distance 1 until 3.
    # The ranking is according to double metaphone from the original name with edit distance.
    #

    def min_ED_of_DM(name_graph, original_name, neighbors_count):
        nodes = nx.single_source_shortest_path_length(name_graph, original_name, neighbors_count)
        
        if len(nodes) > 1:
            nodes = list(nodes.items())

            original_name_series = [original_name] * len(nodes)
            candidates_df = pd.DataFrame(nodes, columns=['Candidate', 'Order'])
            candidates_df['Original'] = original_name_series
            candidates_df = candidates_df[['Original', 'Candidate', 'Order']]

            candidates_df = candidates_df[candidates_df["Order"] != 0]
            candidates_df['Double_Metaphone_Primary_Original_Name'],             candidates_df['Double_Metaphone_Secondary_Original_Name'] = zip(*candidates_df.apply(
                lambda x: get_phonetics_double_metaphone(x["Original"]),
                axis=1))
            candidates_df['Double_Metaphone_Primary_Candidate'], candidates_df['Double_Metaphone_Secondary_Candidate'] = zip(*candidates_df.apply(
                lambda x: get_phonetics_double_metaphone(x["Candidate"]),
                axis=1))
            candidates_df["Edit_Distance_Primary_DM_Original_Candidate"] =                 candidates_df.apply(lambda x: calculate_edit_distance(x["Double_Metaphone_Primary_Original_Name"], x["Double_Metaphone_Primary_Candidate"]),
                axis=1)
            candidates_df["Edit_Distance_Secondary_DM_Original_Candidate"] =                 candidates_df.apply(lambda x: calculate_edit_distance(x["Double_Metaphone_Secondary_Original_Name"], x["Double_Metaphone_Secondary_Candidate"]),
                                    axis=1)

            candidates_df["Edit_Distance_Primary_DM_Original_Secondary_Candidate"] =                 candidates_df.apply(lambda x: calculate_edit_distance(x["Double_Metaphone_Primary_Original_Name"],
                                                                      x["Double_Metaphone_Secondary_Candidate"]),
                                    axis=1)
            candidates_df["Edit_Distance_Secondary_DM_Original_Primary_Candidate"] =                 candidates_df.apply(lambda x: calculate_edit_distance(x["Double_Metaphone_Secondary_Original_Name"],
                                                                      x["Double_Metaphone_Primary_Candidate"]),
                                    axis=1)
            #candidates_df.to_csv(self._output_directory_path + "Metaphone_Edit_distance_graph.csv")
            candidates_df["Min_Edit_Distance_of_DM"] =                 candidates_df.apply(lambda x: find_positive_min_value(x["Edit_Distance_Primary_DM_Original_Candidate"],
                                                                      x["Edit_Distance_Secondary_DM_Original_Candidate"],
                                                                      x["Edit_Distance_Primary_DM_Original_Secondary_Candidate"],
                                                                      x["Edit_Distance_Secondary_DM_Original_Primary_Candidate"]),
                                    axis=1)

            candidates_df["Rank"] = candidates_df["Min_Edit_Distance_of_DM"]
            #candidates_df = candidates_df.sort_values(by='Min_Edit_Distance_of_DM')
            candidates_df = candidates_df.sort_values(by='Rank')
            head_candidates_df = candidates_df.head(10)
            return head_candidates_df
        return None


# In[23]:


def get_graph_info(g):
    node_count = g.number_of_nodes()
    edge_count = g.number_of_edges()
    
    avg_in_degree = sum(d for n, d in g.in_degree()) / float(node_count)
    avg_out_degree = sum(d for n, d in g.out_degree()) / float(node_count)
    
    return node_count, edge_count, avg_in_degree, avg_out_degree


# In[24]:


# input_path = "/home/aviade/Names_Project/Family_Trees_TKDE/"
input_path = "/home/user/project_py_3/Family_Trees_TKDE/Family_Trees_TKDE/"
file_name = "records25k_data_surnames.tsv"

# last_names_ground_truth_df = pd.read_csv(input_path + file_name, delimiter='\t')
#
# last_names_ground_truth_df = last_names_ground_truth_df[['Name', 'Synonym']]
#
# last_names_ground_truth_df["Name"]= last_names_ground_truth_df["Name"].astype(str)
# last_names_ground_truth_df['Name'] = last_names_ground_truth_df['Name'].apply(lambda x: x.capitalize())
#
# last_names_ground_truth_df["Synonym"]= last_names_ground_truth_df["Synonym"].astype(str)
# last_names_ground_truth_df['Synonym'] = last_names_ground_truth_df['Synonym'].apply(lambda x: x.capitalize())
# last_names_ground_truth_df


# In[31]:


# #target_field_names = ["First Name", 'Last_Name_Current']
# target_field_names = ["First Name"]
# output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/First_Names/"

# parental_relation_types = ['Child_Father', 'Child_Grandfather', 'Child_GreatGrandfather']
# max_edit_distances = [2, 3, 4, 5]
# min_chars_counts = [2, 3]
# min_occurances = [1, 5, 10]

target_field_names = ["First Name"]
# target_field_names = ["Last Name Current"]
# output_path = "/home/user/project_py_3/Family_Trees_TKDE/Family_Trees_TKDE/V2/First_Names2/"
output_path = "/home/user/project_py_3/Family_Trees_TKDE/Family_Trees_TKDE/V2/First_Names/"
#output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/V2/Last_Names/"
# output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/V2/Last_Names_Btn_Wikitree/"
#output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/Ancestry/Last_Names/"


ground_truth_df = pd.read_csv('/home/user/project_py_3/Family_Trees_TKDE/Family_Trees_TKDE/ground_truth_constructed_based_on_all_first_names_behindthename.csv')
#ground_truth_df = last_names_ground_truth_df
# ground_truth_df = pd.read_csv('/home/aviade/Names_Project/Family_Trees_TKDE/behind_the_name_ground_truth_last_names_existing_in_wikitree.csv')


original_names = ground_truth_df["Name"].unique().tolist()
original_names = sorted(original_names)


# if not os.path.exists(output_path):
#     os.makedirs(output_path)
    

parental_relation_types = ['Child_Father']
# parental_relation_types = ['Child_Father', 'Child_Grandfather', 'Child_GreatGrandfather', 'Child_All_Ancestors']
#parental_relation_types = ['Child_GreatGrandfather']
#parental_relation_types = ['Child_All_Ancestors']

#max_edit_distances = [2, 3, 4, 5, 100]
#max_edit_distances = [2, 3, 4, 5]
max_edit_distances = [3]
#min_chars_counts = [2, 3]
min_chars_counts = [2]
#min_occurances = [1, 5, 10]
min_occurances = [10]
#neighbors_counts = [1, 2, 3]
neighbors_counts = [2]
ranking_functions = ['ED_and_order', 
                     'order_2_and_ED', 
                     'min_ED_of_DM', 
                     'ED_and_order_and_ED_of_DM']


results = []
for target_field_name in target_field_names:
    for parental_relation_type in parental_relation_types:
        for min_chars_count in min_chars_counts:
            for max_edit_distance in max_edit_distances:                                                             
                for min_occurance in min_occurances:
                    
                    full_path = get_full_path(target_field_name, parental_relation_type, min_chars_count, max_edit_distance, min_occurance)
                    edges_sf = tc.SFrame.read_csv(full_path)
                    
                    start_time = time.time()
                    
                    name_graph = nx.DiGraph() # Creating Undirected Graph
                    # # adding all nodes and vertices at once
                    name_graph.add_weighted_edges_from([(r['Ancestor_Name'],r['Child_Name'], r['sum']) for r in edges_sf])
                    
                    node_count, edge_count, avg_in_degree, avg_out_degree = get_graph_info(name_graph)
                    
                    graph_creation_time = time.time() - start_time
                    
                    start_time = time.time()
                    for neighbors_count in neighbors_counts:
                        for i, ranking_function in tqdm(enumerate(ranking_functions)):
                            print("parental_relation: {0}, min_chars_count:{1}, max_ED:{2}, min_occurance:{3}, neighbors_count: {4}".format(parental_relation_type,
                                                                                                                     min_chars_count, 
                                                                                                                     max_edit_distance,
                                                                                                                     min_occurance,
                                                                                                                     neighbors_count))
                            dfs = []
                            for i, original_name in enumerate(original_names):
                                print("\rSuggesting candidates for: {0} {1} {2}/{3}".format(ranking_function, original_name, i, len(original_names)), end='')

                                if name_graph.has_node(original_name):
                                    candidates_df = getattr(OrderingFunctions, ranking_function)(name_graph, original_name, neighbors_count)
                                    if candidates_df is not None:
                                        dfs.append(candidates_df)

                            ordering_function_execution_time = time.time() - start_time

                            suggestions_df = pd.concat(dfs)
                            suggestions_df = suggestions_df.sort_values(by=['Original', 'Rank'])
                            #results_df.to_csv(output_path + "name_based_network_ED_1_to_3_ranking_by_Order_2_and_ED_suggestions.csv", index=False)

                            results_path = get_results_full_path(target_field_name, parental_relation_type, 
                                                                 min_chars_count, max_edit_distance, min_occurance,
                                                                neighbors_count, ranking_function)

                            suggestions_df.to_csv(results_path, index=False)



                            result = (target_field_name, parental_relation_type, min_chars_count, max_edit_distance, 
                                      min_occurance, ranking_function, neighbors_count, graph_creation_time, 
                                      ordering_function_execution_time, node_count, edge_count, avg_in_degree, 
                                      avg_out_degree)
                            results.append(result)

results_df = pd.DataFrame(results, columns=['target_field_name', 'parental_relation_type', 'min_chars_count', 'max_edit_distance', 
                                            'min_occurance', 'ranking_function', 'neighbors_count', 'graph_creation_time', 'ordering_function_execution_time',
                                            'node_count', 'edge_count', 'avg_in_degree', 'avg_out_degree'])
now = datetime.now()

date_time = now.strftime("%d/%m/%Y_%H:%M:%S")
date_time = date_time.replace(':', '_')
date_time = date_time.replace('/', '_')
results_df.to_csv(output_path + "Ordering_Functions_Time_Performance_{0}.csv".format(date_time), index=False)

print("Done!")
                

# # Performance 

# In[32]:


# def get_full_path_suggestions(target_field_name, parental_relation_type, min_chars_count, max_edit_distance, min_occurance, 
#                               ranking_function, neighbors_count):
#     targeted_field_name = target_field_name.replace(" ", "_")


#     full_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_ancestors_geq_{4}_occur_{5}_{6}_neighb.csv".format(min_chars_count, 
#     max_edit_distance, targeted_field_name, parental_relation_type, min_occurance, ranking_function, neighbors_count)
#     return full_path


def get_full_path_suggestions(target_field_name, parental_relation_type, min_chars_count, max_edit_distance, min_occurance, 
                              ranking_function, neighbors_count):
    targeted_field_name = target_field_name.replace(" ", "_")
    
    full_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_geq_{0}_chars_ED_1_{1}_geq_{4}_occur_{5}_{6}_neighb.csv".format(min_chars_count, 
    max_edit_distance, targeted_field_name, parental_relation_type, min_occurance, ranking_function, neighbors_count)
    return full_path


# def get_results_full_path(full_path, ranking_function):
#     full_path_without_extension = full_path.split(".")[0]
#     updated_path =  full_path_without_extension + "_{0}_suggestions.csv".format(ranking_function)
#     return updated_path


# In[33]:


def compare_suggestion(original_name, candidate, ground_truth_df):
    print("original_name: {0}, candidate: {1}".format(original_name, candidate))
    result_df = ground_truth_df[
        (ground_truth_df["Name"] == original_name) &
        (ground_truth_df["Synonym"] == candidate)]

    if result_df.empty:
        return 0
    return 1


# In[34]:


def compare_suggestion_with_ground_truth_by_provided_dfs(suggestions_df, ground_truth_df, ranking_function, 
                                                         full_path_suggestions_file_no_prefix):
    print('compare_suggestion_with_ground_truth_by_provided_dfs')
    suggestions_df['Is_Original_Synonym'] = suggestions_df.apply(
        lambda x: compare_suggestion(x["Original"], x["Candidate"], ground_truth_df), axis=1)

    suggestions_df.to_csv(full_path_suggestions_file_no_prefix + "_with_gt.csv", index=False)
    return suggestions_df


def calculate_performance_for_suggestions():
    print("calculate_performance_for_suggestions...")
    for ranking_function in ranking_functions:
        suggestions_df = pd.read_csv(output_path + "{0}_{1}_suggest_with_gt.csv".format(graph_type, ranking_function))
        
        # in case first names
        #ground_truth_df = pd.read_csv(original_path + '/results/ground_truth_constructed_based_on_all_first_names_behindthename.csv')
        
        # in case last names from Name2Vec
        #ground_truth_df = pd.read_csv(original_path + "/results/records25k_data_surnames.tsv", sep="\t")
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
      print("Ranking Function: {0} First Name: {1} {2}/{3}".format(ranking_function, source_name, i, len(source_names)))
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
  
        recall_1, recall_2, recall_3, recall_5, recall_10 = calculate_recall_at(predictions, source_name_num_of_relevant_synonyms)
  
        # precision_related_to_ground_truth = num_of_relevant_retrieved / float(num_of_retrieved)
  
        # recall = recall_score(actual, predictions)
  
        result_tuple = (source_name, num_of_relevant_retrieved, num_of_retrieved, num_of_relevant_retrieved_at_10, num_of_retrieved_at_10, source_name_num_of_relevant_synonyms,
        accuracy, accuracy_10, f1, f1_10, precison_1, precison_2, precison_3, precison_5, precision_10, precison, recall_1, recall_2, recall_3, recall_5, recall_10, 
        recall_related_to_ground_truth)
        final_results.append(result_tuple)
    
    final_results_df = pd.DataFrame(final_results, columns=['Source_Name', 'Num of Relevant Retrieved', 'Num of Retrieved', 'Num of Relevant Retrieved@10', 'Num of Retrieved@10',
    'Total Num of Relevant in Ground Truth', 'Accuracy', 'Accuracy@10', 'F1', 'F1@10', 'Precision@1', 'Precision@2', 'Precision@3', 'Precision@5', 'Precision@10',
    'Precision', 'Recall@1', 'Recall@2', 'Recall@3', 'Recall@5', 'Recall@10', 'Recall'])
    
    
    average_results = []
    final_results_columns = final_results_df.columns
    average_performance_tuple = ("AVERAGE",)
    for column in final_results_columns:
        
        if column == "Source_Name":
            continue
        average_score = final_results_df[column].mean()
        average_performance_tuple = average_performance_tuple + (average_score,)
        
    #print("len of average_performance_tuple is:{0}".format(len(average_performance_tuple)))
    #print("len of final_results_columns is:{0}".format(len(final_results_columns)))    
        
    df = pd.DataFrame([average_performance_tuple], columns = final_results_columns)
    
    df.reset_index(drop=True, inplace=True)
    final_results_df.reset_index(drop=True, inplace=True)

    final_results_with_performance_df = pd.concat([final_results_df, df])
    
    final_results_df.to_csv(full_path_suggestions_file_no_prefix + "_with_gt_perf_res.csv",index=False)
    
    final_results_with_performance_df.to_csv(full_path_suggestions_file_no_prefix + "_with_gt_perf_res_avg.csv",index=False)


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
# target_field_names = ["Last Name Current"]
#output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/V2/First_Names/"
# output_path = "/home/user/project_py_3/Family_Trees_TKDE/Family_Trees_TKDE/V2/First_Names2/"
output_path = "/home/user/project_py_3/Family_Trees_TKDE/Family_Trees_TKDE/V2/First_Names/"
# output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/V2/Last_Names/"
#output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/V2/Last_Names_Btn_Wikitree/"


# for first names
#ground_truth_df = pd.read_csv('/home/aviade/Names_Project/Family_Trees_TKDE/ground_truth_constructed_based_on_all_first_names_behindthename.csv')
# for last names ancestry
# ground_truth_df = last_names_ground_truth_df
#ground_truth_df = pd.read_csv('/home/aviade/Names_Project/Family_Trees_TKDE/behind_the_name_ground_truth_last_names_existing_in_wikitree.csv')


original_names = ground_truth_df["Name"].unique().tolist()
original_names = sorted(original_names)


# if not os.path.exists(output_path):
#     os.makedirs(output_path)
    

parental_relation_types = ['Child_Father']
#parental_relation_types = ['Child_Grandfather']
#parental_relation_types = ['Child_GreatGrandfather']
#parental_relation_types = ['Child_All_Ancestors']
# parental_relation_types = ['Child_Father', 'Child_Grandfather', 'Child_GreatGrandfather', 'Child_All_Ancestors']
# max_edit_distances = [2, 3, 4, 5]
# max_edit_distances = [3]
# min_chars_counts = [2]
# # min_occurances = [5]
# min_occurances = [10]
max_edit_distances = [2, 3, 4, 5, 100]
min_chars_counts = [2, 3]
min_occurances = [5, 10]
ranking_functions = ['ED_and_order',
                      'order_2_and_ED', 
                      'min_ED_of_DM', 
                      'ED_and_order_and_ED_of_DM']

neighbors_counts = [2]

results = []
for target_field_name in tqdm(target_field_names):
    for parental_relation_type in tqdm(parental_relation_types):
        for min_chars_count in min_chars_counts:
            for max_edit_distance in tqdm(max_edit_distances):                                                             
                for min_occurance in tqdm(min_occurances):
                     for i, ranking_function in tqdm(enumerate(ranking_functions)):
                            for neighbors_count in neighbors_counts:
                                full_path_suggestions_file = get_full_path_suggestions(target_field_name, parental_relation_type, 
                                                                                       min_chars_count, max_edit_distance, 
                                                                                       min_occurance, ranking_function,
                                                                                      neighbors_count)

                                full_path_suggestions_file_no_prefix = full_path_suggestions_file.split(".csv")[0]

                                suggestions_df = pd.read_csv(full_path_suggestions_file)



                                suggestions_with_ground_truth_df = compare_suggestion_with_ground_truth_by_provided_dfs(suggestions_df, ground_truth_df, ranking_function,
                                                                                                                       full_path_suggestions_file_no_prefix)

                                calculate_performance(suggestions_with_ground_truth_df, ground_truth_df, ranking_function,
                                                      full_path_suggestions_file_no_prefix)
                        
                        
print("Done!")


def min_ED_of_DM2(name_graph, original_name):
        nodes = nx.single_source_shortest_path_length(name_graph, original_name, 3)
        print(nodes)
        if len(nodes) > 1:
            nodes = list(nodes.items())

            original_name_series = [original_name] * len(nodes)
            candidates_df = pd.DataFrame(nodes, columns=['Candidate', 'Order'])
            candidates_df['Original'] = original_name_series
            candidates_df = candidates_df[['Original', 'Candidate', 'Order']]

            candidates_df = candidates_df[candidates_df["Order"] != 0]
            candidates_df['Double_Metaphone_Primary_Original_Name'],             candidates_df['Double_Metaphone_Secondary_Original_Name'] = zip(*candidates_df.apply(
                lambda x: get_phonetics_double_metaphone(x["Original"]),
                axis=1))
            candidates_df['Double_Metaphone_Primary_Candidate'], candidates_df['Double_Metaphone_Secondary_Candidate'] = zip(*candidates_df.apply(
                lambda x: get_phonetics_double_metaphone(x["Candidate"]),
                axis=1))
            candidates_df["Edit_Distance_Primary_DM_Original_Candidate"] =                 candidates_df.apply(lambda x: calculate_edit_distance(x["Double_Metaphone_Primary_Original_Name"], x["Double_Metaphone_Primary_Candidate"]),
                axis=1)
            candidates_df["Edit_Distance_Secondary_DM_Original_Candidate"] =                 candidates_df.apply(lambda x: calculate_edit_distance(x["Double_Metaphone_Secondary_Original_Name"], x["Double_Metaphone_Secondary_Candidate"]),
                                    axis=1)

            candidates_df["Edit_Distance_Primary_DM_Original_Secondary_Candidate"] =                 candidates_df.apply(lambda x: calculate_edit_distance(x["Double_Metaphone_Primary_Original_Name"],
                                                                      x["Double_Metaphone_Secondary_Candidate"]),
                                    axis=1)
            candidates_df["Edit_Distance_Secondary_DM_Original_Primary_Candidate"] =                 candidates_df.apply(lambda x: calculate_edit_distance(x["Double_Metaphone_Secondary_Original_Name"],
                                                                      x["Double_Metaphone_Primary_Candidate"]),
                                    axis=1)
        
            
            #candidates_df.to_csv(self._output_directory_path + "Metaphone_Edit_distance_graph.csv")
            
            
            candidates_df["Min_Edit_Distance_of_DM"] =                 candidates_df.apply(lambda x: find_positive_min_value(x["Edit_Distance_Primary_DM_Original_Candidate"],
                                                                      x["Edit_Distance_Secondary_DM_Original_Candidate"],
                                                                      x["Edit_Distance_Primary_DM_Original_Secondary_Candidate"],
                                                                      x["Edit_Distance_Secondary_DM_Original_Primary_Candidate"]),
                                    axis=1)

            candidates_df["Rank"] = candidates_df["Min_Edit_Distance_of_DM"]
            #candidates_df = candidates_df.sort_values(by='Min_Edit_Distance_of_DM')
            candidates_df = candidates_df.sort_values(by='Rank')
            #head_candidates_df = candidates_df.head(10)
            return candidates_df
        return None

original_name = "Jan"


head_candidates_df = min_ED_of_DM2(name_graph, original_name)

print(head_candidates_df)


print("Done!!!_")



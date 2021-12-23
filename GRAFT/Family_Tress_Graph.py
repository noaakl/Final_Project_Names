#!/usr/bin/env python
# coding: utf-8

# In[18]:


import pandas as pd 
path = "/home/user/project_py_3/Family_Trees_TKDE/Family_Trees_TKDE/Family_Trees/First_Names/Family_Tree_Name_Graph/Grandfather_Grandson_Graph_new/ED_1_3_Min_occur_10/"


file_name = "wt_first_names_child_grandfather_ED_1_3_min_occur_10_graph.csv"

grandfather_son_graph_df = pd.read_csv(path + file_name)


file_name_gt = "wt_fn_son_grandfather_ED_1_3_min_occur_10_graph_ED_and_order_and_ED_of_DM_suggest_with_gt.csv"

grandfather_son_graph_gt_df = pd.read_csv(path + file_name_gt)


# In[23]:


import turicreate as tc

wikitree_sf = tc.SFrame.read_csv('/home/aviade/Names_Project/Family_Trees/dump_people_user_full.csv', delimiter='\t')


# In[29]:


wikitree_sf = wikitree_sf[["First Name", "Last Name Current"]]


# In[26]:


wikitree_sf = wikitree_sf[["First Name", "Last Name Current"]]

column_names = ["First Name", "Last Name Current"]

for target_field_name in column_names:
    wikitree_sf = wikitree_sf[(wikitree_sf[target_field_name] != None) &
                                  (wikitree_sf[target_field_name] != '') &
                                  (wikitree_sf[target_field_name] != 'Unknown') &
                                  (wikitree_sf[target_field_name] != 'Anonymous')]


for target_field_name in column_names:
    wikitree_sf['Son_' + targeted_field_name] = wikitree_sf[target_field_name].apply(
            lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])
    #wikitree_sf = wikitree_sf.stack('Child_' + targeted_field_name, new_column_name='Child_' + targeted_field_name)

    wikitree_sf['Father_Ancestor_' + targeted_field_name] = wikitree_sf["Father " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])
    #wikitree_sf = wikitree_sf.stack('Father_' + targeted_field_name,
     #                               new_column_name='Father_' + targeted_field_name)

    wikitree_sf['Mother_Ancestor_' + targeted_field_name] = wikitree_sf["Mother " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])
    #wikitree_sf = wikitree_sf.stack('Mother_' + targeted_field_name,
    #                                new_column_name='Mother_' + targeted_field_name)

    wikitree_sf['Child_' + targeted_field_name] = wikitree_sf['Son_' + targeted_field_name].apply(lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('Child_' + targeted_field_name, new_column_name='Child_' + targeted_field_name)

    wikitree_sf["Father_" + targeted_field_name] = wikitree_sf["Father_Ancestor_" + targeted_field_name].apply(lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('Father_' + targeted_field_name,
                                                                  new_column_name='Father_' + targeted_field_name)

    wikitree_sf["Mother_" + targeted_field_name] = wikitree_sf["Mother_Ancestor_" + targeted_field_name].apply(lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('Mother_' + targeted_field_name,
                                                                    new_column_name='Mother_' + targeted_field_name)


# In[27]:


wikitree_sf['Son_' + targeted_field_name] = wikitree_sf[target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])
#wikitree_sf = wikitree_sf.stack('Child_' + targeted_field_name, new_column_name='Child_' + targeted_field_name)

wikitree_sf['Father_Ancestor_' + targeted_field_name] = wikitree_sf["Father " + target_field_name].apply(
    lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])
#wikitree_sf = wikitree_sf.stack('Father_' + targeted_field_name,
 #                               new_column_name='Father_' + targeted_field_name)

wikitree_sf['Mother_Ancestor_' + targeted_field_name] = wikitree_sf["Mother " + target_field_name].apply(
    lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])
#wikitree_sf = wikitree_sf.stack('Mother_' + targeted_field_name,
#                                new_column_name='Mother_' + targeted_field_name)

wikitree_sf['Child_' + targeted_field_name] = wikitree_sf['Son_' + targeted_field_name].apply(lambda x: [clean_content(sub_name) for sub_name in x])
wikitree_sf = wikitree_sf.stack('Child_' + targeted_field_name, new_column_name='Child_' + targeted_field_name)

wikitree_sf["Father_" + targeted_field_name] = wikitree_sf["Father_Ancestor_" + targeted_field_name].apply(lambda x: [clean_content(sub_name) for sub_name in x])
wikitree_sf = wikitree_sf.stack('Father_' + targeted_field_name,
                                                              new_column_name='Father_' + targeted_field_name)

wikitree_sf["Mother_" + targeted_field_name] = wikitree_sf["Mother_Ancestor_" + targeted_field_name].apply(lambda x: [clean_content(sub_name) for sub_name in x])
wikitree_sf = wikitree_sf.stack('Mother_' + targeted_field_name,
                                                                    new_column_name='Mother_' + targeted_field_name)


# In[20]:


import turicreare as tc

wikitree_sf = tc.SFrame.read_csv('/home/aviade/Names_Project/Family_Trees/dump_people_user_full.csv', delimiter='\t')
    

wikitree_sf = wikitree_sf[(wikitree_sf[target_field_name] != None) &
                              (wikitree_sf[target_field_name] != '') &
                              (wikitree_sf[target_field_name] != 'Unknown') &
                              (wikitree_sf[target_field_name] != 'Anonymous')]

wikitree_sf["First Name"]

original_names_series = ground_truth_df["Name"]
original_names = original_names_series.tolist()
original_names = list(set(original_names))

original_names = sorted(original_names)

#edges_df = pd.read_csv(output_path + 'first_names_graph_son_father_count_greater_than_9_aggregated.csv')
#edges_df = pd.read_csv(output_path + 'wt_first_names_son_father_ED_1_3_min_occur_5_graph.csv')
#edges_df = pd.read_csv(output_path + 'wt_fn_son_father_ED_1_2_min_occur_5_graph.csv')
#edges_df = pd.read_csv(output_path + 'wt_fn_son_father_ED_1_2_min_occur_10_graph.csv')
#edges_df = pd.read_csv(output_path + 'wt_fn_son_father_ED_1_4_min_occur_10_graph.csv')
#edges_df = pd.read_csv(output_path + 'wt_fn_son_father_ED_1_4_min_occur_5_graph.csv')

#edges_df = pd.read_csv(output_path + 'wt_first_names_child_grandfather_ED_{0}_{1}_min_occur_{2}_graph.csv'.format(min_edit_distance, max_edit_distance, minimal_number_of_occurance))
#edges_df = pd.read_csv(output_path + 'wt_first_names_child_father_ED_{0}_{1}_min_occur_{2}_graph.csv'.format(min_edit_distance, max_edit_distance, minimal_number_of_occurance))
edges_df = pd.read_csv(output_path + 'wt_first_names_child_greatgrandfather_ED_{0}_{1}_min_occur_{2}_graph.csv'.format(min_edit_distance, max_edit_distance, minimal_number_of_occurance))
name_graph = nx.from_pandas_edgelist(edges_df, 'Ancestor_First_Name', 'Child_First_Name', ['count'])


for i, ranking_function in enumerate(ranking_functions):
    dfs = []
    for i, original_name in enumerate(original_names):
        print("\rSuggesting candidates for: {0} {1} {2}/{3}".format(ranking_function, original_name, i, len(original_names)), end='')

        if name_graph.has_node(original_name):
            #candidates_df = self._suggest_names_for_original_name(name_graph, original_name)
            candidates_df = getattr(OrderingFunctions, ranking_function)(name_graph, original_name)
            dfs.append(candidates_df)

    results_df = pd.concat(dfs)
    results_df = results_df.sort_values(by=['Original', 'Rank'])
    #results_df.to_csv(output_path + "name_based_network_ED_1_to_3_ranking_by_Order_2_and_ED_suggestions.csv", index=False)
    results_df.to_csv(output_path + "{0}_{1}_suggest.csv".format(graph_type, ranking_function), index=False)
print("Done provide_suggestions!!!!!")
# return results_df, ground_truth_df


# In[17]:


grandfather_son_graph_gt_df.shape


# In[11]:


def calculate_mutual_chars(row):
    name1 = row[0]
    #print(name1)
    name2 = row[1]
    #print(name2)

    name1_chars = [char for char in name1] 
    name2_chars = [char for char in name2]
    
    num_mutual_chars = list(set(name1_chars).intersection(name2_chars))
    
    return len(num_mutual_chars)
    


grandfather_son_graph_df["mutual_characters"] = grandfather_son_graph_df.apply(lambda x: calculate_mutual_chars(x), axis=1)


# In[12]:


grandfather_son_graph_df


# In[ ]:





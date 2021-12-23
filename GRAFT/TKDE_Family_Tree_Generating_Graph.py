#!/usr/bin/env python
# coding: utf-8

import turicreate as tc
import re
import editdistance
import time
import turicreate.aggregate as agg
import networkx as nx
import pandas as pd

target_field_name = "First Name"
# output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/First_Names/"
output_path = "/home/user/project_py_3/Family_Trees_TKDE/Family_Trees_TKDE/V2/First_Names/"

start_time = time.time()
targeted_field_name = ''

# def create_son_grandfather_grandmother_by_field_name():
#     # target fle should be dump_people_user_full.csv
#     wikitree_sf = tc.SFrame.read_csv('/home/aviade/Names_Project/Family_Trees_TKDE/dump_people_users.csv', delimiter='\t')
#     wikitree_sf = wikitree_sf[(wikitree_sf[target_field_name] != None) &
#                               (wikitree_sf[target_field_name] != '') &
#                               (wikitree_sf[target_field_name] != 'Unknown') &
#                               (wikitree_sf[target_field_name] != 'Anonymous')]

#     wikitree_sf = wikitree_sf[(wikitree_sf['User ID'] != None)]

#     print("Father's calculations are starting!")

#     son_father_sf = wikitree_sf.join(wikitree_sf, on={'Father': 'User ID'}, how='inner')

#     son_father_sf = son_father_sf.select_columns(
#         ['User ID', 'WikiTree ID', 'First Name', 'Father', 'WikiTree ID.1', 'First Name.1', 'Mother'])

#     son_father_sf = son_father_sf.rename({'WikiTree ID.1': 'Father WikiTree ID',
#                                           'First Name.1': 'Father First Name'})

#     print("Father's calculations were finished!")

#     print("Mother's calculations are starting!")
#     son_father_mother_sf = son_father_sf.join(wikitree_sf, on={'Mother': 'User ID'}, how='inner')

#     son_father_mother_sf = son_father_mother_sf.select_columns(['User ID', 'WikiTree ID', 'First Name',
#                                                                 'Father', 'Father WikiTree ID', 'Father First Name',
#                                                                 'Mother', 'WikiTree ID.1', 'First Name.1'])

#     son_father_mother_sf = son_father_mother_sf.rename({'WikiTree ID.1': 'Mother WikiTree ID',
#                                                         'First Name.1': 'Mother First Name'})
#     print("Mother's calculations were finished!")


#     #print("Granndfather's calculations are starting!")
#     son_father_mother_grandfather_sf = son_father_mother_sf.join(wikitree_sf, on={'Father': 'User ID'}, how='right')
#     son_father_mother_grandfather_sf = son_father_mother_grandfather_sf.select_columns(['User ID', 'WikiTree ID', 'First Name',
#                                                                                         'Father', 'Father WikiTree ID', 'Father First Name',
#                                                                                         'Mother', 'Mother WikiTree ID', 'Mother First Name',
#                                                                                         'Father.1'])
#     son_father_mother_grandfather_sf = son_father_mother_grandfather_sf.rename({'Father.1': 'Grandfather'})

#     son_father_mother_grandfather_sf = son_father_mother_grandfather_sf.join(wikitree_sf, on={'Grandfather': 'User ID'}, how='inner')
#     son_father_mother_grandfather_sf = son_father_mother_grandfather_sf.select_columns(['User ID', 'WikiTree ID', 'First Name',
#                                                                                         'Father', 'Father WikiTree ID', 'Father First Name',
#                                                                                         'Mother', 'Mother WikiTree ID', 'Mother First Name',
#                                                                                         'Grandfather', 'WikiTree ID.1', 'First Name.1'])

#     son_father_mother_grandfather_sf = son_father_mother_grandfather_sf.rename({'WikiTree ID.1': 'Grandfather WikiTree ID',
#                                                                                 'First Name.1': 'Grandfather First Name'})

#     print("Granndfather's calculations were finished!")


#     son_father_mother_grandfather_sf = son_father_mother_grandfather_sf[(son_father_mother_grandfather_sf['User ID'] != None)]


#     print("Granndmother's calculations are starting!")
#     son_father_mother_grandfather_grandmother_sf = son_father_mother_grandfather_sf.join(wikitree_sf, on={'Mother': 'User ID'}, how='right')
#     son_father_mother_grandfather_grandmother_sf = son_father_mother_grandfather_grandmother_sf.select_columns(['User ID', 'WikiTree ID', 'First Name',
#                                                                                                                 'Father', 'Father WikiTree ID', 'Father First Name',
#                                                                                                                 'Mother', 'Mother WikiTree ID', 'Mother First Name',
#                                                                                                                 'Grandfather', 'Grandfather WikiTree ID', 'Grandfather First Name',
#                                                                                                                 'Mother.1'])
#     son_father_mother_grandfather_grandmother_sf = son_father_mother_grandfather_grandmother_sf.rename({'Mother.1': 'Grandmother'})

#     son_father_mother_grandfather_grandmother_sf = son_father_mother_grandfather_grandmother_sf.join(wikitree_sf, on={'Grandmother': 'User ID'}, how='inner')
#     son_father_mother_grandfather_grandmother_sf = son_father_mother_grandfather_grandmother_sf.select_columns(['User ID', 'WikiTree ID', 'First Name',
#                                                                                                                 'Father', 'Father WikiTree ID', 'Father First Name',
#                                                                                                                 'Mother', 'Mother WikiTree ID', 'Mother First Name',
#                                                                                                                 'Grandfather', 'Grandfather WikiTree ID', 'Grandfather First Name',
#                                                                                                                 'Grandmother', 'WikiTree ID.1', 'First Name.1'])

#     son_father_mother_grandfather_grandmother_sf = son_father_mother_grandfather_grandmother_sf.rename({'WikiTree ID.1': 'Grandmother WikiTree ID',
#                                                                                                         'First Name.1': 'Grandmother First Name'})
#     print("Granndmother's calculations were finished!")

#     son_father_mother_grandfather_grandmother_sf = son_father_mother_grandfather_grandmother_sf[(son_father_mother_grandfather_grandmother_sf['User ID'] != None)]


#     #son_father_mother_grandfather_grandmother_sf.export_csv(output_path + "wikitree_first_name_grandfather_grandson.csv")

#     return son_father_mother_grandfather_grandmother_sf


# # Create Child_grandfather_and_grandmother_file

def create_child_grandfather_grandmother_by_field_name():
    # target fle should be dump_people_user_full.csv
    # wikitree_sf = tc.SFrame.read_csv('/home/aviade/Names_Project/Family_Trees_TKDE/dump_people_users.csv', delimiter='\t')
    # wikitree_sf = tc.SFrame.read_csv('/Users/noaakless/Desktop/final_project/Names_Students_Project/Family_Trees_TKDE/dump_people_users.csv', delimiter='\t')
    wikitree_sf = tc.SFrame.read_csv(
        '/home/user/project_py_3/Family_Trees_TKDE/Family_Trees_TKDE/dump_people_users.csv', delimiter='\t')

    wikitree_sf = wikitree_sf[(wikitree_sf[target_field_name] != None) &
                              (wikitree_sf[target_field_name] != '') &
                              (wikitree_sf[target_field_name] != 'Unknown') &
                              (wikitree_sf[target_field_name] != 'Anonymous')]

    wikitree_sf = wikitree_sf[(wikitree_sf['User ID'] != None) & wikitree_sf['User ID'] != 0]

    wikitree_sf[target_field_name] = wikitree_sf[target_field_name].apply(lambda x: x.strip())

    print("Father's calculations are starting!")

    son_first_generation_sf = wikitree_sf.join(wikitree_sf, on={'Father': 'User ID'}, how='inner')

    son_first_generation_sf = son_first_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
                                                                      'Father', 'WikiTree ID.1',
                                                                      target_field_name + '.1',
                                                                      'Mother',
                                                                      'Father.1',
                                                                      'Mother.1'])

    son_first_generation_sf = son_first_generation_sf.rename({'WikiTree ID.1': 'Father WikiTree ID',
                                                              target_field_name + '.1': 'Father {0}'.format(
                                                                  target_field_name),
                                                              'Father.1': 'Grandfather father',
                                                              'Mother.1': 'Grandmother father'})
    print("Father's calculations were finished!")

    print("Mother's calculations are starting!")
    son_first_generation_sf = son_first_generation_sf.join(wikitree_sf, on={'Mother': 'User ID'}, how='inner')

    son_first_generation_sf = son_first_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
                                                                      'Father', 'Father WikiTree ID',
                                                                      'Father ' + target_field_name,
                                                                      'Mother', 'WikiTree ID.1',
                                                                      target_field_name + '.1',
                                                                      'Grandfather father',
                                                                      'Grandmother father',
                                                                      'Father.1',
                                                                      'Mother.1'])

    son_first_generation_sf = son_first_generation_sf.rename({'WikiTree ID.1': 'Mother WikiTree ID',
                                                              target_field_name + '.1': 'Mother {0}'.format(
                                                                  target_field_name),
                                                              'Father.1': 'Grandfather mother',
                                                              'Mother.1': 'Grandmother mother'
                                                              })
    print("Mother's calculations were finished!")

    print("Grandfather father's calculations are starting!")

    son_second_generation_sf = son_first_generation_sf.join(wikitree_sf, on={'Grandfather father': 'User ID'},
                                                            how='inner')

    son_second_generation_sf = son_second_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
                                                                        'Father', 'Father WikiTree ID',
                                                                        'Father ' + target_field_name,
                                                                        'Mother', 'Mother WikiTree ID',
                                                                        'Mother ' + target_field_name,
                                                                        'Grandfather father', 'WikiTree ID.1',
                                                                        target_field_name + '.1',
                                                                        'Grandmother father',
                                                                        'Grandfather mother',
                                                                        'Grandmother mother'])
    # 'Father.1',
    # 'Mother.1'])

    son_second_generation_sf = son_second_generation_sf.rename({'WikiTree ID.1': 'Grandfather father WikiTree ID',
                                                                target_field_name + '.1': 'Grandfather father {0}'.format(
                                                                    target_field_name)})
    # 'Father.1': 'GreatGrandfather Grandfather father',
    # 'Mother.1': 'GreatGrandmother Grandfather father'})
    print("Grandfather father's calculations were finished!")

    print("Grandmother father's calculations are starting!")

    son_second_generation_sf = son_second_generation_sf.join(wikitree_sf, on={'Grandmother father': 'User ID'},
                                                             how='inner')

    son_second_generation_sf = son_second_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
                                                                        'Father', 'Father WikiTree ID',
                                                                        'Father ' + target_field_name,
                                                                        'Mother', 'Mother WikiTree ID',
                                                                        'Mother ' + target_field_name,
                                                                        'Grandfather father',
                                                                        'Grandfather father WikiTree ID',
                                                                        'Grandfather father ' + target_field_name,
                                                                        'Grandmother father', 'WikiTree ID.1',
                                                                        target_field_name + '.1',
                                                                        'Grandfather mother',
                                                                        'Grandmother mother'])
    # 'GreatGrandfather Grandfather father',
    # 'GreatGrandmother Grandfather father',
    # 'Father.1',
    # 'Mother.1'])

    son_second_generation_sf = son_second_generation_sf.rename({'WikiTree ID.1': 'Grandmother father WikiTree ID',
                                                                target_field_name + '.1': 'Grandmother father ' + target_field_name})
    # 'Father.1': 'GreatGrandfather Grandmother father',
    # 'Mother.1': 'GreatGrandmother Grandmother father'})
    print("Grandmother father's calculations were finished!")

    print("Grandfather mother's calculations are starting!")

    son_second_generation_sf = son_second_generation_sf.join(wikitree_sf, on={'Grandfather mother': 'User ID'},
                                                             how='inner')

    son_second_generation_sf = son_second_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
                                                                        'Father', 'Father WikiTree ID',
                                                                        'Father ' + target_field_name,
                                                                        'Mother', 'Mother WikiTree ID',
                                                                        'Mother ' + target_field_name,
                                                                        'Grandfather father',
                                                                        'Grandfather father WikiTree ID',
                                                                        'Grandfather father ' + target_field_name,
                                                                        'Grandmother father',
                                                                        'Grandmother father WikiTree ID',
                                                                        'Grandmother father ' + target_field_name,
                                                                        'Grandfather mother', 'WikiTree ID.1',
                                                                        target_field_name + '.1',
                                                                        'Grandmother mother'])
    # 'GreatGrandfather Grandfather father',
    # 'GreatGrandmother Grandfather father',
    # 'GreatGrandfather Grandmother father',
    # 'GreatGrandmother Grandmother father'])
    # 'Father.1',
    # 'Mother.1'])

    son_second_generation_sf = son_second_generation_sf.rename({'WikiTree ID.1': 'Grandfather mother WikiTree ID',
                                                                target_field_name + '.1': 'Grandfather mother {0}'.format(
                                                                    target_field_name)})
    # 'Father.1': 'GreatGrandfather Grandfather mother',
    # 'Mother.1': 'GreatGrandmother Grandfather mother'})
    print("Grandfather mother's calculations were finished!")

    print("Grandmother mother's calculations are starting!")

    son_second_generation_sf = son_second_generation_sf.join(wikitree_sf, on={'Grandmother mother': 'User ID'},
                                                             how='inner')

    son_second_generation_sf = son_second_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
                                                                        'Father', 'Father WikiTree ID',
                                                                        'Father ' + target_field_name,
                                                                        'Mother', 'Mother WikiTree ID',
                                                                        'Mother ' + target_field_name,
                                                                        'Grandfather father',
                                                                        'Grandfather father WikiTree ID',
                                                                        'Grandfather father ' + target_field_name,
                                                                        'Grandmother father',
                                                                        'Grandmother father WikiTree ID',
                                                                        'Grandmother father ' + target_field_name,
                                                                        'Grandfather mother',
                                                                        'Grandfather mother WikiTree ID',
                                                                        'Grandfather mother ' + target_field_name,
                                                                        'Grandmother mother', 'WikiTree ID.1',
                                                                        target_field_name + '.1'])
    # 'GreatGrandfather Grandfather father',
    # 'GreatGrandmother Grandfather father',
    # 'GreatGrandfather Grandmother father',
    # 'GreatGrandmother Grandmother father',
    # 'GreatGrandfather Grandfather mother',
    # 'GreatGrandmother Grandfather mother',
    # 'Father.1',
    # 'Mother.1'])

    son_second_generation_sf = son_second_generation_sf.rename({'WikiTree ID.1': 'Grandmother mother WikiTree ID',
                                                                target_field_name + '.1': 'Grandmother mother {0}'.format(
                                                                    target_field_name)})
    # 'Father.1': 'GreatGrandfather Grandmother mother',
    # 'Mother.1': 'GreatGrandmother Grandmother mother'})

    print("Grandmother mother's calculations were finished!")

    son_second_generation_sf.materialize()

    return son_second_generation_sf


# # Stack records
def clean_content(name):
    regex = re.compile('[^a-zA-Z]')
    # First parameter is the replacement, second parameter is your input string
    cleaned_name = regex.sub('', name)

    cleaned_name = cleaned_name.title()
    return cleaned_name


def clean_slack_names(wikitree_sf):
    # wikitree_sf = tc.SFrame.read_csv(original_path + "wikitree_first_name_greatgrandfather_grandson.csv")

    targeted_field_name = target_field_name.replace(" ", "_")

    wikitree_sf['Son_' + targeted_field_name] = wikitree_sf[target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['Grandfather_father_' + targeted_field_name] = wikitree_sf[
        "Grandfather father " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['Grandmother_father_' + targeted_field_name] = wikitree_sf[
        "Grandmother father " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['Grandfather_mother_' + targeted_field_name] = wikitree_sf[
        "Grandfather mother " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['Grandmother_mother_' + targeted_field_name] = wikitree_sf[
        "Grandmother mother " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['Child_' + targeted_field_name] = wikitree_sf['Son_' + targeted_field_name].apply(
        lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('Child_' + targeted_field_name, new_column_name='Child_' + targeted_field_name)

    wikitree_sf["Grandfather_father_" + targeted_field_name] = wikitree_sf[
        "Grandfather_father_" + targeted_field_name].apply(lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('Grandfather_father_' + targeted_field_name,
                                    new_column_name='Grandfather_father_' + targeted_field_name)

    wikitree_sf["Grandmother_father_" + targeted_field_name] = wikitree_sf[
        "Grandmother_father_" + targeted_field_name].apply(lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('Grandmother_father_' + targeted_field_name,
                                    new_column_name='Grandmother_father_' + targeted_field_name)

    wikitree_sf["Grandfather_mother_" + targeted_field_name] = wikitree_sf[
        "Grandfather_mother_" + targeted_field_name].apply(lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('Grandfather_mother_' + targeted_field_name,
                                    new_column_name='Grandfather_mother_' + targeted_field_name)

    wikitree_sf["Grandmother_mother_" + targeted_field_name] = wikitree_sf[
        "Grandmother_mother_" + targeted_field_name].apply(lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('Grandmother_mother_' + targeted_field_name,
                                    new_column_name='Grandmother_mother_' + targeted_field_name)

    wikitree_sf.materialize()
    return wikitree_sf


def clean_prefix_names(sf):
    prefix_names = ['Van', 'van', 'Der', 'der', 'Del', 'del', 'Da', 'da', 'Mc', 'mc' 'La', 'la', 'Los', 'los',
                    'Don', 'don', 'Von', 'von', 'San', 'san']

    targeted_field_name = target_field_name.replace(" ", "_")

    sf = sf.filter_by(prefix_names, 'Child_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'Grandfather_father_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'Grandmother_father_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'Grandfather_mother_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'Grandmother_mother_' + targeted_field_name, exclude=True)

    #     # clear None names
    #     sf = sf.dropna()
    #     sf = sf[(sf['Child_' + targeted_field_name] != "") &
    #            (sf['Grandfather_father_' + targeted_field_name] != "") &
    #            (sf['Grandmother_father_' + targeted_field_name] != "") &
    #            (sf['Grandfather_mother_' + targeted_field_name] != "") &
    #            (sf['Grandmother_mother_' + targeted_field_name] != "")]

    #     sf['Child_' + targeted_field_name] = sf[~sf['Child_' + targeted_field_name].isin(prefix_names)]
    #     sf['Grandfather_father_' + targeted_field_name] = sf[~sf['Grandfather_father_' + targeted_field_name].isin(prefix_names)]
    #     sf['Grandmother_father_' + targeted_field_name] = sf[~sf['Grandmother_father_' + targeted_field_name].isin(prefix_names)]
    #     sf['Grandfather_mother_' + targeted_field_name] = sf[~sf['Grandfather_mother_' + targeted_field_name].isin(prefix_names)]
    #     sf['Grandmother_mother_' + targeted_field_name] = sf[~sf['Grandmother_mother_' + targeted_field_name].isin(prefix_names)]

    #     sf['Child_' + targeted_field_name] = sf['Child_' + targeted_field_name].apply(lambda x: x not in list(prefix_names))

    #     sf["Grandfather_father_" + targeted_field_name] = sf["Grandfather_father_" + targeted_field_name].apply(lambda x: x not in list(prefix_names))

    #     sf["Grandmother_father_" + targeted_field_name] = sf["Grandmother_father_" + targeted_field_name].apply(lambda x: x not in list(prefix_names))

    #     sf["Grandfather_mother_" + targeted_field_name] = sf["Grandfather_mother_" + targeted_field_name].apply(lambda x: x not in list(prefix_names))

    #     sf["Grandmother_mother_" + targeted_field_name] = sf["Grandmother_mother_" + targeted_field_name].apply(lambda x: x not in list(prefix_names))

    sf.materialize()

    return sf


# # Calculate Edit distance between child and ancestors
def calculate_edit_distance(name1, name2):
    if not name1 or not name2:
        return -1

    name1 = name1.lower()
    name2 = name2.lower()

    edit_dist = editdistance.eval(name1, name2)

    return edit_dist


def calculate_measures(wikitree_sf):
    # wikitree_sf = tc.SFrame.read_csv(original_path + 'wikitree_first_name_grandfather_grandson_clean.csv')

    targeted_field_name = target_field_name.replace(" ", "_")

    print("Calculating Edit Distance")

    wikitree_sf['Edit_Distance_Child_Grandfather_father'] = wikitree_sf.apply(
        lambda x: calculate_edit_distance(x["Child_" + targeted_field_name],
                                          x["Grandfather_father_" + targeted_field_name]))

    wikitree_sf['Edit_Distance_Child_Grandmother_father'] = wikitree_sf.apply(
        lambda x: calculate_edit_distance(x["Child_" + targeted_field_name],
                                          x["Grandmother_father_" + targeted_field_name]))

    wikitree_sf['Edit_Distance_Child_Grandfather_mother'] = wikitree_sf.apply(
        lambda x: calculate_edit_distance(x["Child_" + targeted_field_name],
                                          x["Grandfather_mother_" + targeted_field_name]))

    wikitree_sf['Edit_Distance_Child_Grandmother_mother'] = wikitree_sf.apply(
        lambda x: calculate_edit_distance(x["Child_" + targeted_field_name],
                                          x["Grandmother_mother_" + targeted_field_name]))

    wikitree_sf.materialize()

    return wikitree_sf


# # SELECT only edit distance in selected ranges and len(names)> 2
# def choose_edit_distance_min_to_max_three_characters_and_above(wikitree_sf, min_edit_distance, max_edit_distance):
#     output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/First_Names/higher_three_letters/Child_Grandfather/"
#     #wikitree_sf = tc.SFrame.read_csv(original_path + 'wikitree_first_name_grandfather_grandson_clean_and_measures.csv')

#     targeted_field_name = target_field_name.replace(" ", "_")

#     wikitree_sf['Child_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
#         lambda x: get_num_of_characters(x["Child_" + targeted_field_name]))

#     wikitree_sf['Grandfather_father_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
#         lambda x: get_num_of_characters(x["Grandfather_father_" + targeted_field_name]))

#     wikitree_sf['Grandmother_father_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
#         lambda x: get_num_of_characters(x["Grandmother_father_" + targeted_field_name]))

#     wikitree_sf['Grandfather_mother_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
#         lambda x: get_num_of_characters(x["Grandfather_mother_" + targeted_field_name]))

#     wikitree_sf['Grandmother_mother_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
#         lambda x: get_num_of_characters(x["Grandmother_mother_" + targeted_field_name]))

#     #wikitree_sf.materialize()

#     wikitree_sf = wikitree_sf[(wikitree_sf['Child_{0}_Num_Chars'.format(targeted_field_name)] > 2) &
#                               (wikitree_sf['Grandfather_father_{0}_Num_Chars'.format(targeted_field_name)] > 2) &
#                               (wikitree_sf['Grandmother_father_{0}_Num_Chars'.format(targeted_field_name)] > 2) &
#                               (wikitree_sf['Grandfather_mother_{0}_Num_Chars'.format(targeted_field_name)] > 2) &
#                               (wikitree_sf['Grandmother_mother_{0}_Num_Chars'.format(targeted_field_name)] > 2)
#                              ]


#     child_grandfather_father_edit_distance_sf = wikitree_sf[
#         ((wikitree_sf['Edit_Distance_Child_Grandfather_father'] >= min_edit_distance) &
#          (wikitree_sf['Edit_Distance_Child_Grandfather_father'] <= max_edit_distance))]

#     child_grandmother_father_edit_distance_sf = wikitree_sf[
#         ((wikitree_sf['Edit_Distance_Child_Grandmother_father'] >= min_edit_distance) &
#          (wikitree_sf['Edit_Distance_Child_Grandmother_father'] <= max_edit_distance))]

#     child_grandfather_mother_edit_distance_sf = wikitree_sf[
#         ((wikitree_sf['Edit_Distance_Child_Grandfather_mother'] >= min_edit_distance) &
#          (wikitree_sf['Edit_Distance_Child_Grandfather_mother'] <= max_edit_distance))]

#     child_grandmother_mother_edit_distance_sf = wikitree_sf[
#         ((wikitree_sf['Edit_Distance_Child_Grandmother_mother'] >= min_edit_distance) &
#          (wikitree_sf['Edit_Distance_Child_Grandmother_mother'] <= max_edit_distance))]

#     return child_grandfather_father_edit_distance_sf, child_grandmother_father_edit_distance_sf, child_grandfather_mother_edit_distance_sf, child_grandmother_mother_edit_distance_sf

# import time
# import turicreate as tc

# min_edit_distance = 1
# max_edit_distance = 3

# start_time = time.time()
# child_grandfather_father_edit_distance_sf, child_grandmother_father_edit_distance_sf, /
# child_grandfather_mother_edit_distance_sf, child_grandmother_mother_edit_distance_sf = choose_edit_distance_min_to_max_three_characters_and_above(son_grandfather_grandmother_stacked_measure_sf,
#                                                                                                                                                   min_edit_distance,
#                                                                                                                                                   max_edit_distance)
# print("--- %s seconds filtering between min and max edit distance ---" % (time.time() - start_time))

# output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/First_Names/higher_three_letters/Child_Grandfather/ED_{0}_{1}/".format(min_edit_distance, max_edit_distance)

# child_grandfather_father_edit_distance_sf.export_csv(output_path + "child_grandfather_father_ED_{0}_{1}.csv".format(min_edit_distance, max_edit_distance))
# child_grandmother_father_edit_distance_sf.export_csv(output_path + "child_grandmother_father_ED_{0}_{1}.csv".format(min_edit_distance, max_edit_distance))
# child_grandfather_mother_edit_distance_sf.export_csv(output_path + "child_grandfather_mother_ED_{0}_{1}.csv".format(min_edit_distance, max_edit_distance))
# child_grandmother_mother_edit_distance_sf.export_csv(output_path + "child_grandmother_mother_ED_{0}_{1}.csv".format(min_edit_distance, max_edit_distance))


# # SELECT only edit distance in selected ranges no limitation on names length

def get_num_of_characters(x):
    if x is None:
        return -1
    return len(x)


def choose_ed_min_to_max_limit_characters(wikitree_sf, min_edit_distance, max_edit_distance, min_letters_for_name):
    targeted_field_name = target_field_name.replace(" ", "_")

    wikitree_sf = wikitree_sf[(wikitree_sf["Child_" + targeted_field_name] != "") &
                              (wikitree_sf["Grandfather_father_" + targeted_field_name] != "") &
                              (wikitree_sf["Grandmother_father_" + targeted_field_name] != "") &
                              (wikitree_sf["Grandfather_mother_" + targeted_field_name] != "") &
                              (wikitree_sf["Grandmother_mother_" + targeted_field_name] != "")]

    wikitree_sf['Child_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["Child_" + targeted_field_name]))

    wikitree_sf['Grandfather_father_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["Grandfather_father_" + targeted_field_name]))

    wikitree_sf['Grandmother_father_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["Grandmother_father_" + targeted_field_name]))

    wikitree_sf['Grandfather_mother_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["Grandfather_mother_" + targeted_field_name]))

    wikitree_sf['Grandmother_mother_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["Grandmother_mother_" + targeted_field_name]))

    wikitree_sf = wikitree_sf[(wikitree_sf['Child_{0}_Num_Chars'.format(targeted_field_name)] > min_letters_for_name) &
                              (wikitree_sf['Grandfather_father_{0}_Num_Chars'.format(
                                  targeted_field_name)] > min_letters_for_name) &
                              (wikitree_sf['Grandmother_father_{0}_Num_Chars'.format(
                                  targeted_field_name)] > min_letters_for_name) &
                              (wikitree_sf['Grandfather_mother_{0}_Num_Chars'.format(
                                  targeted_field_name)] > min_letters_for_name) &
                              (wikitree_sf['Grandmother_mother_{0}_Num_Chars'.format(
                                  targeted_field_name)] > min_letters_for_name)
                              ]

    wikitree_sf.materialize()

    child_grandfather_father_ed_sf = wikitree_sf[
        ((wikitree_sf['Edit_Distance_Child_Grandfather_father'] >= min_edit_distance) &
         (wikitree_sf['Edit_Distance_Child_Grandfather_father'] <= max_edit_distance))]

    child_grandfather_father_ed_sf.materialize()

    child_grandmother_father_ed_sf = wikitree_sf[
        ((wikitree_sf['Edit_Distance_Child_Grandmother_father'] >= min_edit_distance) &
         (wikitree_sf['Edit_Distance_Child_Grandmother_father'] <= max_edit_distance))]

    child_grandmother_father_ed_sf.materialize()

    child_grandfather_mother_ed_sf = wikitree_sf[
        ((wikitree_sf['Edit_Distance_Child_Grandfather_mother'] >= min_edit_distance) &
         (wikitree_sf['Edit_Distance_Child_Grandfather_mother'] <= max_edit_distance))]

    child_grandfather_mother_ed_sf.materialize()

    child_grandmother_mother_ed_sf = wikitree_sf[
        ((wikitree_sf['Edit_Distance_Child_Grandmother_mother'] >= min_edit_distance) &
         (wikitree_sf['Edit_Distance_Child_Grandmother_mother'] <= max_edit_distance))]

    child_grandmother_mother_ed_sf.materialize()

    return child_grandfather_father_ed_sf, child_grandmother_father_ed_sf, child_grandfather_mother_ed_sf, child_grandmother_mother_ed_sf


def group_by_child_and_ancestors(child_grandfather_father_ed_sf,
                                 child_grandmother_father_ed_sf,
                                 child_grandfather_mother_ed_sf,
                                 child_grandmother_mother_ed_sf):
    child_grandfather_father_sf = child_grandfather_father_ed_sf.groupby(
        key_column_names=['Child_' + targeted_field_name, 'Grandfather_father_' + targeted_field_name,
                          'Edit_Distance_Child_Grandfather_father'], operations={'count': agg.COUNT()})
    child_grandfather_father_sf = child_grandfather_father_sf.sort(['count'], ascending=False)
    child_grandfather_father_sf = child_grandfather_father_sf.select_columns(['Child_' + targeted_field_name,
                                                                              'Grandfather_father_' + targeted_field_name,
                                                                              'Edit_Distance_Child_Grandfather_father',
                                                                              'count'])

    child_grandfather_father_sf.materialize()

    child_grandmother_father_sf = child_grandmother_father_ed_sf.groupby(
        key_column_names=['Child_' + targeted_field_name, 'Grandmother_father_' + targeted_field_name,
                          'Edit_Distance_Child_Grandmother_father'], operations={'count': agg.COUNT()})
    child_grandmother_father_sf = child_grandmother_father_sf.sort(['count'], ascending=False)
    child_grandmother_father_sf = child_grandmother_father_sf.select_columns(['Child_' + targeted_field_name,
                                                                              'Grandmother_father_' + targeted_field_name,
                                                                              'Edit_Distance_Child_Grandmother_father',
                                                                              'count'])

    child_grandmother_father_sf.materialize()

    child_grandfather_mother_sf = child_grandfather_mother_ed_sf.groupby(
        key_column_names=['Child_' + targeted_field_name, 'Grandfather_mother_' + targeted_field_name,
                          'Edit_Distance_Child_Grandfather_mother'], operations={'count': agg.COUNT()})
    child_grandfather_mother_sf = child_grandfather_mother_sf.sort(['count'], ascending=False)
    child_grandfather_mother_sf.select_columns(['Child_' + targeted_field_name,
                                                'Grandfather_mother_' + targeted_field_name,
                                                'Edit_Distance_Child_Grandfather_mother', 'count'])

    child_grandfather_mother_sf.materialize()

    child_grandmother_mother_sf = child_grandmother_mother_ed_sf.groupby(
        key_column_names=['Child_' + targeted_field_name, 'Grandmother_mother_' + targeted_field_name,
                          'Edit_Distance_Child_Grandmother_mother'], operations={'count': agg.COUNT()})
    child_grandmother_mother_sf = child_grandmother_mother_sf.sort(['count'], ascending=False)
    child_grandmother_mother_sf = child_grandmother_mother_sf.select_columns(['Child_' + targeted_field_name,
                                                                              'Grandmother_mother_' + targeted_field_name,
                                                                              'Edit_Distance_Child_Grandmother_mother',
                                                                              'count'])

    child_grandmother_mother_sf.materialize()

    return child_grandfather_father_sf, child_grandmother_father_sf, child_grandfather_mother_sf, child_grandmother_mother_sf


# # Renaming columns, appending results, summing all
def unite_all_child_ancestors(child_grandfather_father_sf, child_grandmother_father_sf,
                              child_grandfather_mother_sf, child_grandmother_mother_sf):
    targeted_field_name = target_field_name.replace(" ", "_")

    child_grandfather_father_renamed_sf = child_grandfather_father_sf.rename(
        {'Child_' + targeted_field_name: 'Child_Name',
         'Grandfather_father_' + targeted_field_name: 'Ancestor_Name',
         'Edit_Distance_Child_Grandfather_father': 'Edit_Distance'})
    print(child_grandfather_father_renamed_sf.shape)

    child_grandmother_father_renamed_sf = child_grandmother_father_sf.rename(
        {'Child_' + targeted_field_name: 'Child_Name',
         'Grandmother_father_' + targeted_field_name: 'Ancestor_Name',
         'Edit_Distance_Child_Grandmother_father': 'Edit_Distance'})
    print(child_grandmother_father_renamed_sf.shape)

    child_grandfather_mother_renamed_sf = child_grandfather_mother_sf.rename(
        {'Child_' + targeted_field_name: 'Child_Name',
         'Grandfather_mother_' + targeted_field_name: 'Ancestor_Name',
         'Edit_Distance_Child_Grandfather_mother': 'Edit_Distance'})
    print(child_grandfather_mother_renamed_sf.shape)

    child_grandmother_mother_renamed_sf = child_grandmother_mother_sf.rename(
        {'Child_' + targeted_field_name: 'Child_Name',
         'Grandmother_mother_' + targeted_field_name: 'Ancestor_Name',
         'Edit_Distance_Child_Grandmother_mother': 'Edit_Distance'})
    print(child_grandmother_mother_renamed_sf.shape)

    child_ancestors_count_sf = child_grandfather_father_renamed_sf.copy()

    child_ancestors_count_sf = child_ancestors_count_sf.append(child_grandmother_father_renamed_sf)
    child_ancestors_count_sf = child_ancestors_count_sf.append(child_grandfather_mother_renamed_sf)
    child_ancestors_count_sf = child_ancestors_count_sf.append(child_grandmother_mother_renamed_sf)

    child_ancestors_count_united_sf = child_ancestors_count_sf.groupby(
        key_column_names=['Child_Name', 'Ancestor_Name', 'Edit_Distance'],
        operations={'sum': agg.SUM('count')})
    child_ancestors_count_united_sf = child_ancestors_count_united_sf.sort(['sum'], ascending=False)
    child_ancestors_count_united_sf = child_ancestors_count_united_sf.select_columns(
        ['Child_Name', 'Ancestor_Name', 'Edit_Distance', 'sum'])

    return child_ancestors_count_united_sf


# # Filter 10 occurances and above
def filter_higher_than_n_occurances(sf, n):
    filtered_sf = sf[sf["sum"] >= n]
    filtered_sf.materialize()
    return filtered_sf


def main():

    son_father_mother_grandfather_grandmother_sf = create_child_grandfather_grandmother_by_field_name()
    print("--- %s seconds for creating son_father_mother_grandfather_grandmother ---" % (time.time() - start_time))

    targeted_field_name = target_field_name.replace(" ", "_")
    son_father_mother_grandfather_grandmother_sf.export_csv(
        output_path + "wikitree_{0}_child_grandfather_grandmother.csv".format(targeted_field_name))

    short_son_father_mother_grandfather_grandmother_sf = son_father_mother_grandfather_grandmother_sf[
        target_field_name, 'Father ' + target_field_name, 'Mother ' + target_field_name,
        'Grandfather father ' + target_field_name, 'Grandmother father ' + target_field_name,
        'Grandfather mother ' + target_field_name, 'Grandmother mother ' + target_field_name]

    short_son_father_mother_grandfather_grandmother_sf.export_csv(
        output_path + "short_wikitree_{0}_child_grandfather_grandmother.csv".format(targeted_field_name))

    start_time = time.time()
    son_father_mother_grandfather_grandmother_stacked_sf = clean_slack_names(
        short_son_father_mother_grandfather_grandmother_sf)
    print("--- %s seconds for stacked ---" % (time.time() - start_time))
    son_father_mother_grandfather_grandmother_stacked_sf.export_csv(
        output_path + "wikitree_{0}_child_grandfather_grandmother_stacked.csv".format(targeted_field_name))

    targeted_field_name = target_field_name.replace(" ", "_")
    short_son_father_mother_grandfather_grandmother_stacked_sf = son_father_mother_grandfather_grandmother_stacked_sf[
        "Child_" + targeted_field_name, 'Grandfather_father_' + targeted_field_name, 'Grandmother_father_' + targeted_field_name,
        'Grandfather_mother_' + targeted_field_name, 'Grandmother_mother_' + targeted_field_name]

    short_son_father_mother_grandfather_grandmother_stacked_sf.export_csv(
        output_path + "short_wikitree_{0}_child_grandfather_grandmother_stacked.csv".format(targeted_field_name))

    print("Done!")
    # # Clean unnessary names

    targeted_field_name = target_field_name.replace(" ", "_")

    start_time = time.time()
    child_gfather_gmother_stacked_cleaned_sf = clean_prefix_names(
        short_son_father_mother_grandfather_grandmother_stacked_sf)
    print("--- %s seconds for cleaning prefix names ---" % (time.time() - start_time))
    child_gfather_gmother_stacked_cleaned_sf.export_csv(
        output_path + "wikitree_{0}_child_grandfather_grandmother_stacked_clean_prefixes.csv".format(
            targeted_field_name))
    child_gfather_gmother_stacked_cleaned_sf

    start_time = time.time()
    child_gfather_gmother_stacked_cleaned_ed_sf = calculate_measures(child_gfather_gmother_stacked_cleaned_sf)
    print("--- %s seconds for calculating_edit_distance ---" % (time.time() - start_time))

    targeted_field_name = target_field_name.replace(" ", "_")
    child_gfather_gmother_stacked_cleaned_ed_sf.export_csv(
        output_path + 'wikitree_{0}_child_grandfather_grandmother_ed.csv'.format(targeted_field_name))
    print("Done!")

    min_edit_distance = 1
    max_edit_distance = 3
    # it means tak all names higher than X
    min_letters_for_name = 2

    targeted_field_name = target_field_name.replace(" ", "_")


    child_gfather_gmother_stacked_cleaned_ed_sf = tc.SFrame.read_csv(
        output_path + 'wikitree_{0}_child_grandfather_grandmother_ed.csv'.format(targeted_field_name))

    start_time = time.time()
    child_grandfather_father_ed_sf, child_grandmother_father_ed_sf, child_grandfather_mother_ed_sf, child_grandmother_mother_ed_sf = choose_ed_min_to_max_limit_characters(
        child_gfather_gmother_stacked_cleaned_ed_sf, min_edit_distance, max_edit_distance, min_letters_for_name)

    print("--- %s seconds filtering between min and max edit distance ---" % (time.time() - start_time))

    out_path = output_path + "higher_{0}_letters/Child_Grandfather/ED_{1}_{2}/".format(min_letters_for_name,
                                                                                       min_edit_distance,
                                                                                       max_edit_distance)
    print(out_path + "child_grandfather_father_higher_{0}_letters_ED_{1}_{2}.csv".format(min_letters_for_name,
                                                                                         min_edit_distance,
                                                                                         max_edit_distance))

    child_grandfather_father_ed_sf.export_csv(
        out_path + "child_grandfather_father_higher_{0}_letters_ED_{1}_{2}.csv".format(min_letters_for_name,
                                                                                       min_edit_distance,
                                                                                       max_edit_distance))
    child_grandmother_father_ed_sf.export_csv(
        out_path + "child_grandmother_father_higher_{0}_letters_ED_{1}_{2}.csv".format(min_letters_for_name,
                                                                                       min_edit_distance,
                                                                                       max_edit_distance))
    child_grandfather_mother_ed_sf.export_csv(
        out_path + "child_grandfather_mother_higher_{0}_letters_ED_{1}_{2}.csv".format(min_letters_for_name,
                                                                                       min_edit_distance,
                                                                                       max_edit_distance))
    child_grandmother_mother_ed_sf.export_csv(
        out_path + "child_grandmother_mother_higher_{0}_letters_ED_{1}_{2}.csv".format(min_letters_for_name,
                                                                                       min_edit_distance,
                                                                                       max_edit_distance))

    start_time = time.time()
    child_grandfather_father_sf, child_grandmother_father_sf, child_grandfather_mother_sf, child_grandmother_mother_sf = group_by_child_and_ancestors(
        child_grandfather_father_ed_sf,
        child_grandmother_father_ed_sf,
        child_grandfather_mother_ed_sf,
        child_grandmother_mother_ed_sf)
    print("--- %s seconds counting occurances by Group by ---" % (time.time() - start_time))

    out_path = output_path + "higher_{0}_letters/Child_Grandfather/ED_{1}_{2}/".format(min_letters_for_name,
                                                                                       min_edit_distance,
                                                                                       max_edit_distance)

    child_grandfather_father_sf.export_csv(
        out_path + "child_grandfather_father_higher_{0}_letters_ED_{1}_{2}_count.csv".format(min_letters_for_name,
                                                                                             min_edit_distance,
                                                                                             max_edit_distance))
    child_grandmother_father_sf.export_csv(
        out_path + "child_grandmother_father_higher_{0}_letters_ED_{1}_{2}_count.csv".format(min_letters_for_name,
                                                                                             min_edit_distance,
                                                                                             max_edit_distance))
    child_grandfather_mother_sf.export_csv(
        out_path + "child_grandfather_mother_higher_{0}_letters_ED_{1}_{2}_count.csv".format(min_letters_for_name,
                                                                                             min_edit_distance,
                                                                                             max_edit_distance))
    child_grandmother_mother_sf.export_csv(
        out_path + "child_grandmother_mother_higher_{0}_letters_ED_{1}_{2}_count.csv".format(min_letters_for_name,
                                                                                             min_edit_distance,
                                                                                             max_edit_distance))
    start_time = time.time()
    child_ancestors_count_united_sf = unite_all_child_ancestors(child_grandfather_father_sf,
                                                                child_grandmother_father_sf,
                                                                child_grandfather_mother_sf,
                                                                child_grandmother_mother_sf)
    print("--- %s seconds uniting all child and ancestors occurances ---" % (time.time() - start_time))

    out_path = output_path + "higher_{0}_letters/Child_Grandfather/ED_{1}_{2}/".format(min_letters_for_name,
                                                                                       min_edit_distance,
                                                                                       max_edit_distance)

    child_ancestors_count_united_sf.export_csv(
        out_path + "child_grandfather_count_higher_{0}_letters_united_ED_{1}_{2}.csv".format(min_letters_for_name,
                                                                                             min_edit_distance,
                                                                                             max_edit_distance))
    child_ancestors_count_united_sf

    higher_min_occurances = 10

    start_time = time.time()
    filtered_child_ancestors_count_united_sf = filter_higher_than_n_occurances(child_ancestors_count_united_sf,
                                                                               higher_min_occurances)
    print("--- %s seconds removing occurances ---" % (time.time() - start_time))

    out_path = output_path + "higher_{0}_letters/Child_Grandfather/ED_{1}_{2}/".format(min_letters_for_name,
                                                                                       min_edit_distance,
                                                                                       max_edit_distance)
    filtered_child_ancestors_count_united_sf.export_csv(
        out_path + "child_grandfather_count_higher_{0}_letters_united_ED_{1}_{2}_min_occur_{3}.csv.csv".format(
            min_letters_for_name,
            min_edit_distance,
            max_edit_distance,
            higher_min_occurances))

    g = nx.DiGraph()  # Creating Undirected Graph
    # # adding all nodes and vertices at once
    g.add_weighted_edges_from(
        [(r['Ancestor_Name'], r['Child_Name'], r['sum']) for r in filtered_child_ancestors_count_united_sf])
    print(nx.info(g))

    nx.draw(g)

    name_closeness_centrality_score_dict = nx.closeness_centrality(g)

    df = pd.DataFrame({"name": list(name_closeness_centrality_score_dict.keys()),
                       "closeness_centrality": list(name_closeness_centrality_score_dict.values())})

    top_200 = df.sort_values("closeness_centrality", ascending=False).head(200)

    sub_graph = g.subgraph(top_200["name"])

    nx.draw(sub_graph, nodelist=list(top_200["name"]),
            node_size=[v * 10000 for v in list(top_200["closeness_centrality"])],
            with_labels=True)

    print("Done")


main()



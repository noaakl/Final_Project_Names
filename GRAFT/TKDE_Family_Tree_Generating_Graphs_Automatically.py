#!/usr/bin/env python
# coding: utf-8

# In[12]:


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

# original_path = "/Users/noaakless/Desktop/final_project/Names_Students_Project/"
original_path = "/home/user/project_py_3/"


# In[13]:


def create_child_father_sframe(wikitree_sf, target_field_name):
    start_time = time.time()

    wikitree_sf = wikitree_sf[(wikitree_sf[target_field_name] != None) &
                              (wikitree_sf[target_field_name] != '') &
                              (wikitree_sf[target_field_name] != ' ') &
                              (wikitree_sf[target_field_name] != 'Unknown') &
                              (wikitree_sf[target_field_name] != 'Anonymous') &
                              (wikitree_sf[target_field_name] != 'Unnamed')
                              ]

    wikitree_sf = wikitree_sf[(wikitree_sf['User ID'] != None) & wikitree_sf['User ID'] != 0]

    wikitree_sf[target_field_name] = wikitree_sf[target_field_name].apply(lambda x: x.strip())

    print("Father's calculations are starting!")

    son_first_generation_sf = wikitree_sf.join(wikitree_sf, on={'Father': 'User ID'}, how='inner')

    son_first_generation_sf = son_first_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
                                                                      'Father', 'WikiTree ID.1',
                                                                      target_field_name + '.1',
                                                                      'Mother'])

    son_first_generation_sf = son_first_generation_sf.rename({'WikiTree ID.1': 'Father WikiTree ID',
                                                              target_field_name + '.1': 'Father {0}'.format(
                                                                  target_field_name)})

    print("Father's calculations were finished!")

    print("Mother's calculations are starting!")
    son_first_generation_sf = son_first_generation_sf.join(wikitree_sf, on={'Mother': 'User ID'}, how='inner')

    son_first_generation_sf = son_first_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
                                                                      'Father', 'Father WikiTree ID',
                                                                      'Father ' + target_field_name,
                                                                      'Mother', 'WikiTree ID.1',
                                                                      target_field_name + '.1'])

    son_first_generation_sf = son_first_generation_sf.rename({'WikiTree ID.1': 'Mother WikiTree ID',
                                                              target_field_name + '.1': 'Mother {0}'.format(
                                                                  target_field_name),
                                                              })
    print("Mother's calculations were finished!")

    son_first_generation_sf.materialize()
    duration = time.time() - start_time

    return son_first_generation_sf, duration


# In[14]:


def create_child_grandfather_sframe(wikitree_sf, target_field_name):
    start_time = time.time()

    wikitree_sf = wikitree_sf[(wikitree_sf[target_field_name] != None) &
                              (wikitree_sf[target_field_name] != '') &
                              (wikitree_sf[target_field_name] != ' ') &
                              (wikitree_sf[target_field_name] != 'Unknown') &
                              (wikitree_sf[target_field_name] != 'Anonymous') &
                              (wikitree_sf[target_field_name] != 'Unnamed')]

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

    duration = time.time() - start_time
    return son_second_generation_sf, duration


# In[15]:


def create_child_greatgrandfather_sframe(wikitree_sf, target_field_name):
    start_time = time.time()

    print("target_field_name: {0}".format(target_field_name))

    wikitree_sf = wikitree_sf[(wikitree_sf[target_field_name] != None) &
                              (wikitree_sf[target_field_name] != '') &
                              (wikitree_sf[target_field_name] != ' ') &
                              (wikitree_sf[target_field_name] != 'Unknown') &
                              (wikitree_sf[target_field_name] != 'Anonymous') &
                              (wikitree_sf[target_field_name] != 'Unnamed')]

    wikitree_sf = wikitree_sf[(wikitree_sf['User ID'] != None) &
                              (wikitree_sf['User ID'] != 0)]

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
                                                                        'Grandmother mother',
                                                                        'Father.1',
                                                                        'Mother.1'])

    son_second_generation_sf = son_second_generation_sf.rename({'WikiTree ID.1': 'Grandfather father WikiTree ID',
                                                                target_field_name + '.1': 'Grandfather father {0}'.format(
                                                                    target_field_name),
                                                                'Father.1': 'GreatGrandfather Grandfather father',
                                                                'Mother.1': 'GreatGrandmother Grandfather father'})
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
                                                                        'Grandmother mother',
                                                                        'GreatGrandfather Grandfather father',
                                                                        'GreatGrandmother Grandfather father',
                                                                        'Father.1',
                                                                        'Mother.1'])

    son_second_generation_sf = son_second_generation_sf.rename({'WikiTree ID.1': 'Grandmother father WikiTree ID',
                                                                target_field_name + '.1': 'Grandmother father {0}'.format(
                                                                    target_field_name),
                                                                'Father.1': 'GreatGrandfather Grandmother father',
                                                                'Mother.1': 'GreatGrandmother Grandmother father'})
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
                                                                        'Grandmother mother',
                                                                        'GreatGrandfather Grandfather father',
                                                                        'GreatGrandmother Grandfather father',
                                                                        'GreatGrandfather Grandmother father',
                                                                        'GreatGrandmother Grandmother father',
                                                                        'Father.1',
                                                                        'Mother.1'])

    son_second_generation_sf = son_second_generation_sf.rename({'WikiTree ID.1': 'Grandfather mother WikiTree ID',
                                                                target_field_name + '.1': 'Grandfather mother {0}'.format(
                                                                    target_field_name),
                                                                'Father.1': 'GreatGrandfather Grandfather mother',
                                                                'Mother.1': 'GreatGrandmother Grandfather mother'})
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
                                                                        target_field_name + '.1',
                                                                        'GreatGrandfather Grandfather father',
                                                                        'GreatGrandmother Grandfather father',
                                                                        'GreatGrandfather Grandmother father',
                                                                        'GreatGrandmother Grandmother father',
                                                                        'GreatGrandfather Grandfather mother',
                                                                        'GreatGrandmother Grandfather mother',
                                                                        'Father.1',
                                                                        'Mother.1'])

    son_second_generation_sf = son_second_generation_sf.rename({'WikiTree ID.1': 'Grandmother mother WikiTree ID',
                                                                target_field_name + '.1': 'Grandmother mother {0}'.format(
                                                                    target_field_name),
                                                                'Father.1': 'GreatGrandfather Grandmother mother',
                                                                'Mother.1': 'GreatGrandmother Grandmother mother'})

    print("Grandmother mother's calculations were finished!")

    print("GreatGrandfather Grandfather father's calculations are starting!")

    son_third_generation_sf = son_second_generation_sf.join(wikitree_sf,
                                                            on={'GreatGrandfather Grandfather father': 'User ID'},
                                                            how='inner')

    son_third_generation_sf = son_third_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
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
                                                                      'Grandmother mother',
                                                                      'Grandmother mother WikiTree ID',
                                                                      'Grandmother mother ' + target_field_name,
                                                                      'GreatGrandfather Grandfather father',
                                                                      'WikiTree ID.1', target_field_name + '.1',
                                                                      'GreatGrandmother Grandfather father',
                                                                      'GreatGrandfather Grandmother father',
                                                                      'GreatGrandmother Grandmother father',
                                                                      'GreatGrandfather Grandfather mother',
                                                                      'GreatGrandmother Grandfather mother',
                                                                      'GreatGrandfather Grandmother mother',
                                                                      'GreatGrandmother Grandmother mother'
                                                                      ])
    # 'Father.1',
    # 'Mother.1'])

    son_third_generation_sf = son_third_generation_sf.rename(
        {'WikiTree ID.1': 'GreatGrandfather Grandfather father WikiTree ID',
         target_field_name + '.1': 'GreatGrandfather Grandfather father {0}'.format(target_field_name)})
    # 'Father.1': 'GreatGrandfather Grandmother mother',
    # 'Mother.1': 'GreatGrandmother Grandmother mother'})

    print("GreatGrandfather Grandfather father's calculations were finished!")

    print("GreatGrandmother Grandfather father's calculations are starting!")

    son_third_generation_sf = son_third_generation_sf.join(wikitree_sf,
                                                           on={'GreatGrandmother Grandfather father': 'User ID'},
                                                           how='inner')

    son_third_generation_sf = son_third_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
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
                                                                      'Grandmother mother',
                                                                      'Grandmother mother WikiTree ID',
                                                                      'Grandmother mother ' + target_field_name,
                                                                      'GreatGrandfather Grandfather father',
                                                                      'GreatGrandfather Grandfather father WikiTree ID',
                                                                      'GreatGrandfather Grandfather father ' + target_field_name,
                                                                      'GreatGrandmother Grandfather father',
                                                                      'WikiTree ID.1', target_field_name + '.1',
                                                                      'GreatGrandfather Grandmother father',
                                                                      'GreatGrandmother Grandmother father',
                                                                      'GreatGrandfather Grandfather mother',
                                                                      'GreatGrandmother Grandfather mother',
                                                                      'GreatGrandfather Grandmother mother',
                                                                      'GreatGrandmother Grandmother mother'])
    # 'Father.1',
    # 'Mother.1'])

    son_third_generation_sf = son_third_generation_sf.rename(
        {'WikiTree ID.1': 'GreatGrandmother Grandfather father WikiTree ID',
         target_field_name + '.1': 'GreatGrandmother Grandfather father {0}'.format(target_field_name)})
    # 'Father.1': 'GreatGrandfather Grandmother mother',
    # 'Mother.1': 'GreatGrandmother Grandmother mother'})

    print(son_third_generation_sf)

    print("GreatGrandmother Grandfather father's calculations were finished!")

    print("GreatGrandfather Grandmother father's calculations are starting!")

    son_third_generation_sf = son_third_generation_sf.join(wikitree_sf,
                                                           on={'GreatGrandfather Grandmother father': 'User ID'},
                                                           how='inner')

    son_third_generation_sf = son_third_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
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
                                                                      'Grandmother mother',
                                                                      'Grandmother mother WikiTree ID',
                                                                      'Grandmother mother ' + target_field_name,
                                                                      'GreatGrandfather Grandfather father',
                                                                      'GreatGrandfather Grandfather father WikiTree ID',
                                                                      'GreatGrandfather Grandfather father ' + target_field_name,
                                                                      'GreatGrandmother Grandfather father',
                                                                      'GreatGrandmother Grandfather father WikiTree ID',
                                                                      'GreatGrandmother Grandfather father ' + target_field_name,
                                                                      'GreatGrandfather Grandmother father',
                                                                      'WikiTree ID.1', target_field_name + '.1',
                                                                      'GreatGrandmother Grandmother father',
                                                                      'GreatGrandfather Grandfather mother',
                                                                      'GreatGrandmother Grandfather mother',
                                                                      'GreatGrandfather Grandmother mother',
                                                                      'GreatGrandmother Grandmother mother'])
    # 'Father.1',
    # 'Mother.1'])

    son_third_generation_sf = son_third_generation_sf.rename(
        {'WikiTree ID.1': 'GreatGrandfather Grandmother father WikiTree ID',
         target_field_name + '.1': 'GreatGrandfather Grandmother father {0}'.format(target_field_name)})
    # 'Father.1': 'GreatGrandfather Grandmother mother',
    # 'Mother.1': 'GreatGrandmother Grandmother mother'})

    print("GreatGrandfather Grandmother father's calculations were finished!")

    print("GreatGrandmother Grandmother father's calculations are starting!")

    son_third_generation_sf = son_third_generation_sf.join(wikitree_sf,
                                                           on={'GreatGrandmother Grandmother father': 'User ID'},
                                                           how='inner')

    son_third_generation_sf = son_third_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
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
                                                                      'Grandmother mother',
                                                                      'Grandmother mother WikiTree ID',
                                                                      'Grandmother mother ' + target_field_name,
                                                                      'GreatGrandfather Grandfather father',
                                                                      'GreatGrandfather Grandfather father WikiTree ID',
                                                                      'GreatGrandfather Grandfather father ' + target_field_name,
                                                                      'GreatGrandmother Grandfather father',
                                                                      'GreatGrandmother Grandfather father WikiTree ID',
                                                                      'GreatGrandmother Grandfather father ' + target_field_name,
                                                                      'GreatGrandfather Grandmother father',
                                                                      'GreatGrandfather Grandmother father WikiTree ID',
                                                                      'GreatGrandfather Grandmother father ' + target_field_name,
                                                                      'GreatGrandmother Grandmother father',
                                                                      'WikiTree ID.1', target_field_name + '.1',
                                                                      'GreatGrandfather Grandfather mother',
                                                                      'GreatGrandmother Grandfather mother',
                                                                      'GreatGrandfather Grandmother mother',
                                                                      'GreatGrandmother Grandmother mother'
                                                                      ])
    # 'Father.1',
    # 'Mother.1'])

    son_third_generation_sf = son_third_generation_sf.rename(
        {'WikiTree ID.1': 'GreatGrandmother Grandmother father WikiTree ID',
         target_field_name + '.1': 'GreatGrandmother Grandmother father ' + target_field_name})
    # 'Father.1': 'GreatGrandfather Grandmother mother',
    # 'Mother.1': 'GreatGrandmother Grandmother mother'})

    print("GreatGrandfather Grandmother father's calculations were finished!")

    print("GreatGrandfather Grandfather mother's calculations are starting!")

    son_third_generation_sf = son_third_generation_sf.join(wikitree_sf,
                                                           on={'GreatGrandfather Grandfather mother': 'User ID'},
                                                           how='inner')

    son_third_generation_sf = son_third_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
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
                                                                      'Grandmother mother',
                                                                      'Grandmother mother WikiTree ID',
                                                                      'Grandmother mother ' + target_field_name,
                                                                      'GreatGrandfather Grandfather father',
                                                                      'GreatGrandfather Grandfather father WikiTree ID',
                                                                      'GreatGrandfather Grandfather father ' + target_field_name,
                                                                      'GreatGrandmother Grandfather father',
                                                                      'GreatGrandmother Grandfather father WikiTree ID',
                                                                      'GreatGrandmother Grandfather father ' + target_field_name,
                                                                      'GreatGrandfather Grandmother father',
                                                                      'GreatGrandfather Grandmother father WikiTree ID',
                                                                      'GreatGrandfather Grandmother father ' + target_field_name,
                                                                      'GreatGrandmother Grandmother father',
                                                                      'GreatGrandmother Grandmother father WikiTree ID',
                                                                      'GreatGrandmother Grandmother father ' + target_field_name,
                                                                      'GreatGrandfather Grandfather mother',
                                                                      'WikiTree ID.1', target_field_name + '.1',
                                                                      'GreatGrandmother Grandfather mother',
                                                                      'GreatGrandfather Grandmother mother',
                                                                      'GreatGrandmother Grandmother mother'])
    # 'Father.1',
    # 'Mother.1'])

    son_third_generation_sf = son_third_generation_sf.rename(
        {'WikiTree ID.1': 'GreatGrandfather Grandfather mother WikiTree ID',
         target_field_name + '.1': 'GreatGrandfather Grandfather mother {0}'.format(target_field_name)})
    # 'Father.1': 'GreatGrandfather Grandmother mother',
    # 'Mother.1': 'GreatGrandmother Grandmother mother'})

    print("GreatGrandfather Grandfather mother's calculations were finished!")

    print("GreatGrandmother Grandfather mother's calculations are starting!")

    son_third_generation_sf = son_third_generation_sf.join(wikitree_sf,
                                                           on={'GreatGrandmother Grandfather mother': 'User ID'},
                                                           how='inner')

    son_third_generation_sf = son_third_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
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
                                                                      'Grandmother mother',
                                                                      'Grandmother mother WikiTree ID',
                                                                      'Grandmother mother ' + target_field_name,
                                                                      'GreatGrandfather Grandfather father',
                                                                      'GreatGrandfather Grandfather father WikiTree ID',
                                                                      'GreatGrandfather Grandfather father ' + target_field_name,
                                                                      'GreatGrandmother Grandfather father',
                                                                      'GreatGrandmother Grandfather father WikiTree ID',
                                                                      'GreatGrandmother Grandfather father ' + target_field_name,
                                                                      'GreatGrandfather Grandmother father',
                                                                      'GreatGrandfather Grandmother father WikiTree ID',
                                                                      'GreatGrandfather Grandmother father ' + target_field_name,
                                                                      'GreatGrandmother Grandmother father',
                                                                      'GreatGrandmother Grandmother father WikiTree ID',
                                                                      'GreatGrandmother Grandmother father ' + target_field_name,
                                                                      'GreatGrandfather Grandfather mother',
                                                                      'GreatGrandfather Grandfather mother WikiTree ID',
                                                                      'GreatGrandfather Grandfather mother ' + target_field_name,
                                                                      'GreatGrandmother Grandfather mother',
                                                                      'WikiTree ID.1', target_field_name + '.1',
                                                                      'GreatGrandfather Grandmother mother',
                                                                      'GreatGrandmother Grandmother mother'])
    # 'Father.1',
    # 'Mother.1'])

    son_third_generation_sf = son_third_generation_sf.rename(
        {'WikiTree ID.1': 'GreatGrandmother Grandfather mother WikiTree ID',
         target_field_name + '.1': 'GreatGrandmother Grandfather mother {0}'.format(target_field_name)})
    # 'Father.1': 'GreatGrandfather Grandmother mother',
    # 'Mother.1': 'GreatGrandmother Grandmother mother'})

    print("GreatGrandmother Grandfather mother's calculations were finished!")

    print("GreatGrandfather Grandmother mother's calculations are starting!")

    son_third_generation_sf = son_third_generation_sf.join(wikitree_sf,
                                                           on={'GreatGrandfather Grandmother mother': 'User ID'},
                                                           how='inner')

    son_third_generation_sf = son_third_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
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
                                                                      'Grandmother mother',
                                                                      'Grandmother mother WikiTree ID',
                                                                      'Grandmother mother ' + target_field_name,
                                                                      'GreatGrandfather Grandfather father',
                                                                      'GreatGrandfather Grandfather father WikiTree ID',
                                                                      'GreatGrandfather Grandfather father ' + target_field_name,
                                                                      'GreatGrandmother Grandfather father',
                                                                      'GreatGrandmother Grandfather father WikiTree ID',
                                                                      'GreatGrandmother Grandfather father ' + target_field_name,
                                                                      'GreatGrandfather Grandmother father',
                                                                      'GreatGrandfather Grandmother father WikiTree ID',
                                                                      'GreatGrandfather Grandmother father ' + target_field_name,
                                                                      'GreatGrandmother Grandmother father',
                                                                      'GreatGrandmother Grandmother father WikiTree ID',
                                                                      'GreatGrandmother Grandmother father ' + target_field_name,
                                                                      'GreatGrandfather Grandfather mother',
                                                                      'GreatGrandfather Grandfather mother WikiTree ID',
                                                                      'GreatGrandfather Grandfather mother ' + target_field_name,
                                                                      'GreatGrandmother Grandfather mother',
                                                                      'GreatGrandmother Grandfather mother WikiTree ID',
                                                                      'GreatGrandmother Grandfather mother ' + target_field_name,
                                                                      'GreatGrandfather Grandmother mother',
                                                                      'WikiTree ID.1', target_field_name + '.1',
                                                                      'GreatGrandmother Grandmother mother'])
    # 'Father.1',
    # 'Mother.1'])

    son_third_generation_sf = son_third_generation_sf.rename(
        {'WikiTree ID.1': 'GreatGrandfather Grandmother mother WikiTree ID',
         target_field_name + '.1': 'GreatGrandfather Grandmother mother {0}'.format(target_field_name)})
    # 'Father.1': 'GreatGrandfather Grandmother mother',
    # 'Mother.1': 'GreatGrandmother Grandmother mother'})

    print("GreatGrandfather Grandmother mother's calculations were finished!")

    print("GreatGrandmother Grandmother mother's calculations are starting!")

    son_third_generation_sf = son_third_generation_sf.join(wikitree_sf,
                                                           on={'GreatGrandmother Grandmother mother': 'User ID'},
                                                           how='inner')

    son_third_generation_sf = son_third_generation_sf.select_columns(['User ID', 'WikiTree ID', target_field_name,
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
                                                                      'Grandmother mother',
                                                                      'Grandmother mother WikiTree ID',
                                                                      'Grandmother mother ' + target_field_name,
                                                                      'GreatGrandfather Grandfather father',
                                                                      'GreatGrandfather Grandfather father WikiTree ID',
                                                                      'GreatGrandfather Grandfather father ' + target_field_name,
                                                                      'GreatGrandmother Grandfather father',
                                                                      'GreatGrandmother Grandfather father WikiTree ID',
                                                                      'GreatGrandmother Grandfather father ' + target_field_name,
                                                                      'GreatGrandfather Grandmother father',
                                                                      'GreatGrandfather Grandmother father WikiTree ID',
                                                                      'GreatGrandfather Grandmother father ' + target_field_name,
                                                                      'GreatGrandmother Grandmother father',
                                                                      'GreatGrandmother Grandmother father WikiTree ID',
                                                                      'GreatGrandmother Grandmother father ' + target_field_name,
                                                                      'GreatGrandfather Grandfather mother',
                                                                      'GreatGrandfather Grandfather mother WikiTree ID',
                                                                      'GreatGrandfather Grandfather mother ' + target_field_name,
                                                                      'GreatGrandmother Grandfather mother',
                                                                      'GreatGrandmother Grandfather mother WikiTree ID',
                                                                      'GreatGrandmother Grandfather mother ' + target_field_name,
                                                                      'GreatGrandfather Grandmother mother',
                                                                      'GreatGrandfather Grandmother mother WikiTree ID',
                                                                      'GreatGrandfather Grandmother mother ' + target_field_name,
                                                                      'GreatGrandmother Grandmother mother',
                                                                      'WikiTree ID.1', target_field_name + '.1'])
    # 'Father.1',
    # 'Mother.1'])

    son_third_generation_sf = son_third_generation_sf.rename(
        {'WikiTree ID.1': 'GreatGrandmother Grandmother mother WikiTree ID',
         target_field_name + '.1': 'GreatGrandmother Grandmother mother {0}'.format(target_field_name)})
    # 'Father.1': 'GreatGrandfather Grandmother mother',
    # 'Mother.1': 'GreatGrandmother Grandmother mother'})

    print("GreatGrandfather Grandmother mother's calculations were finished!")

    son_third_generation_sf.materialize()

    duration = time.time() - start_time
    return son_third_generation_sf, duration


# ## Create Sframe for child and ancestors and save them

# In[16]:


def save_child_selected_ancestor_sframe(sf, parental_relation_type):
    if not os.path.exists(output_path + parental_relation_type):
        os.makedirs(output_path + parental_relation_type)

    targeted_field_name = target_field_name.replace(" ", "_")
    sf.export_csv(output_path + parental_relation_type + "/wikitree_{0}_{1}.csv".format(targeted_field_name,
                                                                                        parental_relation_type))


def create_child_father_sframe_and_save(wikitree_sf, target_field_name, parental_relation_type):
    child_father_sf, child_father_sframe_time = create_child_father_sframe(wikitree_sf, target_field_name)
    save_child_selected_ancestor_sframe(child_father_sf, parental_relation_type)

    return child_father_sf, child_father_sframe_time


def create_child_grandfather_sframe_and_save(wikitree_sf, target_field_name, parental_relation_type):
    child_grandfather_sf, child_grandfather_sframe_time = create_child_grandfather_sframe(wikitree_sf,
                                                                                          target_field_name)
    save_child_selected_ancestor_sframe(child_grandfather_sf, parental_relation_type)

    return child_grandfather_sf, child_grandfather_sframe_time


def create_child_greatgrandfather_sframe_and_save(wikitree_sf, target_field_name, parental_relation_type):
    child_greatgrandfather_sf, child_greatgrandfather_sframe_time = create_child_greatgrandfather_sframe(wikitree_sf,
                                                                                                         target_field_name)
    save_child_selected_ancestor_sframe(child_greatgrandfather_sf, parental_relation_type)

    return child_greatgrandfather_sf, child_greatgrandfather_sframe_time


# In[17]:


def shorten_child_father_sframe_and_save(child_father_sf, target_field_name, parental_relation_type):
    if not os.path.exists(output_path + parental_relation_type):
        print(output_path + parental_relation_type)
        os.makedirs(output_path + parental_relation_type)

    targeted_field_name = target_field_name.replace(" ", "_")

    short_child_father_sf = child_father_sf[
        target_field_name, 'Father ' + target_field_name, 'Mother ' + target_field_name]
    short_child_father_sf.export_csv(
        output_path + parental_relation_type + "/short_wt_{0}_{1}.csv".format(targeted_field_name,
                                                                              parental_relation_type))

    return short_child_father_sf


def shorten_child_grandfather_sframe_and_save(child_grandfather_sf, target_field_name, parental_relation_type):
    if not os.path.exists(output_path + parental_relation_type):
        os.makedirs(output_path + parental_relation_type)

    targeted_field_name = target_field_name.replace(" ", "_")

    short_child_grandfather_sf = child_grandfather_sf[
        target_field_name, 'Father ' + target_field_name, 'Mother ' + target_field_name,
        'Grandfather father ' + target_field_name, 'Grandmother father ' + target_field_name,
        'Grandfather mother ' + target_field_name, 'Grandmother mother ' + target_field_name]

    short_child_grandfather_sf.export_csv(
        output_path + parental_relation_type + "/short_wikitree_{0}_{1}.csv".format(targeted_field_name,
                                                                                    parental_relation_type))

    return short_child_grandfather_sf


def shorten_child_greatgrandfather_sframe_and_save(child_greatgrandfather_sf, target_field_name,
                                                   parental_relation_type):
    if not os.path.exists(output_path + parental_relation_type):
        os.makedirs(output_path + parental_relation_type)

    targeted_field_name = target_field_name.replace(" ", "_")

    short_child_greatgrandfather_sf = child_greatgrandfather_sf[target_field_name,
                                                                'Father ' + target_field_name,
                                                                'Mother ' + target_field_name,
                                                                'Grandfather father ' + target_field_name,
                                                                'Grandmother father ' + target_field_name,
                                                                'Grandfather mother ' + target_field_name,
                                                                'Grandmother mother ' + target_field_name,

                                                                'GreatGrandfather Grandfather father ' + target_field_name,
                                                                'GreatGrandmother Grandfather father ' + target_field_name,
                                                                'GreatGrandfather Grandmother father ' + target_field_name,
                                                                'GreatGrandmother Grandmother father ' + target_field_name,

                                                                'GreatGrandfather Grandfather mother ' + target_field_name,
                                                                'GreatGrandmother Grandfather mother ' + target_field_name,
                                                                'GreatGrandfather Grandmother mother ' + target_field_name,
                                                                'GreatGrandmother Grandmother mother ' + target_field_name]

    short_child_greatgrandfather_sf.export_csv(
        output_path + parental_relation_type + "/short_wikitree_{0}_{1}.csv".format(targeted_field_name,
                                                                                    parental_relation_type))

    return short_child_greatgrandfather_sf


# ## Stack Names

# In[18]:


def clean_content(name):
    regex = re.compile('[^a-zA-Z]')
    # First parameter is the replacement, second parameter is your input string
    cleaned_name = regex.sub('', name)

    cleaned_name = cleaned_name.title()
    return cleaned_name


def stack_child_father(wikitree_sf, target_field_name):
    start_time = time.time()

    targeted_field_name = target_field_name.replace(" ", "_")

    wikitree_sf['Son_' + targeted_field_name] = wikitree_sf[target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['Father_' + targeted_field_name] = wikitree_sf["Father " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['Mother_' + targeted_field_name] = wikitree_sf["Mother " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['Child_' + targeted_field_name] = wikitree_sf['Son_' + targeted_field_name].apply(
        lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('Child_' + targeted_field_name, new_column_name='Child_' + targeted_field_name)

    wikitree_sf["Father_" + targeted_field_name] = wikitree_sf["Father_" + targeted_field_name].apply(
        lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('Father_' + targeted_field_name, new_column_name='Father_' + targeted_field_name)

    wikitree_sf["Mother_" + targeted_field_name] = wikitree_sf["Mother_" + targeted_field_name].apply(
        lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('Mother_' + targeted_field_name, new_column_name='Mother_' + targeted_field_name)

    wikitree_sf.materialize()

    duration = time.time() - start_time
    return wikitree_sf, duration


def stack_child_grandfather(wikitree_sf, target_field_name):
    start_time = time.time()

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

    duration = time.time() - start_time
    return wikitree_sf, duration


def stack_child_greatgrandfather(wikitree_sf, target_field_name):
    start_time = time.time()

    targeted_field_name = target_field_name.replace(" ", "_")

    wikitree_sf['Son_' + targeted_field_name] = wikitree_sf[target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['GreatGrandfather_Grandfather_father_' + targeted_field_name] = wikitree_sf[
        "GreatGrandfather Grandfather father " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['GreatGrandmother_Grandfather_father_' + targeted_field_name] = wikitree_sf[
        "GreatGrandmother Grandfather father " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['GreatGrandfather_Grandmother_father_' + targeted_field_name] = wikitree_sf[
        "GreatGrandfather Grandmother father " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['GreatGrandmother_Grandmother_father_' + targeted_field_name] = wikitree_sf[
        "GreatGrandmother Grandmother father " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['GreatGrandfather_Grandfather_mother_' + targeted_field_name] = wikitree_sf[
        "GreatGrandfather Grandfather mother " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['GreatGrandmother_Grandfather_mother_' + targeted_field_name] = wikitree_sf[
        "GreatGrandmother Grandfather mother " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['GreatGrandfather_Grandmother_mother_' + targeted_field_name] = wikitree_sf[
        "GreatGrandfather Grandmother mother " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['GreatGrandmother_Grandmother_mother_' + targeted_field_name] = wikitree_sf[
        "GreatGrandmother Grandmother mother " + target_field_name].apply(
        lambda x: [sub_name for sub_name in x.split(" ") if len(sub_name) > 1])

    wikitree_sf['Child_' + targeted_field_name] = wikitree_sf['Son_' + targeted_field_name].apply(
        lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('Child_' + targeted_field_name, new_column_name='Child_' + targeted_field_name)

    wikitree_sf["GreatGrandfather_Grandfather_father_" + targeted_field_name] = wikitree_sf[
        "GreatGrandfather_Grandfather_father_" + targeted_field_name].apply(
        lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('GreatGrandfather_Grandfather_father_' + targeted_field_name,
                                    new_column_name='GreatGrandfather_Grandfather_father_' + targeted_field_name)

    wikitree_sf["GreatGrandmother_Grandfather_father_" + targeted_field_name] = wikitree_sf[
        "GreatGrandmother_Grandfather_father_" + targeted_field_name].apply(
        lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('GreatGrandmother_Grandfather_father_' + targeted_field_name,
                                    new_column_name='GreatGrandmother_Grandfather_father_' + targeted_field_name)

    wikitree_sf["GreatGrandfather_Grandmother_father_" + targeted_field_name] = wikitree_sf[
        "GreatGrandfather_Grandmother_father_" + targeted_field_name].apply(
        lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('GreatGrandfather_Grandmother_father_' + targeted_field_name,
                                    new_column_name='GreatGrandfather_Grandmother_father_' + targeted_field_name)

    wikitree_sf["GreatGrandmother_Grandmother_father_" + targeted_field_name] = wikitree_sf[
        "GreatGrandmother_Grandmother_father_" + targeted_field_name].apply(
        lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('GreatGrandmother_Grandmother_father_' + targeted_field_name,
                                    new_column_name='GreatGrandmother_Grandmother_father_' + targeted_field_name)

    wikitree_sf["GreatGrandfather_Grandfather_mother_" + targeted_field_name] = wikitree_sf[
        "GreatGrandfather_Grandfather_mother_" + targeted_field_name].apply(
        lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('GreatGrandfather_Grandfather_mother_' + targeted_field_name,
                                    new_column_name='GreatGrandfather_Grandfather_mother_' + targeted_field_name)

    wikitree_sf["GreatGrandmother_Grandfather_mother_" + targeted_field_name] = wikitree_sf[
        "GreatGrandmother_Grandfather_mother_" + targeted_field_name].apply(
        lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('GreatGrandmother_Grandfather_mother_' + targeted_field_name,
                                    new_column_name='GreatGrandmother_Grandfather_mother_' + targeted_field_name)

    wikitree_sf["GreatGrandfather_Grandmother_mother_" + targeted_field_name] = wikitree_sf[
        "GreatGrandfather_Grandmother_mother_" + targeted_field_name].apply(
        lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('GreatGrandfather_Grandmother_mother_' + targeted_field_name,
                                    new_column_name='GreatGrandfather_Grandmother_mother_' + targeted_field_name)

    wikitree_sf["GreatGrandmother_Grandmother_mother_" + targeted_field_name] = wikitree_sf[
        "GreatGrandmother_Grandmother_mother_" + targeted_field_name].apply(
        lambda x: [clean_content(sub_name) for sub_name in x])
    wikitree_sf = wikitree_sf.stack('GreatGrandmother_Grandmother_mother_' + targeted_field_name,
                                    new_column_name='GreatGrandmother_Grandmother_mother_' + targeted_field_name)

    wikitree_sf.materialize()

    duration = time.time() - start_time
    return wikitree_sf, duration


def stack_child_father_and_save(shorten_child_father_sf, target_field_name, parental_relation_type):
    if not os.path.exists(output_path + parental_relation_type):
        os.makedirs(output_path + parental_relation_type)

    targeted_field_name = target_field_name.replace(" ", "_")

    child_father_stacked_sf, child_father_stacked_time = stack_child_father(shorten_child_father_sf,
                                                                            target_field_name)

    child_father_stacked_sf.export_csv(
        output_path + parental_relation_type + "/wikitree_{0}_{1}_stacked.csv".format(targeted_field_name,
                                                                                      parental_relation_type))

    short_child_father_stacked_sf = child_father_stacked_sf["Child_" + targeted_field_name,
                                                            'Father_' + targeted_field_name,
                                                            'Mother_' + targeted_field_name]

    short_child_father_stacked_sf.export_csv(
        output_path + parental_relation_type + "/short_wt_{0}_{1}_stacked.csv".format(targeted_field_name,
                                                                                      parental_relation_type))

    return short_child_father_stacked_sf, child_father_stacked_time


def stack_child_grandfather_and_save(shorten_child_grandfather_sf, target_field_name, parental_relation_type):
    if not os.path.exists(output_path + parental_relation_type):
        os.makedirs(output_path + parental_relation_type)

    targeted_field_name = target_field_name.replace(" ", "_")

    child_grandfather_stacked_sf, child_grandfather_stacked_time = stack_child_grandfather(shorten_child_grandfather_sf,
                                                                                           target_field_name)

    child_grandfather_stacked_sf.export_csv(
        output_path + "{0}/wikitree_{1}_{0}_stacked.csv".format(parental_relation_type,
                                                                targeted_field_name))

    short_child_grandfather_stacked_sf = child_grandfather_stacked_sf["Child_" + targeted_field_name,
                                                                      'Grandfather_father_' + targeted_field_name,
                                                                      'Grandmother_father_' + targeted_field_name,
                                                                      'Grandfather_mother_' + targeted_field_name,
                                                                      'Grandmother_mother_' + targeted_field_name]

    short_child_grandfather_stacked_sf.export_csv(
        output_path + parental_relation_type + "/short_wt_{0}_{1}_stacked.csv".format(targeted_field_name,
                                                                                      parental_relation_type))

    return short_child_grandfather_stacked_sf, child_grandfather_stacked_time


def stack_child_greatgrandfather_and_save(shorten_child_greatgrandfather_sf, target_field_name, parental_relation_type):
    if not os.path.exists(output_path + parental_relation_type):
        os.makedirs(output_path + parental_relation_type)

    targeted_field_name = target_field_name.replace(" ", "_")

    child_greatgrandfather_stacked_sf, child_greatgrandfather_stacked_time = stack_child_greatgrandfather(
        shorten_child_greatgrandfather_sf,
        target_field_name)

    child_greatgrandfather_stacked_sf.export_csv(
        output_path + "{0}/wikitree_{1}_{0}_stacked.csv".format(parental_relation_type,
                                                                targeted_field_name))

    short_child_greatgrandfather_stacked_sf = child_greatgrandfather_stacked_sf["Child_" + targeted_field_name,
                                                                                'GreatGrandfather_Grandfather_father_' + targeted_field_name,
                                                                                'GreatGrandmother_Grandfather_father_' + targeted_field_name,
                                                                                'GreatGrandfather_Grandmother_father_' + targeted_field_name,
                                                                                'GreatGrandmother_Grandmother_father_' + targeted_field_name,
                                                                                'GreatGrandfather_Grandfather_mother_' + targeted_field_name,
                                                                                'GreatGrandmother_Grandfather_mother_' + targeted_field_name,
                                                                                'GreatGrandfather_Grandmother_mother_' + targeted_field_name,
                                                                                'GreatGrandmother_Grandmother_mother_' + targeted_field_name,
    ]

    short_child_greatgrandfather_stacked_sf.export_csv(
        output_path + parental_relation_type + "/short_wt_{0}_{1}_stacked.csv".format(targeted_field_name,
                                                                                      parental_relation_type))

    return short_child_greatgrandfather_stacked_sf, child_greatgrandfather_stacked_time


# ## Clean Prefixes - Unnessary Names

# In[19]:


def clean_prefix_names_child_father(sf, target_field_name, parental_relation_type, prefix_names):
    start_time = time.time()

    targeted_field_name = target_field_name.replace(" ", "_")

    sf = sf.filter_by(prefix_names, 'Child_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'Father_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'Mother_' + targeted_field_name, exclude=True)

    sf.materialize()

    duration = time.time() - start_time

    sf.export_csv(output_path + parental_relation_type + "/wt_{0}_{1}_stacked_no_prefix.csv".format(targeted_field_name,
                                                                                                    parental_relation_type))

    return sf, duration


def clean_prefix_names_child_grandfather(sf, target_field_name, parental_relation_type, prefix_names):
    start_time = time.time()

    targeted_field_name = target_field_name.replace(" ", "_")

    sf = sf.filter_by(prefix_names, 'Child_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'Grandfather_father_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'Grandmother_father_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'Grandfather_mother_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'Grandmother_mother_' + targeted_field_name, exclude=True)

    sf.materialize()

    duration = time.time() - start_time

    sf.export_csv(output_path + parental_relation_type + "/wt_{0}_{1}_stacked_no_prefix.csv".format(targeted_field_name,
                                                                                                    parental_relation_type))

    return sf, duration


def clean_prefix_names_child_greatgrandfather(sf, target_field_name, parental_relation_type, prefix_names):
    start_time = time.time()
    print(sf)
    targeted_field_name = target_field_name.replace(" ", "_")

    sf = sf.filter_by(prefix_names, 'Child_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'GreatGrandfather_Grandfather_father_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'GreatGrandmother_Grandfather_father_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'GreatGrandfather_Grandmother_father_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'GreatGrandmother_Grandmother_father_' + targeted_field_name, exclude=True)

    sf = sf.filter_by(prefix_names, 'GreatGrandfather_Grandfather_mother_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'GreatGrandmother_Grandfather_mother_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'GreatGrandfather_Grandmother_mother_' + targeted_field_name, exclude=True)
    sf = sf.filter_by(prefix_names, 'GreatGrandmother_Grandmother_mother_' + targeted_field_name, exclude=True)

    sf.materialize()

    duration = time.time() - start_time

    sf.export_csv(output_path + parental_relation_type + "/wt_{0}_{1}_stacked_no_prefix.csv".format(targeted_field_name,
                                                                                                    parental_relation_type))

    return sf, duration


# In[20]:


def calculate_edit_distance(name1, name2):
    if not name1 or not name2:
        return -1

    name1 = name1.lower()
    name2 = name2.lower()

    edit_dist = editdistance.eval(name1, name2)

    return edit_dist


# In[21]:


def calculate_child_father_ed(wikitree_sf, target_field_name, parental_relation_type):
    start_time = time.time()

    targeted_field_name = target_field_name.replace(" ", "_")

    print("Calculating Edit Distance")

    wikitree_sf['Edit_Distance_Child_Father'] = wikitree_sf.apply(
        lambda x: calculate_edit_distance(x["Child_" + targeted_field_name], x["Father_" + targeted_field_name]))

    wikitree_sf['Edit_Distance_Child_Mother'] = wikitree_sf.apply(
        lambda x: calculate_edit_distance(x["Child_" + targeted_field_name], x["Mother_" + targeted_field_name]))

    wikitree_sf.materialize()

    duration = time.time() - start_time

    wikitree_sf.export_csv(
        output_path + parental_relation_type + "/wt_{0}_{1}_stacked_no_prefix_ed.csv".format(targeted_field_name,
                                                                                             parental_relation_type))
    return wikitree_sf, duration


def calculate_child_grandfather_ed(wikitree_sf, target_field_name, parental_relation_type):
    start_time = time.time()

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

    duration = time.time() - start_time

    wikitree_sf.export_csv(
        output_path + parental_relation_type + "/wt_{0}_{1}_stacked_no_prefix_ed.csv".format(targeted_field_name,
                                                                                             parental_relation_type))
    return wikitree_sf, duration


def calculate_child_greatgrandfather_ed(wikitree_sf, target_field_name, parental_relation_type):
    start_time = time.time()

    targeted_field_name = target_field_name.replace(" ", "_")

    print("Calculating Edit Distance")

    wikitree_sf['Edit_Distance_Child_GreatGrandfather_Grandfather_father'] = wikitree_sf.apply(
        lambda x: calculate_edit_distance(x["Child_" + targeted_field_name],
                                          x["GreatGrandfather_Grandfather_father_" + targeted_field_name]))

    wikitree_sf['Edit_Distance_Child_GreatGrandmother_Grandfather_father'] = wikitree_sf.apply(
        lambda x: calculate_edit_distance(x["Child_" + targeted_field_name],
                                          x["GreatGrandmother_Grandfather_father_" + targeted_field_name]))

    wikitree_sf['Edit_Distance_Child_GreatGrandfather_Grandmother_father'] = wikitree_sf.apply(
        lambda x: calculate_edit_distance(x["Child_" + targeted_field_name],
                                          x["GreatGrandfather_Grandmother_father_" + targeted_field_name]))

    wikitree_sf['Edit_Distance_Child_GreatGrandmother_Grandmother_father'] = wikitree_sf.apply(
        lambda x: calculate_edit_distance(x["Child_" + targeted_field_name],
                                          x["GreatGrandmother_Grandmother_father_" + targeted_field_name]))

    wikitree_sf['Edit_Distance_Child_GreatGrandfather_Grandfather_mother'] = wikitree_sf.apply(
        lambda x: calculate_edit_distance(x["Child_" + targeted_field_name],
                                          x["GreatGrandfather_Grandfather_mother_" + targeted_field_name]))

    wikitree_sf['Edit_Distance_Child_GreatGrandmother_Grandfather_mother'] = wikitree_sf.apply(
        lambda x: calculate_edit_distance(x["Child_" + targeted_field_name],
                                          x["GreatGrandmother_Grandfather_mother_" + targeted_field_name]))

    wikitree_sf['Edit_Distance_Child_GreatGrandfather_Grandmother_mother'] = wikitree_sf.apply(
        lambda x: calculate_edit_distance(x["Child_" + targeted_field_name],
                                          x["GreatGrandfather_Grandmother_mother_" + targeted_field_name]))

    wikitree_sf['Edit_Distance_Child_GreatGrandmother_Grandmother_mother'] = wikitree_sf.apply(
        lambda x: calculate_edit_distance(x["Child_" + targeted_field_name],
                                          x["GreatGrandmother_Grandmother_mother_" + targeted_field_name]))

    wikitree_sf.materialize()

    duration = time.time() - start_time

    wikitree_sf.export_csv(
        output_path + parental_relation_type + "/wt_{0}_{1}_stacked_no_prefix_ed.csv".format(targeted_field_name,
                                                                                             parental_relation_type))
    return wikitree_sf, duration


# ## Filtering num of characters for each name

# In[22]:


def filter_higher_n_chars_child_father(wikitree_sf, target_field_name, parental_relation_type, min_chars_count):
    if not os.path.exists(output_path + parental_relation_type + "/geq_{0}_chars".format(min_chars_count)):
        os.makedirs(output_path + parental_relation_type + "/geq_{0}_chars".format(min_chars_count))

    start_time = time.time()
    targeted_field_name = target_field_name.replace(" ", "_")

    wikitree_sf = wikitree_sf[(wikitree_sf["Child_" + targeted_field_name] != "") &
                              (wikitree_sf["Father_" + targeted_field_name] != "") &
                              (wikitree_sf["Mother_" + targeted_field_name] != "")
                              ]

    wikitree_sf = wikitree_sf.dropna()

    wikitree_sf['Child_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["Child_" + targeted_field_name]))

    wikitree_sf['Father_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["Father_" + targeted_field_name]))

    wikitree_sf['Mother_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["Mother_" + targeted_field_name]))

    wikitree_sf = wikitree_sf[(wikitree_sf['Child_{0}_Num_Chars'.format(targeted_field_name)] >= min_chars_count) &
                              (wikitree_sf['Father_{0}_Num_Chars'.format(targeted_field_name)] >= min_chars_count) &
                              (wikitree_sf['Mother_{0}_Num_Chars'.format(targeted_field_name)] >= min_chars_count)
                              ]

    wikitree_sf.materialize()

    duration = time.time() - start_time

    wikitree_sf.export_csv(output_path + parental_relation_type +
                           "/geq_{0}_chars/wt_{1}_{2}_stacked_no_prefix_ed_geq_{0}_chars.csv".format(min_chars_count,
                                                                                                     targeted_field_name,
                                                                                                     parental_relation_type))
    return wikitree_sf, duration


def filter_higher_n_chars_child_grandfather(wikitree_sf, target_field_name, parental_relation_type, min_chars_count):
    if not os.path.exists(output_path + parental_relation_type + "/geq_{0}_chars".format(min_chars_count)):
        os.makedirs(output_path + parental_relation_type + "/geq_{0}_chars".format(min_chars_count))

    start_time = time.time()
    targeted_field_name = target_field_name.replace(" ", "_")

    wikitree_sf = wikitree_sf[(wikitree_sf["Child_" + targeted_field_name] != "") &
                              (wikitree_sf["Grandfather_father_" + targeted_field_name] != "") &
                              (wikitree_sf["Grandmother_father_" + targeted_field_name] != "") &
                              (wikitree_sf["Grandfather_mother_" + targeted_field_name] != "") &
                              (wikitree_sf["Grandmother_mother_" + targeted_field_name] != "")]

    wikitree_sf = wikitree_sf.dropna()

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

    wikitree_sf = wikitree_sf[(wikitree_sf['Child_{0}_Num_Chars'.format(targeted_field_name)] >= min_chars_count) &
                              (wikitree_sf[
                                   'Grandfather_father_{0}_Num_Chars'.format(targeted_field_name)] >= min_chars_count) &
                              (wikitree_sf[
                                   'Grandmother_father_{0}_Num_Chars'.format(targeted_field_name)] >= min_chars_count) &
                              (wikitree_sf[
                                   'Grandfather_mother_{0}_Num_Chars'.format(targeted_field_name)] >= min_chars_count) &
                              (wikitree_sf[
                                   'Grandmother_mother_{0}_Num_Chars'.format(targeted_field_name)] >= min_chars_count)
                              ]

    wikitree_sf.materialize()

    duration = time.time() - start_time

    wikitree_sf.export_csv(output_path + parental_relation_type +
                           "/geq_{0}_chars/wt_{1}_{2}_stacked_no_prefix_ed_geq_{0}_chars.csv".format(min_chars_count,
                                                                                                     targeted_field_name,
                                                                                                     parental_relation_type))
    return wikitree_sf, duration


def filter_higher_n_chars_child_greatgrandfather(wikitree_sf, target_field_name, parental_relation_type,
                                                 min_chars_count):
    if not os.path.exists(output_path + parental_relation_type + "/geq_{0}_chars".format(min_chars_count)):
        os.makedirs(output_path + parental_relation_type + "/geq_{0}_chars".format(min_chars_count))

    start_time = time.time()
    targeted_field_name = target_field_name.replace(" ", "_")

    wikitree_sf = wikitree_sf[(wikitree_sf["Child_" + targeted_field_name] != "") &
                              (wikitree_sf["GreatGrandfather_Grandfather_father_" + targeted_field_name] != "") &
                              (wikitree_sf["GreatGrandmother_Grandfather_father_" + targeted_field_name] != "") &
                              (wikitree_sf["GreatGrandfather_Grandmother_father_" + targeted_field_name] != "") &
                              (wikitree_sf["GreatGrandmother_Grandmother_father_" + targeted_field_name] != "") &
                              (wikitree_sf["GreatGrandfather_Grandfather_mother_" + targeted_field_name] != "") &
                              (wikitree_sf["GreatGrandmother_Grandfather_mother_" + targeted_field_name] != "") &
                              (wikitree_sf["GreatGrandfather_Grandmother_mother_" + targeted_field_name] != "") &
                              (wikitree_sf["GreatGrandmother_Grandmother_mother_" + targeted_field_name] != "")
                              ]

    wikitree_sf = wikitree_sf.dropna()

    wikitree_sf['Child_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["Child_" + targeted_field_name]))

    wikitree_sf['GreatGrandfather_Grandfather_father_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["GreatGrandfather_Grandfather_father_" + targeted_field_name]))

    wikitree_sf['GreatGrandmother_Grandfather_father_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["GreatGrandmother_Grandfather_father_" + targeted_field_name]))

    wikitree_sf['GreatGrandfather_Grandmother_father_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["GreatGrandfather_Grandmother_father_" + targeted_field_name]))

    wikitree_sf['GreatGrandmother_Grandmother_father_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["GreatGrandmother_Grandmother_father_" + targeted_field_name]))

    wikitree_sf['GreatGrandfather_Grandfather_mother_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["GreatGrandfather_Grandfather_mother_" + targeted_field_name]))

    wikitree_sf['GreatGrandmother_Grandfather_mother_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["GreatGrandmother_Grandfather_mother_" + targeted_field_name]))

    wikitree_sf['GreatGrandfather_Grandmother_mother_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["GreatGrandfather_Grandmother_mother_" + targeted_field_name]))

    wikitree_sf['GreatGrandmother_Grandmother_mother_{0}_Num_Chars'.format(targeted_field_name)] = wikitree_sf.apply(
        lambda x: len(x["GreatGrandmother_Grandmother_mother_" + targeted_field_name]))

    wikitree_sf = wikitree_sf[(wikitree_sf['Child_{0}_Num_Chars'.format(targeted_field_name)] >= min_chars_count) &
                              (wikitree_sf['GreatGrandfather_Grandfather_father_{0}_Num_Chars'.format(
                                  targeted_field_name)] >= min_chars_count) &
                              (wikitree_sf['GreatGrandmother_Grandfather_father_{0}_Num_Chars'.format(
                                  targeted_field_name)] >= min_chars_count) &
                              (wikitree_sf['GreatGrandfather_Grandmother_father_{0}_Num_Chars'.format(
                                  targeted_field_name)] >= min_chars_count) &
                              (wikitree_sf['GreatGrandmother_Grandmother_father_{0}_Num_Chars'.format(
                                  targeted_field_name)] >= min_chars_count) &

                              (wikitree_sf['GreatGrandfather_Grandfather_mother_{0}_Num_Chars'.format(
                                  targeted_field_name)] >= min_chars_count) &
                              (wikitree_sf['GreatGrandmother_Grandfather_mother_{0}_Num_Chars'.format(
                                  targeted_field_name)] >= min_chars_count) &
                              (wikitree_sf['GreatGrandfather_Grandmother_mother_{0}_Num_Chars'.format(
                                  targeted_field_name)] >= min_chars_count) &
                              (wikitree_sf['GreatGrandmother_Grandmother_mother_{0}_Num_Chars'.format(
                                  targeted_field_name)] >= min_chars_count)
                              ]

    wikitree_sf.materialize()

    duration = time.time() - start_time

    wikitree_sf.export_csv(output_path + parental_relation_type +
                           "/geq_{0}_chars/wt_{1}_{2}_stacked_no_prefix_ed_geq_{0}_chars.csv".format(min_chars_count,
                                                                                                     targeted_field_name,
                                                                                                     parental_relation_type))
    return wikitree_sf, duration


# ## Filter by Edit Distance

# In[23]:


def filter_by_ed_child_father(wikitree_sf, target_field_name, parental_relation_type, min_chars_count,
                              max_edit_distance):
    new_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}".format(min_chars_count,
                                                                                       max_edit_distance)
    if not os.path.exists(new_path):
        os.makedirs(new_path)

    targeted_field_name = target_field_name.replace(" ", "_")
    min_edit_distance = 1

    start_time = time.time()

    child_father_ed_sf = wikitree_sf[
        ((wikitree_sf['Edit_Distance_Child_Father'] >= min_edit_distance) &
         (wikitree_sf['Edit_Distance_Child_Father'] <= max_edit_distance))]

    child_father_ed_sf.materialize()

    child_mother_ed_sf = wikitree_sf[
        ((wikitree_sf['Edit_Distance_Child_Mother'] >= min_edit_distance) &
         (wikitree_sf['Edit_Distance_Child_Mother'] <= max_edit_distance))]

    child_mother_ed_sf.materialize()

    duration = time.time() - start_time

    child_father_ed_sf.export_csv(output_path + parental_relation_type +
                                  "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_child_father_ED_1_{1}.csv".format(
                                      min_chars_count,
                                      max_edit_distance,
                                      targeted_field_name,
                                      parental_relation_type))

    child_mother_ed_sf.export_csv(output_path + parental_relation_type +
                                  "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_child_mother_ED_1_{1}.csv".format(
                                      min_chars_count,
                                      max_edit_distance,
                                      targeted_field_name,
                                      parental_relation_type))

    return child_father_ed_sf, child_mother_ed_sf, duration


def filter_by_ed_child_grandfather(wikitree_sf, target_field_name, parental_relation_type, min_chars_count,
                                   max_edit_distance):
    new_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}".format(min_chars_count,
                                                                                       max_edit_distance)
    if not os.path.exists(new_path):
        os.makedirs(new_path)

    targeted_field_name = target_field_name.replace(" ", "_")
    min_edit_distance = 1

    start_time = time.time()

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

    duration = time.time() - start_time

    child_grandfather_father_ed_sf.export_csv(output_path + parental_relation_type +
                                              "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_child_grandfather_father_ED_1_{1}.csv".format(
                                                  min_chars_count,
                                                  max_edit_distance,
                                                  targeted_field_name,
                                                  parental_relation_type))

    child_grandmother_father_ed_sf.export_csv(output_path + parental_relation_type +
                                              "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_child_grandmother_father_ED_1_{1}.csv".format(
                                                  min_chars_count,
                                                  max_edit_distance,
                                                  targeted_field_name,
                                                  parental_relation_type))

    child_grandfather_mother_ed_sf.export_csv(output_path + parental_relation_type +
                                              "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_child_grandfather_mother_ED_1_{1}.csv".format(
                                                  min_chars_count,
                                                  max_edit_distance,
                                                  targeted_field_name,
                                                  parental_relation_type))

    child_grandmother_mother_ed_sf.export_csv(output_path + parental_relation_type +
                                              "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_child_grandmother_mother_ED_1_{1}.csv".format(
                                                  min_chars_count,
                                                  max_edit_distance,
                                                  targeted_field_name,
                                                  parental_relation_type))

    return child_grandfather_father_ed_sf, child_grandmother_father_ed_sf, child_grandfather_mother_ed_sf, child_grandmother_mother_ed_sf, duration


def filter_by_ed_child_greatgrandfather(wikitree_sf, target_field_name, parental_relation_type, min_chars_count,
                                        max_edit_distance):
    new_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}".format(min_chars_count,
                                                                                       max_edit_distance)
    if not os.path.exists(new_path):
        os.makedirs(new_path)

    targeted_field_name = target_field_name.replace(" ", "_")
    min_edit_distance = 1

    start_time = time.time()

    child_greatgrandfather_grandfather_father_ed_sf = wikitree_sf[
        ((wikitree_sf['Edit_Distance_Child_GreatGrandfather_Grandfather_father'] >= min_edit_distance) &
         (wikitree_sf['Edit_Distance_Child_GreatGrandfather_Grandfather_father'] <= max_edit_distance))]
    child_greatgrandfather_grandfather_father_ed_sf.materialize()

    child_greatgrandmother_grandfather_father_ed_sf = wikitree_sf[
        ((wikitree_sf['Edit_Distance_Child_GreatGrandmother_Grandfather_father'] >= min_edit_distance) &
         (wikitree_sf['Edit_Distance_Child_GreatGrandmother_Grandfather_father'] <= max_edit_distance))]
    child_greatgrandmother_grandfather_father_ed_sf.materialize()

    child_greatgrandfather_grandmother_father_ed_sf = wikitree_sf[
        ((wikitree_sf['Edit_Distance_Child_GreatGrandfather_Grandmother_father'] >= min_edit_distance) &
         (wikitree_sf['Edit_Distance_Child_GreatGrandfather_Grandmother_father'] <= max_edit_distance))]
    child_greatgrandfather_grandmother_father_ed_sf.materialize()

    child_greatgrandmother_grandmother_father_ed_sf = wikitree_sf[
        ((wikitree_sf['Edit_Distance_Child_GreatGrandmother_Grandmother_father'] >= min_edit_distance) &
         (wikitree_sf['Edit_Distance_Child_GreatGrandmother_Grandmother_father'] <= max_edit_distance))]
    child_greatgrandmother_grandmother_father_ed_sf.materialize()

    child_greatgrandfather_grandfather_mother_ed_sf = wikitree_sf[
        ((wikitree_sf['Edit_Distance_Child_GreatGrandfather_Grandfather_mother'] >= min_edit_distance) &
         (wikitree_sf['Edit_Distance_Child_GreatGrandfather_Grandfather_mother'] <= max_edit_distance))]
    child_greatgrandfather_grandfather_mother_ed_sf.materialize()

    child_greatgrandmother_grandfather_mother_ed_sf = wikitree_sf[
        ((wikitree_sf['Edit_Distance_Child_GreatGrandmother_Grandfather_mother'] >= min_edit_distance) &
         (wikitree_sf['Edit_Distance_Child_GreatGrandmother_Grandfather_mother'] <= max_edit_distance))]
    child_greatgrandmother_grandfather_mother_ed_sf.materialize()

    child_greatgrandfather_grandmother_mother_ed_sf = wikitree_sf[
        ((wikitree_sf['Edit_Distance_Child_GreatGrandfather_Grandmother_mother'] >= min_edit_distance) &
         (wikitree_sf['Edit_Distance_Child_GreatGrandfather_Grandmother_mother'] <= max_edit_distance))]
    child_greatgrandfather_grandmother_mother_ed_sf.materialize()

    child_greatgrandmother_grandmother_mother_ed_sf = wikitree_sf[
        ((wikitree_sf['Edit_Distance_Child_GreatGrandmother_Grandmother_mother'] >= min_edit_distance) &
         (wikitree_sf['Edit_Distance_Child_GreatGrandmother_Grandmother_mother'] <= max_edit_distance))]
    child_greatgrandmother_grandmother_mother_ed_sf.materialize()

    duration = time.time() - start_time

    child_greatgrandfather_grandfather_father_ed_sf.export_csv(output_path + parental_relation_type +
                                                               "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_child_greatgrandfather_grandfather_father_ED_1_{1}.csv".format(
                                                                   min_chars_count,
                                                                   max_edit_distance,
                                                                   targeted_field_name,
                                                                   parental_relation_type))

    child_greatgrandmother_grandfather_father_ed_sf.export_csv(output_path + parental_relation_type +
                                                               "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_child_greatgrandmother_grandfather_father_ED_1_{1}.csv".format(
                                                                   min_chars_count,
                                                                   max_edit_distance,
                                                                   targeted_field_name,
                                                                   parental_relation_type))

    child_greatgrandfather_grandmother_father_ed_sf.export_csv(output_path + parental_relation_type +
                                                               "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_child_greatgrandfather_grandmother_father_ED_1_{1}.csv".format(
                                                                   min_chars_count,
                                                                   max_edit_distance,
                                                                   targeted_field_name,
                                                                   parental_relation_type))

    child_greatgrandmother_grandmother_father_ed_sf.export_csv(output_path + parental_relation_type +
                                                               "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_child_greatgrandmother_grandmother_father_ED_1_{1}.csv".format(
                                                                   min_chars_count,
                                                                   max_edit_distance,
                                                                   targeted_field_name,
                                                                   parental_relation_type))

    child_greatgrandfather_grandfather_mother_ed_sf.export_csv(output_path + parental_relation_type +
                                                               "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_child_greatgrandfather_grandfather_mother_ED_1_{1}.csv".format(
                                                                   min_chars_count,
                                                                   max_edit_distance,
                                                                   targeted_field_name,
                                                                   parental_relation_type))

    child_greatgrandmother_grandfather_mother_ed_sf.export_csv(output_path + parental_relation_type +
                                                               "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_child_greatgrandmother_grandfather_mother_ED_1_{1}.csv".format(
                                                                   min_chars_count,
                                                                   max_edit_distance,
                                                                   targeted_field_name,
                                                                   parental_relation_type))

    child_greatgrandfather_grandmother_mother_ed_sf.export_csv(output_path + parental_relation_type +
                                                               "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_child_greatgrandfather_grandmother_mother_ED_1_{1}.csv".format(
                                                                   min_chars_count,
                                                                   max_edit_distance,
                                                                   targeted_field_name,
                                                                   parental_relation_type))

    child_greatgrandmother_grandmother_mother_ed_sf.export_csv(output_path + parental_relation_type +
                                                               "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_child_greatgrandmother_grandmother_mother_ED_1_{1}.csv".format(
                                                                   min_chars_count,
                                                                   max_edit_distance,
                                                                   targeted_field_name,
                                                                   parental_relation_type))

    return child_greatgrandfather_grandfather_father_ed_sf, child_greatgrandmother_grandfather_father_ed_sf, child_greatgrandfather_grandmother_father_ed_sf, child_greatgrandmother_grandmother_father_ed_sf, child_greatgrandfather_grandfather_mother_ed_sf, child_greatgrandmother_grandfather_mother_ed_sf, child_greatgrandfather_grandmother_mother_ed_sf, child_greatgrandmother_grandmother_mother_ed_sf, duration


# ## Group by

# In[24]:


def group_by_child_father(child_father_ed_sf,
                          child_mother_ed_sf,
                          target_field_name,
                          parental_relation_type,
                          min_chars_count,
                          max_edit_distance):
    new_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}".format(min_chars_count,
                                                                                       max_edit_distance)
    if not os.path.exists(new_path):
        os.makedirs(new_path)

    targeted_field_name = target_field_name.replace(" ", "_")
    start_time = time.time()

    child_father_sf = child_father_ed_sf.groupby(
        key_column_names=['Child_' + targeted_field_name, 'Father_' + targeted_field_name,
                          'Edit_Distance_Child_Father'], operations={'count': agg.COUNT()})
    child_father_sf = child_father_sf.sort(['count'], ascending=False)
    child_father_sf = child_father_sf.select_columns(['Child_' + targeted_field_name,
                                                      'Father_' + targeted_field_name,
                                                      'Edit_Distance_Child_Father', 'count'])

    child_father_sf.materialize()

    child_mother_sf = child_mother_ed_sf.groupby(
        key_column_names=['Child_' + targeted_field_name, 'Mother_' + targeted_field_name,
                          'Edit_Distance_Child_Mother'], operations={'count': agg.COUNT()})
    child_mother_sf = child_mother_sf.sort(['count'], ascending=False)
    child_mother_sf = child_mother_sf.select_columns(['Child_' + targeted_field_name,
                                                      'Mother_' + targeted_field_name,
                                                      'Edit_Distance_Child_Mother', 'count'])

    child_mother_sf.materialize()

    duration = time.time() - start_time

    child_father_sf.export_csv(output_path + parental_relation_type +
                               "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_father_count.csv".format(
                                   min_chars_count,
                                   max_edit_distance,
                                   targeted_field_name,
                                   parental_relation_type))
    child_mother_sf.export_csv(output_path + parental_relation_type +
                               "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_mother_count.csv".format(
                                   min_chars_count,
                                   max_edit_distance,
                                   targeted_field_name,
                                   parental_relation_type))

    return child_father_sf, child_mother_sf, duration


def group_by_child_grandfather(child_grandfather_father_ed_sf,
                               child_grandmother_father_ed_sf,
                               child_grandfather_mother_ed_sf,
                               child_grandmother_mother_ed_sf,
                               target_field_name,
                               parental_relation_type,
                               min_chars_count,
                               max_edit_distance):
    targeted_field_name = target_field_name.replace(" ", "_")
    start_time = time.time()

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

    duration = time.time() - start_time

    child_grandfather_father_sf.export_csv(output_path + parental_relation_type +
                                           "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_grandfather_father_count.csv".format(
                                               min_chars_count,
                                               max_edit_distance,
                                               targeted_field_name,
                                               parental_relation_type))
    child_grandmother_father_sf.export_csv(output_path + parental_relation_type +
                                           "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_grandmother_father_count.csv".format(
                                               min_chars_count,
                                               max_edit_distance,
                                               targeted_field_name,
                                               parental_relation_type))

    child_grandfather_mother_sf.export_csv(output_path + parental_relation_type +
                                           "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_grandfather_mother_count.csv".format(
                                               min_chars_count,
                                               max_edit_distance,
                                               targeted_field_name,
                                               parental_relation_type))
    child_grandmother_mother_sf.export_csv(output_path + parental_relation_type +
                                           "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_grandmother_mother_count.csv".format(
                                               min_chars_count,
                                               max_edit_distance,
                                               targeted_field_name,
                                               parental_relation_type))

    return child_grandfather_father_sf, child_grandmother_father_sf, child_grandfather_mother_sf, child_grandmother_mother_sf, duration


def group_by_child_greatgrandfather(child_greatgrandfather_grandfather_father_ed_sf,
                                    child_greatgrandmother_grandfather_father_ed_sf,
                                    child_greatgrandfather_grandmother_father_ed_sf,
                                    child_greatgrandmother_grandmother_father_ed_sf,
                                    child_greatgrandfather_grandfather_mother_ed_sf,
                                    child_greatgrandmother_grandfather_mother_ed_sf,
                                    child_greatgrandfather_grandmother_mother_ed_sf,
                                    child_greatgrandmother_grandmother_mother_ed_sf,
                                    target_field_name,
                                    parental_relation_type,
                                    min_chars_count,
                                    max_edit_distance):
    targeted_field_name = target_field_name.replace(" ", "_")

    start_time = time.time()

    child_greatgrandfather_grandfather_father_sf = child_greatgrandfather_grandfather_father_ed_sf.groupby(
        key_column_names=['Child_' + targeted_field_name, 'GreatGrandfather_Grandfather_father_' + targeted_field_name,
                          'Edit_Distance_Child_GreatGrandfather_Grandfather_father'], operations={'count': agg.COUNT()})
    child_greatgrandfather_grandfather_father_sf = child_greatgrandfather_grandfather_father_sf.sort(['count'],
                                                                                                     ascending=False)
    child_greatgrandfather_grandfather_father_sf = child_greatgrandfather_grandfather_father_sf.select_columns(
        ['Child_' + targeted_field_name,
         'GreatGrandfather_Grandfather_father_' + targeted_field_name,
         'Edit_Distance_Child_GreatGrandfather_Grandfather_father', 'count'])

    child_greatgrandfather_grandfather_father_sf.materialize()

    child_greatgrandmother_grandfather_father_sf = child_greatgrandmother_grandfather_father_ed_sf.groupby(
        key_column_names=['Child_' + targeted_field_name, 'GreatGrandmother_Grandfather_father_' + targeted_field_name,
                          'Edit_Distance_Child_GreatGrandmother_Grandfather_father'], operations={'count': agg.COUNT()})
    child_greatgrandmother_grandfather_father_sf = child_greatgrandmother_grandfather_father_sf.sort(['count'],
                                                                                                     ascending=False)
    child_greatgrandmother_grandfather_father_sf = child_greatgrandmother_grandfather_father_sf.select_columns(
        ['Child_' + targeted_field_name,
         'GreatGrandmother_Grandfather_father_' + targeted_field_name,
         'Edit_Distance_Child_GreatGrandmother_Grandfather_father', 'count'])

    child_greatgrandmother_grandfather_father_sf.materialize()

    child_greatgrandfather_grandmother_father_sf = child_greatgrandfather_grandmother_father_ed_sf.groupby(
        key_column_names=['Child_' + targeted_field_name, 'GreatGrandfather_Grandmother_father_' + targeted_field_name,
                          'Edit_Distance_Child_GreatGrandfather_Grandmother_father'], operations={'count': agg.COUNT()})
    child_greatgrandfather_grandmother_father_sf = child_greatgrandfather_grandmother_father_sf.sort(['count'],
                                                                                                     ascending=False)
    child_greatgrandfather_grandmother_father_sf = child_greatgrandfather_grandmother_father_sf.select_columns(
        ['Child_' + targeted_field_name,
         'GreatGrandfather_Grandmother_father_' + targeted_field_name,
         'Edit_Distance_Child_GreatGrandfather_Grandmother_father', 'count'])

    child_greatgrandfather_grandmother_father_sf.materialize()

    child_greatgrandmother_grandmother_father_sf = child_greatgrandmother_grandmother_father_ed_sf.groupby(
        key_column_names=['Child_' + targeted_field_name, 'GreatGrandmother_Grandmother_father_' + targeted_field_name,
                          'Edit_Distance_Child_GreatGrandmother_Grandmother_father'], operations={'count': agg.COUNT()})
    child_greatgrandmother_grandmother_father_sf = child_greatgrandmother_grandmother_father_sf.sort(['count'],
                                                                                                     ascending=False)
    child_greatgrandmother_grandmother_father_sf = child_greatgrandmother_grandmother_father_sf.select_columns(
        ['Child_' + targeted_field_name,
         'GreatGrandmother_Grandmother_father_' + targeted_field_name,
         'Edit_Distance_Child_GreatGrandmother_Grandmother_father', 'count'])

    child_greatgrandmother_grandmother_father_sf.materialize()

    ########################################################3

    child_greatgrandfather_grandfather_mother_sf = child_greatgrandfather_grandfather_mother_ed_sf.groupby(
        key_column_names=['Child_' + targeted_field_name, 'GreatGrandfather_Grandfather_mother_' + targeted_field_name,
                          'Edit_Distance_Child_GreatGrandfather_Grandfather_mother'], operations={'count': agg.COUNT()})
    child_greatgrandfather_grandfather_mother_sf = child_greatgrandfather_grandfather_mother_sf.sort(['count'],
                                                                                                     ascending=False)
    child_greatgrandfather_grandfather_mother_sf = child_greatgrandfather_grandfather_mother_sf.select_columns(
        ['Child_' + targeted_field_name,
         'GreatGrandfather_Grandfather_mother_' + targeted_field_name,
         'Edit_Distance_Child_GreatGrandfather_Grandfather_mother', 'count'])

    child_greatgrandfather_grandfather_mother_sf.materialize()

    child_greatgrandmother_grandfather_mother_sf = child_greatgrandmother_grandfather_mother_ed_sf.groupby(
        key_column_names=['Child_' + targeted_field_name, 'GreatGrandmother_Grandfather_mother_' + targeted_field_name,
                          'Edit_Distance_Child_GreatGrandmother_Grandfather_mother'], operations={'count': agg.COUNT()})
    child_greatgrandmother_grandfather_mother_sf = child_greatgrandmother_grandfather_mother_sf.sort(['count'],
                                                                                                     ascending=False)
    child_greatgrandmother_grandfather_mother_sf = child_greatgrandmother_grandfather_mother_sf.select_columns(
        ['Child_' + targeted_field_name,
         'GreatGrandmother_Grandfather_mother_' + targeted_field_name,
         'Edit_Distance_Child_GreatGrandmother_Grandfather_mother', 'count'])

    child_greatgrandmother_grandfather_mother_sf.materialize()

    child_greatgrandfather_grandmother_mother_sf = child_greatgrandfather_grandmother_mother_ed_sf.groupby(
        key_column_names=['Child_' + targeted_field_name, 'GreatGrandfather_Grandmother_mother_' + targeted_field_name,
                          'Edit_Distance_Child_GreatGrandfather_Grandmother_mother'], operations={'count': agg.COUNT()})
    child_greatgrandfather_grandmother_mother_sf = child_greatgrandfather_grandmother_mother_sf.sort(['count'],
                                                                                                     ascending=False)
    child_greatgrandfather_grandmother_mother_sf = child_greatgrandfather_grandmother_mother_sf.select_columns(
        ['Child_' + targeted_field_name,
         'GreatGrandfather_Grandmother_mother_' + targeted_field_name,
         'Edit_Distance_Child_GreatGrandfather_Grandmother_mother', 'count'])

    child_greatgrandfather_grandmother_mother_sf.materialize()

    child_greatgrandmother_grandmother_mother_sf = child_greatgrandmother_grandmother_mother_ed_sf.groupby(
        key_column_names=['Child_' + targeted_field_name, 'GreatGrandmother_Grandmother_mother_' + targeted_field_name,
                          'Edit_Distance_Child_GreatGrandmother_Grandmother_mother'], operations={'count': agg.COUNT()})
    child_greatgrandmother_grandmother_mother_sf = child_greatgrandmother_grandmother_mother_sf.sort(['count'],
                                                                                                     ascending=False)
    child_greatgrandmother_grandmother_mother_sf = child_greatgrandmother_grandmother_mother_sf.select_columns(
        ['Child_' + targeted_field_name,
         'GreatGrandmother_Grandmother_mother_' + targeted_field_name,
         'Edit_Distance_Child_GreatGrandmother_Grandmother_mother', 'count'])

    child_greatgrandmother_grandmother_mother_sf.materialize()

    duration = time.time() - start_time

    child_greatgrandfather_grandfather_father_sf.export_csv(output_path + parental_relation_type +
                                                            "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_greatgrandfather_grandfather_father_count.csv".format(
                                                                min_chars_count,
                                                                max_edit_distance,
                                                                targeted_field_name,
                                                                parental_relation_type))

    child_greatgrandmother_grandfather_father_sf.export_csv(output_path + parental_relation_type +
                                                            "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_greatgrandmother_grandfather_father_count.csv".format(
                                                                min_chars_count,
                                                                max_edit_distance,
                                                                targeted_field_name,
                                                                parental_relation_type))

    child_greatgrandfather_grandmother_father_sf.export_csv(output_path + parental_relation_type +
                                                            "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_greatgrandfather_grandmother_father_count.csv".format(
                                                                min_chars_count,
                                                                max_edit_distance,
                                                                targeted_field_name,
                                                                parental_relation_type))

    child_greatgrandmother_grandmother_father_sf.export_csv(output_path + parental_relation_type +
                                                            "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_greatgrandmother_grandmother_father_count.csv".format(
                                                                min_chars_count,
                                                                max_edit_distance,
                                                                targeted_field_name,
                                                                parental_relation_type))

    ######
    child_greatgrandfather_grandfather_mother_sf.export_csv(output_path + parental_relation_type +
                                                            "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_greatgrandfather_grandfather_mother_count.csv".format(
                                                                min_chars_count,
                                                                max_edit_distance,
                                                                targeted_field_name,
                                                                parental_relation_type))

    child_greatgrandmother_grandfather_mother_sf.export_csv(output_path + parental_relation_type +
                                                            "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_greatgrandmother_grandfather_mother_count.csv".format(
                                                                min_chars_count,
                                                                max_edit_distance,
                                                                targeted_field_name,
                                                                parental_relation_type))

    child_greatgrandfather_grandmother_mother_sf.export_csv(output_path + parental_relation_type +
                                                            "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_greatgrandfather_grandmother_mother_count.csv".format(
                                                                min_chars_count,
                                                                max_edit_distance,
                                                                targeted_field_name,
                                                                parental_relation_type))

    child_greatgrandmother_grandmother_mother_sf.export_csv(output_path + parental_relation_type +
                                                            "/geq_{0}_chars/ED_1_{1}/wt_{2}_{3}_stacked_no_prefix_ed_geq_{0}_chars_ED_1_{1}_child_greatgrandmother_grandmother_mother_count.csv".format(
                                                                min_chars_count,
                                                                max_edit_distance,
                                                                targeted_field_name,
                                                                parental_relation_type))

    ######
    ##

    return child_greatgrandfather_grandfather_father_sf, child_greatgrandmother_grandfather_father_sf, child_greatgrandfather_grandmother_father_sf, child_greatgrandmother_grandmother_father_sf, child_greatgrandfather_grandfather_mother_sf, child_greatgrandmother_grandfather_mother_sf, child_greatgrandfather_grandmother_mother_sf, child_greatgrandmother_grandmother_mother_sf, duration


# ## Unite records

# In[25]:


def unite_child_parents(child_father_sf, child_mother_sf, target_field_name, parental_relation_type,
                        min_chars_count, max_edit_distance):
    new_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}".format(min_chars_count,
                                                                                       max_edit_distance)
    if not os.path.exists(new_path):
        os.makedirs(new_path)

    start_time = time.time()
    targeted_field_name = target_field_name.replace(" ", "_")

    child_father_renamed_sf = child_father_sf.rename({'Child_' + targeted_field_name: 'Child_Name',
                                                      'Father_' + targeted_field_name: 'Ancestor_Name',
                                                      'Edit_Distance_Child_Father': 'Edit_Distance'})

    child_mother_renamed_sf = child_mother_sf.rename({'Child_' + targeted_field_name: 'Child_Name',
                                                      'Mother_' + targeted_field_name: 'Ancestor_Name',
                                                      'Edit_Distance_Child_Mother': 'Edit_Distance'})

    child_ancestors_count_sf = child_father_renamed_sf.copy()

    child_ancestors_count_sf = child_ancestors_count_sf.append(child_mother_renamed_sf)

    child_ancestors_count_united_sf = child_ancestors_count_sf.groupby(
        key_column_names=['Child_Name', 'Ancestor_Name', 'Edit_Distance'],
        operations={'sum': agg.SUM('count')})
    child_ancestors_count_united_sf = child_ancestors_count_united_sf.sort(['sum'], ascending=False)
    child_ancestors_count_united_sf = child_ancestors_count_united_sf.select_columns(
        ['Child_Name', 'Ancestor_Name', 'Edit_Distance', 'sum'])

    duration = time.time() - start_time

    child_ancestors_count_united_sf.export_csv(
        new_path + "/wt_{0}_{1}_stacked_no_prefix_ed_geq_{2}_chars_ED_1_{3}_child_ancestors.csv".format(
            targeted_field_name, parental_relation_type, min_chars_count, max_edit_distance))

    return child_ancestors_count_united_sf, duration


def unite_child_grandparents(child_grandfather_father_sf, child_grandmother_father_sf,
                             child_grandfather_mother_sf, child_grandmother_mother_sf,
                             target_field_name, parental_relation_type, min_chars_count, max_edit_distance):
    new_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}".format(min_chars_count,
                                                                                       max_edit_distance)
    if not os.path.exists(new_path):
        os.makedirs(new_path)

    start_time = time.time()
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

    duration = time.time() - start_time

    child_ancestors_count_united_sf.export_csv(
        new_path + "/wt_{0}_{1}_stacked_no_prefix_ed_geq_{2}_chars_ED_1_{3}_child_ancestors.csv".format(
            targeted_field_name, parental_relation_type, min_chars_count, max_edit_distance))

    return child_ancestors_count_united_sf, duration


def unite_child_greatgrandparents(child_greatgrandfather_grandfather_father_sf,
                                  child_greatgrandmother_grandfather_father_sf,
                                  child_greatgrandfather_grandmother_father_sf,
                                  child_greatgrandmother_grandmother_father_sf,
                                  child_greatgrandfather_grandfather_mother_sf,
                                  child_greatgrandmother_grandfather_mother_sf,
                                  child_greatgrandfather_grandmother_mother_sf,
                                  child_greatgrandmother_grandmother_mother_sf,
                                  target_field_name, parental_relation_type, min_chars_count, max_edit_distance):
    new_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}".format(min_chars_count,
                                                                                       max_edit_distance)
    if not os.path.exists(new_path):
        os.makedirs(new_path)

    start_time = time.time()
    targeted_field_name = target_field_name.replace(" ", "_")

    child_greatgrandfather_grandfather_father_renamed_sf = child_greatgrandfather_grandfather_father_sf.rename(
        {'Child_' + targeted_field_name: 'Child_Name',
         'GreatGrandfather_Grandfather_father_' + targeted_field_name: 'Ancestor_Name',
         'Edit_Distance_Child_GreatGrandfather_Grandfather_father': 'Edit_Distance'})

    child_greatgrandmother_grandfather_father_renamed_sf = child_greatgrandmother_grandfather_father_sf.rename(
        {'Child_' + targeted_field_name: 'Child_Name',
         'GreatGrandmother_Grandfather_father_' + targeted_field_name: 'Ancestor_Name',
         'Edit_Distance_Child_GreatGrandmother_Grandfather_father': 'Edit_Distance'})

    child_greatgrandfather_grandmother_father_renamed_sf = child_greatgrandfather_grandmother_father_sf.rename(
        {'Child_' + targeted_field_name: 'Child_Name',
         'GreatGrandfather_Grandmother_father_' + targeted_field_name: 'Ancestor_Name',
         'Edit_Distance_Child_GreatGrandfather_Grandmother_father': 'Edit_Distance'})

    child_greatgrandmother_grandmother_father_renamed_sf = child_greatgrandmother_grandmother_father_sf.rename(
        {'Child_' + targeted_field_name: 'Child_Name',
         'GreatGrandmother_Grandmother_father_' + targeted_field_name: 'Ancestor_Name',
         'Edit_Distance_Child_GreatGrandmother_Grandmother_father': 'Edit_Distance'})

    ####
    child_greatgrandfather_grandfather_mother_renamed_sf = child_greatgrandfather_grandfather_mother_sf.rename(
        {'Child_' + targeted_field_name: 'Child_Name',
         'GreatGrandfather_Grandfather_mother_' + targeted_field_name: 'Ancestor_Name',
         'Edit_Distance_Child_GreatGrandfather_Grandfather_mother': 'Edit_Distance'})

    child_greatgrandmother_grandfather_mother_renamed_sf = child_greatgrandmother_grandfather_mother_sf.rename(
        {'Child_' + targeted_field_name: 'Child_Name',
         'GreatGrandmother_Grandfather_mother_' + targeted_field_name: 'Ancestor_Name',
         'Edit_Distance_Child_GreatGrandmother_Grandfather_mother': 'Edit_Distance'})

    child_greatgrandfather_grandmother_mother_renamed_sf = child_greatgrandfather_grandmother_mother_sf.rename(
        {'Child_' + targeted_field_name: 'Child_Name',
         'GreatGrandfather_Grandmother_mother_' + targeted_field_name: 'Ancestor_Name',
         'Edit_Distance_Child_GreatGrandfather_Grandmother_mother': 'Edit_Distance'})

    child_greatgrandmother_grandmother_mother_renamed_sf = child_greatgrandmother_grandmother_mother_sf.rename(
        {'Child_' + targeted_field_name: 'Child_Name',
         'GreatGrandmother_Grandmother_mother_' + targeted_field_name: 'Ancestor_Name',
         'Edit_Distance_Child_GreatGrandmother_Grandmother_mother': 'Edit_Distance'})

    child_ancestors_count_sf = child_greatgrandfather_grandfather_father_renamed_sf.copy()

    child_ancestors_count_sf = child_ancestors_count_sf.append(child_greatgrandmother_grandfather_father_renamed_sf)
    child_ancestors_count_sf = child_ancestors_count_sf.append(child_greatgrandfather_grandmother_father_renamed_sf)
    child_ancestors_count_sf = child_ancestors_count_sf.append(child_greatgrandmother_grandmother_father_renamed_sf)
    child_ancestors_count_sf = child_ancestors_count_sf.append(child_greatgrandfather_grandfather_mother_renamed_sf)
    child_ancestors_count_sf = child_ancestors_count_sf.append(child_greatgrandmother_grandfather_mother_renamed_sf)
    child_ancestors_count_sf = child_ancestors_count_sf.append(child_greatgrandfather_grandmother_mother_renamed_sf)
    child_ancestors_count_sf = child_ancestors_count_sf.append(child_greatgrandmother_grandmother_mother_renamed_sf)

    child_ancestors_count_united_sf = child_ancestors_count_sf.groupby(
        key_column_names=['Child_Name', 'Ancestor_Name', 'Edit_Distance'],
        operations={'sum': agg.SUM('count')})
    child_ancestors_count_united_sf = child_ancestors_count_united_sf.sort(['sum'], ascending=False)
    child_ancestors_count_united_sf = child_ancestors_count_united_sf.select_columns(
        ['Child_Name', 'Ancestor_Name', 'Edit_Distance', 'sum'])

    duration = time.time() - start_time

    child_ancestors_count_united_sf.export_csv(
        new_path + "/wt_{0}_{1}_stacked_no_prefix_ed_geq_{2}_chars_ED_1_{3}_child_ancestors.csv".format(
            targeted_field_name, parental_relation_type, min_chars_count, max_edit_distance))

    return child_ancestors_count_united_sf, duration


# ## Filtering by Occurances

# In[26]:


def filter_higher_than_n_occurances(sf, n):
    sf = sf[sf["sum"] >= n]

    sf.materialize()
    # print(filtered_sf)
    return sf


def filter_by_occurances(sf, n, target_field_name, parental_relation_type, min_chars_count, max_edit_distance):
    new_path = output_path + parental_relation_type + "/geq_{0}_chars/ED_1_{1}".format(min_chars_count,
                                                                                       max_edit_distance)
    if not os.path.exists(new_path):
        os.makedirs(new_path)

    targeted_field_name = target_field_name.replace(" ", "_")

    start_time = time.time()

    sf = filter_higher_than_n_occurances(sf, n)
    print(sf)

    duration = time.time() - start_time

    sf.export_csv(new_path +
                  "/wt_{0}_{1}_stacked_no_prefix_ed_geq_{2}_chars_ED_1_{3}_child_ancestors_geq_{4}_occur.csv".format(
                      targeted_field_name,
                      parental_relation_type,
                      min_chars_count,
                      max_edit_distance,
                      n))

    return sf, duration


# ## Create Graph

# In[27]:


def create_graph(sf):
    g = nx.DiGraph()  # Creating Undirected Graph
    # # adding all nodes and vertices at once
    g.add_weighted_edges_from([(r['Ancestor_Name'], r['Child_Name'], r['sum']) for r in sf])

    print(nx.info(g))

    node_count = g.number_of_nodes()
    edge_count = g.number_of_edges()

    avg_in_degree = sum(d for n, d in g.in_degree()) / float(node_count)
    avg_out_degree = sum(d for n, d in g.out_degree()) / float(node_count)

    return node_count, edge_count, avg_in_degree, avg_out_degree


# In[28]:


def preprocess_child_parents(wikitree_sf, target_field_name, parental_relation_type):
    child_father_sf, sframe_time = create_child_father_sframe_and_save(wikitree_sf,
                                                                       target_field_name,
                                                                       parental_relation_type)
    print("sframe_time: {0}".format(sframe_time))

    shorten_child_father_sf = shorten_child_father_sframe_and_save(child_father_sf,
                                                                   target_field_name,
                                                                   parental_relation_type)

    child_father_stacked_sf, stacked_time = stack_child_father_and_save(shorten_child_father_sf,
                                                                        target_field_name,
                                                                        parental_relation_type)
    print("stacked_time: {0}".format(stacked_time))

    child_father_no_prefix_sf, no_prefix_time = clean_prefix_names_child_father(child_father_stacked_sf,
                                                                                target_field_name,
                                                                                parental_relation_type,
                                                                                prefix_names)

    print("no_prefix_time: {0}".format(no_prefix_time))

    child_father_ed_sf, ed_time = calculate_child_father_ed(child_father_no_prefix_sf,
                                                            target_field_name,
                                                            parental_relation_type)

    print("ed_time: {0}".format(ed_time))

    return child_father_ed_sf, sframe_time, stacked_time, no_prefix_time, ed_time


def preprocess_child_grandparents(wikitree_sf, target_field_name, parental_relation_type):
    child_grandfather_sf, sframe_time = create_child_grandfather_sframe_and_save(wikitree_sf,
                                                                                 target_field_name,
                                                                                 parental_relation_type)

    print("sframe_time: {0}".format(sframe_time))

    shorten_child_greatgrandfather_sf = shorten_child_grandfather_sframe_and_save(child_grandfather_sf,
                                                                                  target_field_name,
                                                                                  parental_relation_type)

    child_grandfather_stacked_sf, stacked_time = stack_child_grandfather_and_save(shorten_child_greatgrandfather_sf,
                                                                                  target_field_name,
                                                                                  parental_relation_type)

    print("stacked_time: {0}".format(stacked_time))

    child_grandfather_no_prefix_sf, no_prefix_time = clean_prefix_names_child_grandfather(child_grandfather_stacked_sf,
                                                                                          target_field_name,
                                                                                          parental_relation_type,
                                                                                          prefix_names)

    print("no_prefix_time: {0}".format(no_prefix_time))

    child_grandfather_ed_sf, ed_time = calculate_child_grandfather_ed(child_grandfather_no_prefix_sf,
                                                                      target_field_name,
                                                                      parental_relation_type)

    print("ed_time: {0}".format(ed_time))

    return child_grandfather_ed_sf, sframe_time, stacked_time, no_prefix_time, ed_time


def preprocess_child_greatgrandparents(wikitree_sf, target_field_name, parental_relation_type):
    child_greatgrandfather_sf, sframe_time = create_child_greatgrandfather_sframe_and_save(wikitree_sf,
                                                                                           target_field_name,
                                                                                           parental_relation_type)

    print("sframe_time: {0}".format(sframe_time))

    shorten_child_greatgrandfather_sf = shorten_child_greatgrandfather_sframe_and_save(child_greatgrandfather_sf,
                                                                                       target_field_name,
                                                                                       parental_relation_type)

    child_greatgrandfather_stacked_sf, stacked_time = stack_child_greatgrandfather_and_save(
        shorten_child_greatgrandfather_sf,
        target_field_name,
        parental_relation_type)

    print("stacked_time: {0}".format(stacked_time))

    child_greatgrandfather_no_prefix_sf, no_prefix_time = clean_prefix_names_child_greatgrandfather(
        child_greatgrandfather_stacked_sf,
        target_field_name,
        parental_relation_type,
        prefix_names)

    print("no_prefix_time: {0}".format(no_prefix_time))

    child_greatgrandfather_ed_sf, ed_time = calculate_child_greatgrandfather_ed(child_greatgrandfather_no_prefix_sf,
                                                                                target_field_name,
                                                                                parental_relation_type)

    print("ed_time: {0}".format(ed_time))

    return child_greatgrandfather_ed_sf, sframe_time, stacked_time, no_prefix_time, ed_time


# In[29]:


def create_edges_child_parents(child_father_ed_filter_chars_sf,
                               target_field_name,
                               parental_relation_type,
                               min_chars_count,
                               max_edit_distance):
    child_father_ed_sf, child_mother_ed_sf, filter_by_ed_time = filter_by_ed_child_father(
        child_father_ed_filter_chars_sf,
        target_field_name,
        parental_relation_type,
        min_chars_count,
        max_edit_distance)

    print("filter_by_ed_time: {0}".format(filter_by_ed_time))

    child_father_groupby_sf, child_mother_groupby_sf, group_by_time = group_by_child_father(child_father_ed_sf,
                                                                                            child_mother_ed_sf,
                                                                                            target_field_name,
                                                                                            parental_relation_type,
                                                                                            min_chars_count,
                                                                                            max_edit_distance)

    print("group_by_time: {0}".format(group_by_time))

    child_ancestors_count_united_sf, unite_time = unite_child_parents(child_father_groupby_sf,
                                                                      child_mother_groupby_sf,
                                                                      target_field_name,
                                                                      parental_relation_type,
                                                                      min_chars_count,
                                                                      max_edit_distance)

    print("unite_time: {0}".format(unite_time))

    return child_ancestors_count_united_sf, filter_by_ed_time, group_by_time, unite_time


def create_edges_child_grandparents(child_grandfather_ed_filter_chars_sf,
                                    target_field_name,
                                    parental_relation_type,
                                    min_chars_count,
                                    max_edit_distance):
    child_grandfather_father_ed_sf, child_grandmother_father_ed_sf, child_grandfather_mother_ed_sf, child_grandmother_mother_ed_sf, filter_by_ed_time = filter_by_ed_child_grandfather(
        child_grandfather_ed_filter_chars_sf,
        target_field_name, parental_relation_type, min_chars_count, max_edit_distance)

    print("filter_by_ed_time: {0}".format(filter_by_ed_time))

    child_grandfather_father_groupby_sf, child_grandmother_father_groupby_sf, child_grandfather_mother_groupby_sf, child_grandmother_mother_groupby_sf, group_by_time = group_by_child_grandfather(
        child_grandfather_father_ed_sf,
        child_grandmother_father_ed_sf, child_grandfather_mother_ed_sf, child_grandmother_mother_ed_sf,
        target_field_name, parental_relation_type, min_chars_count, max_edit_distance)

    print("group_by_time: {0}".format(group_by_time))

    child_ancestors_count_united_sf, unite_time = unite_child_grandparents(child_grandfather_father_groupby_sf,
                                                                           child_grandmother_father_groupby_sf,
                                                                           child_grandfather_mother_groupby_sf,
                                                                           child_grandmother_mother_groupby_sf,
                                                                           target_field_name,
                                                                           parental_relation_type,
                                                                           min_chars_count,
                                                                           max_edit_distance)

    print("unite_time: {0}".format(unite_time))

    return child_ancestors_count_united_sf, filter_by_ed_time, group_by_time, unite_time


def create_edges_child_greatgrandparents(child_greatgrandfather_ed_filter_chars_sf,
                                         target_field_name,
                                         parental_relation_type,
                                         min_chars_count,
                                         max_edit_distance):
    child_greatgrandfather_grandfather_father_ed_sf, child_greatgrandmother_grandfather_father_ed_sf, child_greatgrandfather_grandmother_father_ed_sf, child_greatgrandmother_grandmother_father_ed_sf, child_greatgrandfather_grandfather_mother_ed_sf, child_greatgrandmother_grandfather_mother_ed_sf, child_greatgrandfather_grandmother_mother_ed_sf, child_greatgrandmother_grandmother_mother_ed_sf, filter_by_ed_time = filter_by_ed_child_greatgrandfather(
        child_greatgrandfather_ed_filter_chars_sf,
        target_field_name, parental_relation_type, min_chars_count, max_edit_distance)

    print("filter_by_ed_time: {0}".format(filter_by_ed_time))

    child_greatgrandfather_grandfather_father_groupby_sf, child_greatgrandmother_grandfather_father_groupby_sf, child_greatgrandfather_grandmother_father_groupby_sf, child_greatgrandmother_grandmother_father_groupby_sf, child_greatgrandfather_grandfather_mother_groupby_sf, child_greatgrandmother_grandfather_mother_groupby_sf, child_greatgrandfather_grandmother_mother_groupby_sf, child_greatgrandmother_grandmother_mother_groupby_sf, group_by_time = group_by_child_greatgrandfather(
        child_greatgrandfather_grandfather_father_ed_sf,
        child_greatgrandmother_grandfather_father_ed_sf,
        child_greatgrandfather_grandmother_father_ed_sf,
        child_greatgrandmother_grandmother_father_ed_sf,
        child_greatgrandfather_grandfather_mother_ed_sf,
        child_greatgrandmother_grandfather_mother_ed_sf,
        child_greatgrandfather_grandmother_mother_ed_sf,
        child_greatgrandmother_grandmother_mother_ed_sf,
        target_field_name, parental_relation_type,
        min_chars_count, max_edit_distance)

    print("group_by_time: {0}".format(group_by_time))

    child_ancestors_count_united_sf, unite_time = unite_child_greatgrandparents(
        child_greatgrandfather_grandfather_father_groupby_sf,
        child_greatgrandmother_grandfather_father_groupby_sf,
        child_greatgrandfather_grandmother_father_groupby_sf,
        child_greatgrandmother_grandmother_father_groupby_sf,
        child_greatgrandfather_grandfather_mother_groupby_sf,
        child_greatgrandmother_grandfather_mother_groupby_sf,
        child_greatgrandfather_grandmother_mother_groupby_sf,
        child_greatgrandmother_grandmother_mother_groupby_sf,
        target_field_name,
        parental_relation_type,
        min_chars_count,
        max_edit_distance)

    print("unite_time: {0}".format(unite_time))

    return child_ancestors_count_united_sf, filter_by_ed_time, group_by_time, unite_time


# In[31]:


# target_field_names = ['Last Name Current']
target_field_names = ["First Name"]
# target_field_names = ["Preferred Name"]

# output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/V2/First_Names/"
# output_path = "/home/aviade/Names_Project/Family_Trees_TKDE/V2/Last_Names/"
# output_path = original_path + "Family_Trees_TKDE/Family_Trees_TKDE/V2/First_Names2/"
output_path = original_path + "Family_Trees_TKDE/Family_Trees_TKDE/V2/First_Names/"

if not os.path.exists(output_path):
    os.makedirs(output_path)

# parental_relation_types = ['Child_Father', 'Child_Grandfather', 'Child_GreatGrandfather']
parental_relation_types = ['Child_Father']
# parental_relation_types = ['Child_Grandfather', 'Child_GreatGrandfather']
max_edit_distances = [2, 3, 4, 5, 100]
# max_edit_distances = [3]
min_chars_counts = [2, 3]
# min_chars_counts = [2]
min_occurances = [5, 10]
# min_occurances = [10]

prefix_names = ['Van', 'van',
                'Der', 'der',
                'Del', 'del',
                'Da', 'da',
                'Mc', 'mc',
                'La', 'la',
                'Los', 'los',
                'De', 'de',
                'Don', 'don',
                'Von', 'von',
                'San', 'san',
                'Le', 'le',
                'St', 'st',
                'Zu', 'zu',
                'Und', 'und',
                'Den', 'den',
                'Du', 'du',
                'Di', 'di',
                'Dos', 'dos',
                'Ha', 'ha']

# dataset_path = '/home/aviade/Names_Project/Family_Trees_TKDE/'
dataset_path = original_path + 'Family_Trees_TKDE/Family_Trees_TKDE/'

# target fle should be dump_people_user_full.csv
original_wikitree_sf = tc.SFrame.read_csv(dataset_path + 'dump_people_users.csv', delimiter='\t')
# original_wikitree_sf = tc.SFrame.read_csv(dataset_path + 'short_dump_people_users_100k.csv')
# short_sf = original_wikitree_sf.head(n=100000)
# short_sf.export_csv(dataset_path + "short_dump_people_users_100k.csv")
print("exported dump_people_users_100k")
# original_wikitree_sf = short_sf

results = []
for target_field_name in tqdm(target_field_names):
    for parental_relation_type in tqdm(parental_relation_types):
        wikitree_sf = original_wikitree_sf.copy()
        if parental_relation_type == "Child_Father":
            child_father_ed_sf, sframe_time, stacked_time, no_prefix_time, ed_time = preprocess_child_parents(
                wikitree_sf, target_field_name, parental_relation_type)

        elif parental_relation_type == "Child_Grandfather":
            child_grandfather_ed_sf, sframe_time, stacked_time, no_prefix_time, ed_time = preprocess_child_grandparents(
                wikitree_sf, target_field_name, parental_relation_type)

        elif parental_relation_type == "Child_GreatGrandfather":
            child_greatgrandfather_ed_sf, sframe_time, stacked_time, no_prefix_time, ed_time = preprocess_child_greatgrandparents(
                wikitree_sf, target_field_name, parental_relation_type)

        for min_chars_count in min_chars_counts:
            if parental_relation_type == "Child_Father":
                child_father_ed_filter_chars_sf, filter_chars_time = filter_higher_n_chars_child_father(
                    child_father_ed_sf,
                    target_field_name,
                    parental_relation_type,
                    min_chars_count)

                print("filter_chars_time: {0}".format(filter_chars_time))



            elif parental_relation_type == "Child_Grandfather":
                child_grandfather_ed_filter_chars_sf, filter_chars_time = filter_higher_n_chars_child_grandfather(
                    child_grandfather_ed_sf,
                    target_field_name,
                    parental_relation_type,
                    min_chars_count)

                print("filter_chars_time: {0}".format(filter_chars_time))


            elif parental_relation_type == "Child_GreatGrandfather":
                child_greatgrandfather_ed_filter_chars_sf, filter_chars_time = filter_higher_n_chars_child_greatgrandfather(
                    child_greatgrandfather_ed_sf,
                    target_field_name,
                    parental_relation_type,
                    min_chars_count)

                print("filter_chars_time: {0}".format(filter_chars_time))

            for max_edit_distance in tqdm(max_edit_distances):

                if parental_relation_type == "Child_Father":
                    child_ancestors_count_united_sf, filter_by_ed_time, group_by_time, unite_time = create_edges_child_parents(
                        child_father_ed_filter_chars_sf, target_field_name, parental_relation_type, min_chars_count,
                        max_edit_distance)


                elif parental_relation_type == "Child_Grandfather":
                    child_ancestors_count_united_sf, filter_by_ed_time, group_by_time, unite_time = create_edges_child_grandparents(
                        child_grandfather_ed_filter_chars_sf, target_field_name, parental_relation_type,
                        min_chars_count, max_edit_distance)
                    print(child_ancestors_count_united_sf)

                elif parental_relation_type == "Child_GreatGrandfather":

                    child_ancestors_count_united_sf, filter_by_ed_time, group_by_time, unite_time = create_edges_child_greatgrandparents(
                        child_greatgrandfather_ed_filter_chars_sf, target_field_name, parental_relation_type,
                        min_chars_count, max_edit_distance)

                for min_occurance in tqdm(min_occurances):
                    filter_by_occurances_sf, filter_occur_time = filter_by_occurances(child_ancestors_count_united_sf,
                                                                                      min_occurance,
                                                                                      target_field_name,
                                                                                      parental_relation_type,
                                                                                      min_chars_count,
                                                                                      max_edit_distance)

                    print("filter_occur_time: {0}".format(filter_occur_time))

                    node_count, edge_count, avg_in_degree, avg_out_degree = create_graph(filter_by_occurances_sf)

                    result = (target_field_name, parental_relation_type, min_chars_count, min_occurance,
                              max_edit_distance, sframe_time, stacked_time, no_prefix_time,
                              ed_time, filter_chars_time, filter_by_ed_time, group_by_time,
                              unite_time, node_count, edge_count, avg_in_degree, avg_out_degree)
                    results.append(result)

results_df = pd.DataFrame(results,
                          columns=['target_field_name', 'parental_relation_type', 'min_chars_count', 'min_occurance',
                                   'max_edit_distance', 'sframe_time', 'stacked_time', 'no_prefix_time', 'ed_time',
                                   'filter_chars_time', 'filter_by_ed_time', 'group_by_time', 'unite_time',
                                   'node_count', 'edge_count', 'avg_in_degree', 'avg_out_degree'])
print(results_df)
now = datetime.now()

date_time = now.strftime("%d/%m/%Y_%H:%M:%S")
date_time = date_time.replace(':', '_')
date_time = date_time.replace('/', '_')
results_df.to_csv(output_path + "Generating_Graphs_Time_Performance_{0}.csv".format(date_time), index=False)

print("Done!")

x, y = filter_by_occurances(child_ancestors_count_united_sf, min_occurance,
                            target_field_name,
                            parental_relation_type,
                            min_chars_count,
                            max_edit_distance)

tc.SFrame(
    '/home/user/project_py_3/Family_Trees_TKDE/Family_Trees_TKDE/First_Names/Child_GreatGrandfather/short_wt_First_Name_Child_GreatGrandfather_stacked.csv')

sf_x = tc.SFrame(
    original_path + 'Family_Trees_TKDE/Family_Trees_TKDE/First_Names/Child_Father/geq_2_chars/wt_First_Name_Child_Father_stacked_no_prefix_ed_geq_2_chars.csv')
sf_x_2 = sf_x.sort(['Edit_Distance_Child_Mother'], ascending=False)


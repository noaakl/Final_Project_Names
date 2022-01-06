from nAIme.SpokenName2Vec.run_sound import get_suggestion as spoken
from nAIme.Phonetic.phonetic_suggestion import get_suggestion as phonetic
from nAIme.GRAFT.GRAFT_Create_Suggestions_for_Family_Trees_Graphs_Using_Ordering_Functions_for_package import get_suggestion as GRAFT

def spokenname2vec(name):
    return spoken(name)

def graft(name):
    return GRAFT(name)

def soundex(name):
    return phonetic(name, 'Soundex')

def nysiis(name):
    return phonetic(name, 'Nysiis')

def match_rating_codex(name):
    return phonetic(name, 'Matching_Rating_Codex')

def metaphone(name):
    return phonetic(name, 'Metaphone')


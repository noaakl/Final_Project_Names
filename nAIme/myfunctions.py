from nAIme.SpokenName2Vec.run_sound import get_suggestion as spoken
from nAIme.Phonetic.phonetic_suggestion import get_suggestion as phonetic
from GRAFT.GRAFT_Create_Suggestions_for_Family_Trees_Graphs_Using_Ordering_Functions import get_suggestion as graft


class nAIme:
    def __init__(self): pass

    def spokenname2vec(self, name):
        return spoken(name)

    def graft(self, name):
        return graft(name)

    def soundex(self, name):
        return phonetic(name, 'Soundex')

    def nysiis(self, name):
        return phonetic(name, 'Nysiis')

    def match_rating_codex(self, name):
        return phonetic(name, 'Matching_Rating_Codex')

    def metaphone(self, name):
        return phonetic(name, 'Metaphone')


from SpokenName2Vec.run_names_project_sound import get_suggestion as spoken
from Phonetic.phonetic_suggestion import get_suggestion as phonetic


class nAIme:
    def __init__(self): pass

    def SpokenName2Vec(self, name):
        return spoken(name)

    def soundex(self, name):
        return phonetic(name, 'Soundex')

    def nysiis(self, name):
        return phonetic(name, 'Nysiis')

    def match_rating_codex(self, name):
        return phonetic(name, 'Matching_Rating_Codex')

    def metaphone(self, name):
        return phonetic(name, 'Metaphone')


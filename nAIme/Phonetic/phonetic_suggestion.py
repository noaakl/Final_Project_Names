import jellyfish
import pandas as pd
import editdistance
from nAIme.Phonetic import RelevantFiles
import importlib_resources as pkg_resources


def calculate_edit_distance(name1, name2):
    if not name1 or not name2:
        return -1

    name1 = name1.lower()
    name2 = name2.lower()

    edit_dist = editdistance.eval(name1, name2)
    return edit_dist


def create_suggestions(name, df):
    if not df.empty:
        suggestions_df = df[['First_Name']]
        suggestions_df["Original"] = name
        suggestions_df = suggestions_df.rename(columns={'First_Name': "Candidate"})
        suggestions_df = suggestions_df[["Original", "Candidate"]]
        suggestions_df['Edit_Distance'] = suggestions_df.apply(
            lambda x: calculate_edit_distance(x["Original"], x["Candidate"]), axis=1)
        suggestions_df_sorted_by_ED = suggestions_df.sort_values(['Original', 'Edit_Distance'], ascending=True)
        top_10_suggestions_df = suggestions_df_sorted_by_ED.head(10)
        return top_10_suggestions_df
    else:
        return pd.DataFrame()


def get_suggestion(name, algorithm):
    phonetic_algorithms_dict = {'Soundex': jellyfish.soundex(name), 'Metaphone': jellyfish.metaphone(name),
                                'Nysiis': jellyfish.nysiis(name), 'Matching_Rating_Codex': jellyfish.match_rating_codex(name)}
    name = name.capitalize()
    with pkg_resources.path(RelevantFiles, "wt_First_Name_phonetic_algorithm_codes.csv") as p:
        package_path = p
    name_phonetic_algorithm_df = pd.read_csv(package_path)
    phonetic_algorithm = algorithm
    selected_name_code = phonetic_algorithms_dict[algorithm]
    candidate_df = name_phonetic_algorithm_df[
                    name_phonetic_algorithm_df[phonetic_algorithm] == selected_name_code]
    candidate_df_without_name = candidate_df[candidate_df["First_Name"] != name]

    top_10_synonyms_df = create_suggestions(name, candidate_df_without_name)
    if not top_10_synonyms_df.empty:
        return top_10_synonyms_df['Candidate'].tolist()
    else:
        return []


def main():
    print(get_suggestion("Noa", 'Soundex'))
    print("Done!!")


if __name__ == "__main__":
    main()
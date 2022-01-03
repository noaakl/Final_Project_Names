from gensim.models.fasttext import FastText, load_facebook_model
import pandas as pd
import fasttext.util
# fasttext.util.download_model('en', if_exists='ignore')  # English
# ft = fasttext.load_model('cc.en.300.bin')
# df = pd.DataFrame({'Names': ['guy','joe','john','jack','george']})
# df.to_csv("names.csv", encoding='utf-8', index=False)

# df = pd.read_csv("data.csv", encoding='ISO-8859-1')
# df.to_csv("data.csv", encoding='utf-8', index=False)

mod = fasttext.train_unsupervised('data.csv')
print(mod.words)
# model = fasttext.train_unsupervised("data2.csv")
# print(model.words)
# print(model.get_word_vector("mor"))
# df = pd.DataFrame({'name': ['Guy', 'Hila', 'Joseph', 'John','Joe']})
# df.to_csv("data.csv")
# model = FastText.load_fasttext_format('data.csv')
# print(ft.words)
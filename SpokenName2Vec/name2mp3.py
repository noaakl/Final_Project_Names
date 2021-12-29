from __future__ import print_function
import os
import pandas as pd
from gtts import gTTS
import time
import requests


def count_down_time(seconds_to_wait):
    for i in range(seconds_to_wait, 0, -1):
        time.sleep(1)
        msg = "\rCount Down {0}".format(str(i))
        print(msg, end="")


def create_sound_and_save_mp3(name, output_path):
    tts = gTTS(text=name, lang='en')
    tts.save("{0}/{1}.mp3".format(output_path, name))


def handle_exception_from_google(name, output_path):
    print("Problematic_name:{}".format(name))
    count_down_time(3000)
    create_sound_and_save_mp3(name, output_path)


def convert_name_to_mp3(name=None):
    if not os.path.exists('./mp3s'):
        os.makedirs('./mp3s')
    output_path = "./mp3s"
    if name is not None:
        all_names = [name]
    else:
        names_df = pd.read_csv("./RelevantFiles/all_distinct_names_length_higher_than_2_characters1.csv")
        all_names = names_df["Name"].tolist()
        all_names = sorted(all_names)

    for i, name in enumerate(all_names):
        print("Name:{0} {1}/{2}".format(name, i, len(all_names)))
        try:
            #count_down_time(4)
            create_sound_and_save_mp3(name, output_path)
        except requests.exceptions.HTTPError as e:
            handle_exception_from_google(name, output_path)
        except requests.exceptions.RequestException as e:  # pragma: no cover
            handle_exception_from_google(name, output_path)
        except requests.exceptions.ConnectionError as e:  # pragma: no cover
            handle_exception_from_google(name, output_path)
        except:
            handle_exception_from_google(name, output_path)
    print("convert name to mp3 - Done")


def main():
    convert_name_to_mp3()
    print("Done!!")


if __name__ == "__main__":
    main()
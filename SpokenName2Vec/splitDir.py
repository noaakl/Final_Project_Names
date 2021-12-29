import os.path
import shutil


def split_dir():
    folder_path = "./wavs"
    wavs = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    count = 0
    number = 0
    for wav in wavs:
        if count == 1000:
            count = 0
            number = number + 1
        new_path = folder_path + str(number)
        if not os.path.exists(new_path):
            os.makedirs(new_path)
        old_image_path = os.path.join(folder_path, wav)
        new_image_path = os.path.join(new_path, wav)
        shutil.copy(old_image_path, new_image_path)
        count = count + 1


def main():
    split_dir()


if __name__ == "__main__":
    main()

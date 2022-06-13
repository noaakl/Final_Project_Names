import setuptools
from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='nAIme',
    packages=setuptools.find_packages(),
    package_data={'nAIme': ['Phonetic/RelevantFiles/wt_First_Name_phonetic_algorithm_codes.csv',
                            'SpokenName2Vec/RelevantFiles/RelevantFiles.7z',
                            'GRAFT/RelevantFiles/RelevantFiles.zip'
                            ]},
    include_package_data=True,
    install_requires=['turicreate',
                      'jellyfish',
                      'pandas',
                      'editdistance',
                      'importlib_resources',
                      'pydub',
                      'pyAudioAnalysis',
                      'tqdm',
                      'gtts',
                      'requests',
                      'networkx',
                      'phonetics',
                      'pyunpack',
                      'patool',
                      'matplotlib',
                      'eyed3',
                      'py7zr'
                      ],
    version='0.0.1',
    description='name suggestion python package',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/noaakl/Final_Project_Names",
    author="Students: Noaa Kless, Tal Meridor, Guy Shimony. Academic Instructors: Dr. Aviad Elyashar, Dr. Michael Fire, Dr. Rami Puzis",
    author_email="nAIme.project@gmail.com",
    license='MIT',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Linux",
    ],

    python_requires=">=3.6",
)

# nAIme - Name Search Final Project

### Our nAime project contains:
- SpokenName2Vec algorithm
- GRAFT algorithm
- Python open source package [Click Here](https://pypi.org/project/nAIme/)
- Flask and React website: for more information [Click Here](https://github.com/noaakl/naime-app)
- Reaserch Infrastructure for Siamese Neural Network using SpokenName2Vec algorithm [Click Here](https://github.com/noaakl/Final_Project_Names/tree/main/SiameseNetwork)

##

### Python Package:

#### Run Package:

- use python 3.6 on linux
- install fsmpeg on your computer
- upgrade pip
- run `pip install nAIme`
- import `name.get_suggestion`
- now you can use any of the package's functions. for example: `nAIme.get_suggestion.spokenname2vec('Anny')`

#### Update Package:

run:
- `cd nAIme`
- `python setup.py sdist`
- `pip install twine`
- `twine upload dist/*`

##

### Website:

Website available on [nAIme.cs.bgu.ac.il](https://naime.cs.bgu.ac.il/)

##

### Siamese Neural Network Classifiere:

- Execute the `siamese_network_bgrams_updated.py` file to build and train the network
- The main activation function will run the process and test it
- Change the different parameters to experiment with the network results and scores

##

### Citation


&copy; Algorithms by: Dr. Aviad Elyashar, Dr. Rami Puzis, Dr. Michael Fire


- To cite [SpokenName2Vec](https://doi.org/10.1109/TKDE.2021.3096670) paper, please use the following bibtex reference:

```
@article{elyashar2021does,
  title={How does that name sound? Name representation learning using accent-specific speech generation},
  author={Elyashar, Aviad and Puzis, Rami and Fire, Michael},
  journal={Knowledge-Based Systems},
  volume={227},
  pages={107229},
  year={2021},
  publisher={Elsevier}
}
```

- To cite [GRAFT](https://doi.org/10.1016/j.knosys.2021.107229) paper, please use the following bibtex reference:

```
article{elyashar2021runs,
  title={It Runs in the Family: Unsupervised Algorithm for Alternative Name Suggestion Using Digitized Family Trees},
  author={Elyashar, Aviad and Puzis, Rami and Fire, Michael},
  journal={IEEE Transactions on Knowledge \& Data Engineering},
  number={01},
  pages={1--1},
  year={2021},
  publisher={IEEE Computer Society}
}
```

##

### Authors

- [Noaa Kless](https://github.com/noaakl)
- [Tal Meridor](https://github.com/talmeri)
- [Guy Shimony](https://github.com/guyshimony)

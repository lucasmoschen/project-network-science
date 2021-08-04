# Network analysis Chamber of Deputies Brazil 

Project for the discipline of Network Science of the PhD course in Applied Mathematics at FGV.

## Getting Started

### Dependencies

- All code is written in Python 3.
- [requests](https://docs.python-requests.org/en/master/)
- [pandas](https://pandas.pydata.org)
- [numpy](https://numpy.org/install/)
- [matplotlib](https://matplotlib.org/stable/users/installing.html)
- [seaborn](https://seaborn.pydata.org/installing.html)
- [networkx](https://networkx.org/documentation/stable/install.html)
- To download the data, we use [DadosAbertosBrasil](https://www.gustavofurtado.com/DadosAbertosBrasil/index.html) Package, which can be installed with:
```
pip install DadosAbertosBrasil
```
- For the progress meters, we use [tqdm](https://github.com/tqdm/tqdm), which can be installed with:
```
pip install tqdm
```

### Setup

* Download and prepare deputies, votes, and propositions data:
```
python pyscripts\prepare_data.py
```

## Description of files

Non-Python files:

filename                          |  description
----------------------------------|------------------------------------------------------------------------------------
README.md                         |  Text file (markdown format) description of the project.
notes\preliminary-project.pdf     |  Pdf file used for the Preliminary Presentation of the project.

Python Scripts:

filename                          |  description
----------------------------------|------------------------------------------------------------------------------------
prepare_data.py                   |  Download and prepare deputies, votes, and propositions data.


Python Notebooks:

filename                           |  description
-----------------------------------|------------------------------------------------------------------------------------
data_exploration.ipynb             |  Experiments regarding how to obtain the data and how to use it.
primary_network_construction.ipynb |  Construction of network from preliminary presentation.
network_construction.ipynb         |  Experiments regarding how to define te links between deputies.
#!/usr/bin/python

import pandas as pd
import networkx as nx
import numpy as np
import os 

from tqdm import trange

class GraphConstruction: 
    """
    Class with object of build the adjacency matrices and the graphs
    necessaries for the project. 
    """

    def __init__(self) -> None:
        """
        Init function. It creates all necessary folders. 
        """
        self.data_folder()

    def data_folder(self) -> None: 
        """
        Create the data folder structure. 
        """
        if not os.path.exists('../data/graphs'):
            os.mkdir('../data/graphs/')

    def import_incidence_matrix(self, legislature, year = None):

        if year is None: 
            file_name = '../data/tables/incidence_matrix_{}.csv'.format(legislature) 
        else: 
            file_name = '../data/tables/incidence_matrix_{}_year_{}.csv'.format(legislature, year) 

        incidence_matrix = pd.read_csv(file_name, index_col = 0)
        return incidence_matrix

    def import_adjacency_matrix(self, legislature, abstention_decision, obstruction_decision):

        name = "adjacency_matrix_legislature_{}_{}_{}".format(legislature, abstention_decision, obstruction_decision)
        file_name = '../data/graphs/{}.csv'.format(name) 

        adjacency_matrix = pd.read_csv(file_name, index_col = 0)
        adjacency_matrix.rename(columns = {i: int(i) for i in adjacency_matrix.columns}, inplace = True)

        return adjacency_matrix

    def save_adjacency_matrix(self, adjacency_matrix, name): 
        
        adjacency_matrix.to_csv('../data/graphs/{}.csv'.format(name))
        return 

    def build_adjacency_matrix(self, incidence_matrix, abstention_decision, obstruction_decision, agreement = False): 
        """
        This functions builds the adjacency matrix given a decision: 
        - The -1 and +1 is kept. 
        - The NaN are filled with 0. 
        - The 17 is replaced with 0. 
        - The 0.1 is replaced depending on the decision.
        - The 0 is replaced depending on the decision.
        """
        incidence_matrix = incidence_matrix.replace({17: np.nan, 278: np.nan, 255: np.nan, 0.5: np.nan})
        if obstruction_decision == 'against': 
            incidence_matrix = incidence_matrix.replace({0.1: -1}) 
        elif obstruction_decision == 'unknown': 
            incidence_matrix = incidence_matrix.replace({0.1: np.nan})
        elif obstruction_decision == 'same':
            pass
        else: 
            raise Exception('The decision for obstruction {} was not programmed'.format(obstruction_decision))
        
        if abstention_decision == 'unknown': 
            pass
        elif abstention_decision == 'partial' or abstention_decision == 'partial-unknown' or abstention_decision == 'same': 
            incidence_matrix = incidence_matrix.replace({0: 0.5})
        elif abstention_decision == 'strong': 
            for voting, votes in incidence_matrix.iterrows():  
                majority = np.sign(votes.sum()) 
                incidence_matrix.loc[voting] = incidence_matrix.loc[voting].replace({0: majority})
        else: 
            raise Exception('The decision for abstention {} was not programmed'.format(abstention_decision))
            
        incidence_matrix = incidence_matrix.fillna(0)
        
        n = incidence_matrix.shape[0]
        
        if abstention_decision == 'unknown' or abstention_decision == 'strong': 
            metric = lambda u1, u2: np.inner(u1, u2)
        elif abstention_decision == 'partial': 
            metric = lambda u1, u2: np.inner(u1, u2) + sum(0.25*((u1 == u2)&(u1 == 0.5)) + 1*(u1 + u2 == -0.5))
        elif abstention_decision == 'partial-unknown': 
            metric = lambda u1, u2: sum(1*(u1 - u2 == 0) - 1*(u1 + u2 == 0))
        elif abstention_decision == 'same': 
            if agreement == True:
                def metric(u1, u2): 
                    s = sum((u1!=0)&(u2!=0))
                    if s == 0: return 0 
                    else: 
                        return n*sum((u1==u2)&(u1!=0))/s
            else: 
                metric = lambda u1, u2: sum(1*((u1!=0)*(u2!=0)*(1*(u1 == u2) - 1*(u1 != u2))))
            
        adj = incidence_matrix.corr(method = metric)/n 
        
        return adj

    def build_graph_threshold(self, adjacency_matrix, threshold, legislature = None): 

        adj = 1*(adjacency_matrix > threshold)
        G = nx.from_pandas_adjacency(df = adj)

        if legislature is not None: 
            G = self.deputies_info(G, legislature)

        return G

    def deputies_info(self, graph, legislature): 

        deputies = pd.read_csv('../data/tables/deputies.csv', index_col = 0)

        parties = dict(zip(deputies[deputies.idLegislatura==legislature].id.astype(int), 
                           deputies[deputies.idLegislatura==legislature].siglaPartido))
        uf = dict(zip(deputies[deputies.idLegislatura==legislature].id.astype(int), 
                      deputies[deputies.idLegislatura==legislature].siglaUf))
        region = dict(zip(deputies[deputies.idLegislatura==legislature].id.astype(int), 
                          deputies[deputies.idLegislatura==legislature].region))
        names = dict(zip(deputies[deputies.idLegislatura==legislature].id.astype(int), 
                         deputies[deputies.idLegislatura==legislature].nome))

        nx.set_node_attributes(graph, parties, 'party')
        nx.set_node_attributes(graph, uf, 'uf')
        nx.set_node_attributes(graph, region, 'region')
        nx.set_node_attributes(graph, names, 'names')

        return graph

if __name__ == '__main__': 

    graphConstrutor = GraphConstruction()

    parameters = [('partial-unknown', 'against', False), 
                    ('same', 'same', True)
                    ]

    print("INFO - You can change the parameters of the graph constructor in the script file!")
    print('INFO - These were used by the project. ')
    
    print("QUESTION - Do you want to verify if the file were already generated?")
    while True: 
        ans = input("[y/n]")
        if ans == 'y': 
            verify = True
            break
        elif ans == 'n':
            verify = False
            break
        else: 
            continue

    print("INFO - This procedure can take about 30 minutes!")

    for legislature in trange(52,57,desc='Legislature'): 

        incidence_matrix = graphConstrutor.import_incidence_matrix(legislature)
    
        for par in parameters: 

            if verify: 
                if os.path.exists("adjacency_matrix_legislature_{}_{}_{}".format(legislature, par[0], par[1])): 
                    continue

            adjacency_matrix = graphConstrutor.build_adjacency_matrix(incidence_matrix, 
                                                                    abstention_decision = par[0],
                                                                    obstruction_decision = par[1],
                                                                    agreement = par[2])
                                                                
            graphConstrutor.save_adjacency_matrix(adjacency_matrix, 
                                                "adjacency_matrix_legislature_{}_{}_{}".format(legislature, par[0], par[1]))
                                                
    print("INFO - The adjacency matrices were generated!")
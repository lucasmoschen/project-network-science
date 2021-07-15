#!/usr/bin/python

import pandas as pd
import os 
import requests 
import re

import time
from tqdm import trange, tqdm
import json

from DadosAbertosBrasil import camara

class DataPreprocessing: 
    """
    Class devoted to download and prepare the necessary data to the modeling
    to the votes and deputies from the Brazil National Congress. 
    """

    def __init__(self) -> None:
        """
        Init function. It creates all necessary folders. 
        """

        self.api_website = 'https://dadosabertos.camara.leg.br/api/v2/'
        self.archive_website = 'http://dadosabertos.camara.leg.br/arquivos/'

        self.data_folder()

    def data_folder(self) -> None: 
        """
        Create the data folder structure. 
        """

        if not os.path.exists('../data/'):
            os.mkdir('../data/')

        if not os.path.exists('../data/raw/'): 
            os.mkdir('../data/raw/') 

        if not os.path.exists('../data/tables/'): 
            os.mkdir('../data/tables/') 

    def download_votes_api(self, year1 = 1995, year2 = 2021) -> None: 
        """
        Download voting ids during years year1 to year2. From the ids, all the
        necessary information can be obtained with `camara.Votacao(cod=id)`.
        This function takes too long and it is not recommended.                 
        - year1: MMMM with staring year from 1990 to 2021. 
        - year2: MMMM with ending year from 1990 to 2021. 
        """
        print("WARNING - This function takes to long!")

        votes = pd.DataFrame()

        with trange(year1, year2+1, desc='Year') as years: 
            
            for year in years: 
                
                location = "votacoes?dataInicio={}-01-01&dataFim={}-12-31&itens=200".format(year,year)
                page = requests.get(self.api_website+location)
                last_page = int(re.findall('pagina=([1-9]*)', page.json()['links'][-1]['href'])[0])
            
                for page in trange(1,last_page+1, position=1, leave=False, desc='Page'):
                
                    while True: 
                        try:        
                            vot = camara.lista_votacoes(inicio=str(year)+'-01-01', 
                                                        fim=str(year)+'-12-31', 
                                                        itens=200,
                                                        pagina=page)
                            break
                        except: 
                            time.sleep(10)
                
                    votes = votes.append(vot.reset_index(drop=True))
            
        votes.to_csv('../dados/raw/votacoes_api.csv')


    def download_necessary_files(self, year1 = 1995, year2 = 2021) -> None: 
        """
        This function downloads all the necessary raw data. It includes the
        voting ids from year1 to year2 and the deputies' voting pattern.  
        - year1: MMMM with staring year from 1990 to 2021. 
        - year2: MMMM with ending year from 1990 to 2021. 
        """
        print("MESSAGE - Starting to download the voting informations.")

        for year in trange(year1, year2 + 1, desc='Year'): 
            filepath = '../data/raw/votacoes-{}.csv'.format(year)
            if os.path.exists(filepath): 
                continue
            else: 
                page = requests.get(self.archive_website+'votacoes/csv/votacoes-{}.csv'.format(year))
                with open(filepath, 'w') as f: 
                    f.write(page.text)

        print("MESSAGE - Starting to download the the voting pattern for the deputies. ")

        for year in trange(year1, year2 + 1, desc='Year'): 
            filepath = '../data/raw/votacoesVotos-{}.csv'.format(year)
            if os.path.exists(filepath): 
                continue
            else: 
                page = requests.get(self.archive_website+'votacoesVotos/csv/votacoesVotos-{}.csv'.format(year))
                with open(filepath, 'w') as f: 
                    f.write(page.text)

        print('MESSAGE - The download concluded!')

    def get_deputies(self, l1 = 52, l2 = 56, verify = True) -> None: 
        """
        This function gets the information of the deputies from legislature l1
        to l2. It saves the id, uri, party, state, region, name, and
        legislature. 
        - l1 (int): Starting legislature. 
        - l2 (int): Ending legislature. 
        - verify (bool): verify if the file already exists. If true, the
          function does not do anything.  
        """
        print('MESSAGE - Starting to download the deputies.')

        if verify: 
            if os.path.exists('../data/tables/deputies.csv'): 
                print('MESSAGE - The file already exists.')
                return

        deputies = pd.DataFrame() 
        information = ['id', 'uri', 'nome', 'siglaPartido', 'siglaUf', 'idLegislatura']

        for leg in trange(l1, l2+1, desc='Legislature'):
            while True: 
                try: 
                    deputies = deputies.append(camara.lista_deputados(legislatura=leg)[information].reset_index(drop=True))
                    break
                except Exception as e:
                    print(e) 
                    print("ESSAGE - Trying after 10 seconds...")
                    time.sleep(10)

        regions = {'RR': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte', 'AC': 'Norte', 
                   'RO': 'Norte', 'TO': 'Norte', 'MA': 'Nordeste', 'PI': 'Nordeste', 'CE': 'Nordeste', 
                   'RN': 'Nordeste', 'PB': 'Nordeste', 'PE': 'Nordeste', 'AL': 'Nordeste', 'SE': 'Nordeste', 
                   'BA': 'Nordeste', 'MT': 'Centro-oeste', 'DF': 'Centro-oeste', 'GO': 'Centro-oeste', 
                   'MS': 'Centro-oeste', 'MG': 'Sudeste', 'ES': 'Sudeste', 'RJ': 'Sudeste', 
                   'SP': 'Sudeste', 'PR': 'Sul', 'SC': 'Sul', 'RS': 'Sul'}

        deputies['region'] = deputies.siglaUf.apply(lambda x: regions[x])

        deputies.to_csv('../data/tables/deputies.csv')

        print('MESSAGE - The download is concluded.')

    def prepare_votes_table(self, year1 = 2003, year2 = 2021, verify = True) -> None: 
        """
        Get the important information from the voting files and generate the voting
        tables relating to the ids. We gatter all the voting and filter by
        that which have votes computed. 
        - year1: MMMM with staring year from 1990 to 2021. 
        - year2: MMMM with ending year from 1990 to 2021. 
        """

        print("MESSAGE - Stating to prepare the voting tables.")

        if verify: 
            if os.path.exists('../data/tables/votes_deputies.csv'): 
                if os.path.exists('../data/tables/votes_info.csv'): 
                    print('MESSAGE - The file already exists.')
                    return
                return

        info_votes = ['data', 'siglaOrgao', 'aprovacao', 'votosSim', 'votosNao', 'votosOutros', 
                      'ultimaApresentacaoProposicao_idProposicao']
        
        votes = pd.DataFrame()
        for year in range(year1, year2 + 1): 
            votes = votes.append(pd.read_csv('../data/raw/votacoes-{}.csv'.format(year), 
                                              sep = ';', 
                                              index_col=0)[info_votes])

        info_deputies = ['idVotacao', 'voto', 'deputado_id']

        votes_deputies = pd.DataFrame() 
        for year in range(year1, year2 + 1): 
            votes_deputies = votes_deputies.append(pd.read_csv('../data/raw/votacoesVotos-{}.csv'.format(year), 
                                                               sep = ';')[info_deputies])
        votes_deputies = votes_deputies.reset_index(drop=True)

        votes = votes.loc[votes_deputies.idVotacao.unique()]

        # Separating year, month, and day from date 
        votes['data'] = pd.to_datetime(votes['data'])
        votes['year'] = votes['data'].apply(lambda x: x.year)
        votes['month'] = votes['data'].apply(lambda x: x.month)
        votes['day'] = votes['data'].apply(lambda x: x.day)

        # Adding legislature
        def legislature_calc(row): 
            year = (row.year - 2003)
            return year//4 + 52 - ((row.month == 1)&(year%4==0))
        votes['legislature'] = votes.apply(legislature_calc, axis=1)

        votes.drop(columns='data').to_csv('../data/tables/votes_info.csv')

        # Fixing nan values 
        votes_deputies.voto = votes_deputies.voto.fillna('Secreto')
        
        votes_deputies.to_csv('../data/tables/votes_deputies.csv', index=False)
        
        print("MESSAGE - Voting tables finished!")

    def get_propositions(self) -> None: 
        """
        Get the topics and types of the propositions related to the votes. 
        """
        print("WARNING - The propositions table is still being developed...")

        propositions = {'id': [], 'siglaTipo': [], 'codTema': [], 'Tema': []}
        votes = pd.read_csv('../data/tables/votes_info.csv')

        for proposition in tqdm(votes.ultimaApresentacaoProposicao_idProposicao.unique()):
                
            error = False
            while True: 
                try:   
                    prop = camara.Proposicao(cod=proposition)
                    break
                except KeyError: 
                    error = True
                    break
                except:
                    time.sleep(5)
                        
            if error: 
                propositions['id'].append(proposition)
                propositions['siglaTipo'].append(None)
                propositions['codTema'].append(None)
                propositions['Tema'].append(None)
            else:
                propositions['id'].append(proposition)
                propositions['siglaTipo'].append(prop.tipo_sigla)

                tema = prop.temas() 
                if tema.shape[0] > 0: 
                    propositions['codTema'].append(list(tema.codTema))
                    propositions['Tema'].append(list(tema.tema))
                else: 
                    propositions['codTema'].append(None)
                    propositions['Tema'].append(None)

        pd.DataFrame(propositions).to_csv("../data/tables/propositions.csv", index=False) 

        print("MESSAGE - Proposition file is done!")

    def get_fronts(self, verify=True) -> None: 
        """
        This function downloads all the parliamentary fronts and their
        members. It saves as a csv table in the end. It gets data from the
        legislature 54. Before that, membership information was unavailable. 
        """

        print("MESSAGE - Starting to download and prepare the fronts file.")

        if verify: 
            if os.path.exists('../data/tables/fronts.csv'): 
                print('MESSAGE - The file already exists.')
                return

        page = requests.get(self.archive_website+"frentesDeputados/csv/frentesDeputados.csv")
        with open('../data/raw/fronts.csv', 'w') as f: 
            f.write(page.text)

        print('MESSAGE - The file was downloaded! Saving...')

        info_needed = ['id', 'titulo', 'deputado_.id', 'deputado_.idLegislatura','deputado_.titulo']
        fronts = pd.read_csv('../data/raw/fronts.csv', sep=';')[info_needed]
        
        fronts['deputado_.id'].fillna(0, inplace=True)
        fronts['deputado_.id'] = fronts['deputado_.id'].astype(int)  

        fronts['deputado_.titulo'] = fronts['deputado_.titulo'].apply(lambda x: 1*(x[0] == 'C'))
        
        fronts = fronts.rename(columns={'id': 'id_front', 
                                        'deputado_.id': 'id_deputado', 
                                        'deputado_.idLegislatura': 'legislatura', 
                                        'deputado_.titulo': 'coordenador'})

        fronts.to_csv('../data/tables/fronts.csv')

        print('MESSAGE - Parliamentary fronts file done!')

    def incidence_matrix(self, verify = True) -> None: 
        """
        This function creates the incidence matrix, where the rows are the
        votes and the columns are the deputies. Each legislature is saved in a
        different file. 
        """

        print("MESSAGE - Starting to build the incidence matrices.")

        if verify:
            for legislature in range(52,57): 
                if not os.path.exists('../data/tables/incidence_matrix_{}.csv'.format(legislature)): 
                    break
            else: 
                print('MESSAGE - The file already exists.')
                return

        vote_mapping = {"Não": -1, 
                "Sim": 1, 
                "Abstenção": 0, 
                "Secreto": 278, 
                "Artigo 17": 17, 
                "Branco": 255, 
                "Obstrução": 0.1, 
                "Favorável com restrições": 0.5}
        with open("../data/tables/vote_mapping.json", 'w') as f: 
            json.dump(vote_mapping, f)

        votes = pd.read_csv('../data/tables/votes_info.csv')
        votes_deputies = pd.read_csv('../data/tables/votes_deputies.csv')
        votes_deputies["voto"] = votes_deputies["voto"].replace(vote_mapping)
        votes_info = pd.merge(left=votes_deputies, right=votes, left_on='idVotacao', right_on='id').drop(columns='id')
        
        del votes, votes_deputies

        for legislature in trange(52,57, position=0, desc='Legislature'): 
            
            votes = votes_info[votes_info.legislature==legislature]
            unique_votes = votes.idVotacao.unique()
            unique_deputies = votes.deputado_id.unique()
            
            incidence_matrix = pd.DataFrame(index = unique_votes, columns = unique_deputies)

            for deputy in tqdm(unique_deputies, position=1,leave=True, desc='Deputies'): 

                filter_dep = votes[votes.deputado_id == deputy]
                incidence_matrix.loc[filter_dep.idVotacao, deputy] = filter_dep.voto.values
                
            incidence_matrix.to_csv("../data/tables/incidence_matrix_{}.csv".format(legislature))
            
        print("\n")                
        print("MESSAGE - The incidence matrices are done!")

if __name__ == '__main__': 

    preprocessing = DataPreprocessing()
    preprocessing.download_necessary_files()

    preprocessing.get_deputies()

    preprocessing.prepare_votes_table()

    preprocessing.get_fronts()

    print("Do you want to download the propositions? It takes 20min - 30min and it is in development.")
    while True: 
        ans = input("[y/n]")
        if ans == 'y': 
            preprocessing.get_propositions()
            break
        elif ans == 'n':
            break
        else: 
            continue

    preprocessing.incidence_matrix()
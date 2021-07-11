#!/usr/bin/python

import pandas as pd
import os 
import requests 
import re

import time
from tqdm import trange

from DadosAbertosBrasil import camara

class DataPreprocessing: 
    """
    Class devoted to download and prepare the necessary data to the modeling
    to the polls and deputies from the Brazil National Congress. 
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

    def download_polls_api(self, year1 = 1995, year2 = 2021) -> None: 
        """
        Download voting ids during years year1 to year2. From the ids, all the
        necessary information can be obtained with `camara.Votacao(cod=id)`.
        This function takes too long and it is not recommended.                 
        - year1: MMMM with staring year from 1990 to 2021. 
        - year2: MMMM with ending year from 1990 to 2021. 
        """
        print("WARNING - This function takes to long!")

        polls = pd.DataFrame()

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
                
                    polls = polls.append(vot.reset_index(drop=True))
            
        polls.to_csv('../dados/raw/votacoes_api.csv')


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

        print('MESSAGE - download concluded!')

if __name__ == '__main__': 

    preprocessing = DataPreprocessing()
    preprocessing.download_necessary_files()
import pandas as pd
import os

class TableCreator:
    def __init__(self, data, folder_path):
        self.data = data
        self.folder_path = folder_path

    def create_table(self, table_name, columns):
        df = self.data[columns].drop_duplicates().reset_index(drop=True)
        file_path = os.path.join(self.folder_path, f'{table_name}.parquet')
        df.to_parquet(file_path, index=False)

    def create_tables(self):
        tables = {
            'dim_games': ['gameid', 'league', 'year', 'split', 'playoffs', 'patch', 'gamelength'],
            'fact_game_date': ['gameid', 'date'],
            'dim_teams': ['teamid', 'teamname'],
            'dim_players': ['playerid', 'playername'],
            'dim_participants': ['participantid', 'gameid', 'teamid', 'playerid', 'position', 'champion'],
            'fact_side': ['teamid', 'position'],
            'dim_bans': ['gameid', 'teamid', 'ban1', 'ban2', 'ban3', 'ban4', 'ban5'],
            'fact_amount': ['gameid', 'game'],
            'fact_gamestats': ['gameid', 'teamid', 'playerid', 'teamkills', 'teamdeaths', 'result'],
            'fact_teamstats': ['gameid', 'teamid', 'firstblood', 'firstdragon', 'firstherald', 'inhibitors', 'opp_inhibitors',
                               'totalgold', 'earnedgold', 'earned gpm', 'earnedgoldshare', 'goldspent', 'firstbaron', 'firsttower',
                               'firstmidtower', 'turretplates', 'opp_turretplates', 'firsttothreetowers', 'team kpm', 'ckpm', 'result'],
            'fact_playerstats': ['gameid', 'participantid', 'playerid', 'kills', 'deaths', 'assists', 'doublekills', 'triplekills',
                                 'quadrakills', 'pentakills', 'damagetochampions', 'damageshare', 'damagetakenperminute',
                                 'damagemitigatedperminute', 'wardsplaced', 'wpm', 'wardskilled', 'controlwardsbought', 'visionscore'],
            'fact_goldandxpstats': ['gameid', 'playerid', 'participantid', 'goldat10', 'xpat10', 'csat10', 'csdiffat10', 'golddiffat10',
                                    'goldat15', 'xpat15', 'csat15', 'csdiffat15', 'golddiffat15', 'killsat10', 'assistsat10', 'deathsat10',
                                    'opp_killsat10', 'opp_assistsat10', 'opp_deathsat10', 'killsat15', 'assistsat15', 'deathsat15',
                                    'opp_killsat15', 'opp_assistsat15', 'opp_deathsat15'],
            'dim_dragons': ['gameid', 'teamid', 'dragons', 'opp_dragons', 'elementaldrakes', 'opp_elementaldrakes',
                            'infernals', 'mountains', 'clouds', 'oceans', 'chemtechs', 'hextechs', 'dragons (type unknown)',
                            'elders', 'opp_elders'],
            'dim_barons': ['gameid', 'teamid', 'firstbaron', 'barons', 'opp_barons']
        }

        for table_name, columns in tables.items():
            self.create_table(table_name, columns)


# Define o caminho para a pasta
folder_path = 'data/silver/2022/'

# Verifica se a pasta existe, se não, cria a pasta
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Lê o arquivo parquet
df_original = pd.read_parquet('data/bronze/parquet-data/2022_LoL_esports_match_data_from_OraclesElixir.parquet')

# Instancia o TableCreator e cria as tabelas
table_creator = TableCreator(df_original, folder_path)
table_creator.create_tables()

# Define o caminho para a pasta
folder_path = 'data/silver/2021/'

# Verifica se a pasta existe, se não, cria a pasta
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Lê o arquivo parquet
df_original = pd.read_parquet('data/bronze/parquet-data/2021_LoL_esports_match_data_from_OraclesElixir.parquet')

# Instancia o TableCreator e cria as tabelas
table_creator = TableCreator(df_original, folder_path)
table_creator.create_tables()

# Define o caminho para a pasta
folder_path = 'data/silver/2020/'

# Verifica se a pasta existe, se não, cria a pasta
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Lê o arquivo parquet
df_original = pd.read_parquet('data/bronze/parquet-data/2020_LoL_esports_match_data_from_OraclesElixir.parquet')

# Instancia o TableCreator e cria as tabelas
table_creator = TableCreator(df_original, folder_path)
table_creator.create_tables()

# Define o caminho para a pasta
folder_path = 'data/silver/2019/'

# Verifica se a pasta existe, se não, cria a pasta
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Lê o arquivo parquet
df_original = pd.read_parquet('data/bronze/parquet-data/2019_LoL_esports_match_data_from_OraclesElixir.parquet')

# Instancia o TableCreator e cria as tabelas
table_creator = TableCreator(df_original, folder_path)
table_creator.create_tables()

# Define o caminho para a pasta
folder_path = 'data/silver/2018/'

# Verifica se a pasta existe, se não, cria a pasta
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Lê o arquivo parquet
df_original = pd.read_parquet('data/bronze/parquet-data/2018_LoL_esports_match_data_from_OraclesElixir.parquet')

# Instancia o TableCreator e cria as tabelas
table_creator = TableCreator(df_original, folder_path)
table_creator.create_tables()

# Define o caminho para a pasta
folder_path = 'data/silver/2017/'

# Verifica se a pasta existe, se não, cria a pasta
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Lê o arquivo parquet
df_original = pd.read_parquet('data/bronze/parquet-data/2017_LoL_esports_match_data_from_OraclesElixir.parquet')

# Instancia o TableCreator e cria as tabelas
table_creator = TableCreator(df_original, folder_path)
table_creator.create_tables()

# Define o caminho para a pasta
folder_path = 'data/silver/2016/'

# Verifica se a pasta existe, se não, cria a pasta
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Lê o arquivo parquet
df_original = pd.read_parquet('data/bronze/parquet-data/2016_LoL_esports_match_data_from_OraclesElixir.parquet')

# Instancia o TableCreator e cria as tabelas
table_creator = TableCreator(df_original, folder_path)
table_creator.create_tables()

# Define o caminho para a pasta
folder_path = 'data/silver/2015/'

# Verifica se a pasta existe, se não, cria a pasta
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Lê o arquivo parquet
df_original = pd.read_parquet('data/bronze/parquet-data/2015_LoL_esports_match_data_from_OraclesElixir.parquet')

# Instancia o TableCreator e cria as tabelas
table_creator = TableCreator(df_original, folder_path)
table_creator.create_tables()

# Define o caminho para a pasta
folder_path = 'data/silver/2014/'

# Verifica se a pasta existe, se não, cria a pasta
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Lê o arquivo parquet
df_original = pd.read_parquet('data/bronze/parquet-data/2014_LoL_esports_match_data_from_OraclesElixir.parquet')

# Instancia o TableCreator e cria as tabelas
table_creator = TableCreator(df_original, folder_path)
table_creator.create_tables()
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
            'dim_barons': ['gameid', 'teamid', 'firstbaron', 'barons', 'opp_barons'],
            'dim_heralds': ['gameid', 'teamid', 'firstherald', 'heralds', 'opp_heralds']
        }

        for table_name, columns in tables.items():
            self.create_table(table_name, columns)

def main():
    # Define the base folder path
    base_folder_path = 'data/silver/'

    # Years to process
    years = ['2022', '2021', '2020', '2019', '2018', '2017', '2016', '2015', '2014']

    for year in years:
        folder_path = os.path.join(base_folder_path, year)

        # Check if the folder exists, if not, create it
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        # Read the parquet file
        file_path = f'data/bronze/parquet-data/{year}_LoL_esports_match_data_from_OraclesElixir.parquet'
        df_original = pd.read_parquet(file_path)

        # Instantiate TableCreator and create the tables
        table_creator = TableCreator(df_original, folder_path)
        table_creator.create_tables()

if __name__ == '__main__':
    main()

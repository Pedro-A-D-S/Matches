# Imports

import pandas as pd

import yaml
import os

# config yaml path file
config = yaml.safe_load(open('../configuration/path.yaml'))

# Get Data
df_original = pd.read_parquet(config['data']['bronze']['parquet'][2022])

# Table Games
games_cols = ['gameid', 'date', 'league', 'year', 'split', 'playoffs', 'patch', 'gamelength']
df_games = df_original[games_cols].drop_duplicates().reset_index(drop=True)

# Table Teams
teams_cols = ['teamid', 'teamname']
df_teams = df_original[teams_cols].drop_duplicates().reset_index(drop=True)

# Table Players
players_cols = ['playerid', 'playername']
df_players = df_original[players_cols].drop_duplicates().reset_index(drop=True)

# Table Participants
participants_cols = ['participantid', 'gameid', 'teamid', 'playerid', 'side', 'position', 'champion']
df_participants = df_original[participants_cols].drop_duplicates().reset_index(drop=True)

# Table Bans
bans_cols = ['gameid', 'teamid', 'ban1', 'ban2', 'ban3', 'ban4', 'ban5']
df_bans = df_original[bans_cols].drop_duplicates().reset_index(drop=True)

# Table Game Stats
gamestats_cols = ['gameid', 'teamid', 'playerid', 'teamkills', 'teamdeaths', 'firstblood', 'firstdragon', 'firstherald',
                  'inhibitors', 'opp_inhibitors', 'totalgold', 'earnedgold', 'earned gpm', 'earnedgoldshare', 'goldspent', 
                  'firstbaron', 'firsttower', 'firstmidtower', 'turretplates', 'opp_turretplates', 'firsttothreetowers',
                  'team kpm', 'ckpm', 'result']
df_gamestats = df_original[gamestats_cols].drop_duplicates().reset_index(drop=True)

# Table Player Stats
playerstats_cols = ['gameid', 'participantid', 'playerid', 'kills', 'deaths', 'assists', 'doublekills', 'triplekills', 'quadrakills',
                    'pentakills', 'damagetochampions', 'damageshare', 'damagetakenperminute', 'damagemitigatedperminute', 'wardsplaced',
                    'wpm', 'wardskilled', 'controlwardsbought', 'visionscore']
df_playerstats = df_original[playerstats_cols].drop_duplicates().reset_index(drop=True)

# Table GoldandXPStats
goldandxpstats_cols = ['gameid', 'playerid', 'participantid', 'goldat10', 'xpat10', 'csat10', 'csdiffat10', 'golddiffat10', 'goldat15',
                       'xpat15', 'csat15', 'csdiffat15', 'golddiffat15', 'killsat10', 'assistsat10', 'deathsat10',
                       'opp_killsat10', 'opp_assistsat10', 'opp_deathsat10', 'killsat15', 'assistsat15', 'deathsat15',
                       'opp_killsat15', 'opp_assistsat15', 'opp_deathsat15']
df_goldandxpstats = df_original[goldandxpstats_cols].drop_duplicates().reset_index(drop=True)

# Table Dragons
dragons_cols = ['gameid', 'teamid', 'dragons', 'opp_dragons', 'elementaldrakes', 'opp_elementaldrakes',
                'infernals', 'mountains', 'clouds', 'oceans', 'chemtechs', 'hextechs', 'dragons (type unknown)',
                'elders', 'opp_elders']
df_dragons = df_original[dragons_cols].drop_duplicates().reset_index(drop=True)

# Table Heralds
heralds_cols = ['gameid', 'teamid', 'firstherald', 'heralds', 'opp_heralds']
df_heralds = df_original[heralds_cols].drop_duplicates().reset_index(drop=True)

# Table Barons
barons_cols = ['gameid', 'teamid', 'firstbaron', 'barons', 'opp_barons']
df_barons = df_original[barons_cols].drop_duplicates().reset_index(drop=True)

# define the path to the folder
folder_path = '../data/silver/2022/'

# create the golder if it doesn't exist
if not os.path.exists(folder_path):
    os.makedirs(folder_path)
    
# Save the tables as Parquet files in the folder

df_games.to_parquet(os.path.join(folder_path, 'dim_games_2022.parquet'), index=False)
df_teams.to_parquet(os.path.join(folder_path, 'dim_teams_2022.parquet'), index=False)
df_players.to_parquet(os.path.join(folder_path, 'dim_players_2022.parquet'), index=False)
df_participants.to_parquet(os.path.join(folder_path, 'dim_participants_2022.parquet'), index=False)
df_bans.to_parquet(os.path.join(folder_path, 'dim_bans_2022.parquet'), index=False)
df_gamestats.to_parquet(os.path.join(folder_path, 'fact_gamestats_2022.parquet'), index=False)
df_playerstats.to_parquet(os.path.join(folder_path, 'fact_playerstats_2022.parquet'), index=False)
df_goldandxpstats.to_parquet(os.path.join(folder_path, 'fact_goldandxpstats_2022.parquet'), index=False)
df_dragons.to_parquet(os.path.join(folder_path, 'dim_dragons_2022.parquet'), index=False)
df_barons.to_parquet(os.path.join(folder_path, 'dim_barons_2022.parquet'), index=False)
import sqlite3
import pandas as pd
    
conn = sqlite3.connect('database/matches_2022.db')

create_table_query_games = '''
CREATE TABLE IF NOT EXISTS dim_games (
    gameid TEXT PRIMARY KEY,
    date TIMESTAMP,
    league TEXT,
    year INTEGER,
    split TEXT,
    playoffs INTEGER,
    gamelength INTEGER)'''

create_table_query_teams = '''
CREATE TABLE IF NOT EXISTS dim_teams (
    teamid TEXT PRIMARY KEY,
    teamname TEXT)'''
    
create_table_query_players = '''
CREATE TABLE IF NOT EXISTS dim_players (
    playerid TEXT PRIMARY KEY
    playername TEXT)'''
    
create_table_query_participants = '''
CREATE TABLE IF NOT EXISTS dim_participants (
    playerid TEXT PRIMARY KEY
    teamid TEXT
    gameid TEXT
    side TEXT
    position TEXT
    champion TEXT)'''

create_table_query_bans = '''
CREATE TABLE IF NOT EXISTS dim_bans (
    teamid TEXT PRIMARY KEY
    gameid TEXT
    ban1 TEXT
    ban2 TEXT
    ban3 TEXT
    ban4 TEXT
    ban5 TEXT)'''
    
create_table_query_gamestats = '''
CREATE TABLE IF NOT EXISTS fact_gamestats (
    playerid TEXT PRIMARY KEY
    gameid TEXT
    teamid TEXT
    teamkills INTEGER
    teamdeaths INTEGER
    firstblood INTEGER
    firstdragon INTEGER
    firstherald INTEGER
    firstbaron INTEGER
    firsttower INTEGER
    firstmidtower INTEGER
    turretplates INTEGER
    oop_turretplates INTEGER
    firstofthreetowers INTEGER
    inhibitors INTEGER
    oop_inhubitors INTEGER
    totalgold INTEGER
    earnedgold INTEGER
    earned gpm REAL
    earnedgoldshare REAL
    goldspent INTEGER
    team kpm REAL
    ckpm REAL
    result INTEGER)'''

create_table_query_playerstats = '''
CREATE TABLE IF NOT EXISTS fact_playerstats (
    playerid TEXT PRIMARY KEY
    participantid INTEGER
    gameid TEXT
    kills INTEGER
    deaths INTEGER
    assists INTEGER
    doublekills INTEGER
    triplekills INTEGER
    quadrakills INTEGER
    pentakills INTEGER
    damagetochampions REAL
    damageshare REAL
    damagetakenperminute REAL
    damagemitigatedperminute REAL
    wardsplaced INTEGER
    controlwardsbought INTEGER
    visionscore INTEGER)'''



conn.execute(create_table_query_games)
conn.execute(create_table_query_teams)
conn.execute(create_table_query_players)
conn.execute(create_table_query_participants)
conn.execute(create_table_query_bans)
conn.execute(create_table_query_gamestats)
conn.execute(create_table_query_playerstats)




# Load data
df_games = pd.read_parquet('data/silver/2022/dim_games_2022.parquet')
df_games.to_sql('dim_games', conn, if_exists = 'replace', index = False)

query = "SELECT * FROM dim_games WHERE patch = '12.01'"
df_result = pd.read_sql_query(query, conn)

print(df_result)
-- overview of leagues

SELECT *

        FROM(SELECT 
            league,
            COUNT(DISTINCT gameid) AS amount_games,
            COUNT(DISTINCT playerid) AS amount_players,
            COUNT(DISTINCT teamname) AS amount_teams,
            COUNT(DISTINCT champion) AS champions_played,
            COUNT(DISTINCT league) AS leagues_amount,
            SUM(pentakills) AS amount_pentakills,
            SUM(quadrakills) AS amount_quadrakills,
            SUM(triplekills) AS amount_triplekills,
            SUM(doublekills) AS amount_doublekills
                     FROM df
                     WHERE year = 2022
                     GROUP BY league) AS T1
WHERE T1.amount_pentakills > 0
ORDER BY amount_games DESC


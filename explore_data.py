import pandas as pd

Europe_df = pd.read_csv("games/soloq/Europe_stats.csv")
Asia_df = pd.read_csv("games/soloq/Asia_stats.csv")

# print("Europe soloq champion samples: ", Europe_df.shape[0])
# print("Asia soloq champion samples: ", Asia_df.shape[0])

total_games = pd.concat([Europe_df, Asia_df])

print("Total soloq champion samples: ", total_games.shape[0])
print("Total soloq games: ", total_games.shape[0]/10)
soloq_samples = total_games.shape[0]

print("Number of champions in soloq games: ", pd.unique(total_games['championName']).shape[0])

champions = []
games = []
for champ in pd.unique(total_games['championName']):
    champions.append(champ)
    games.append(total_games[total_games['championName'] == champ].shape[0])

soloq_games_by_champions = pd.DataFrame({'champion': champions, 'games': games})
soloq_games_by_champions['percentage'] = soloq_games_by_champions.apply(lambda row: (row['games'] / soloq_samples) * 100, axis=1)
# print("Most amount of games: ", soloq_games_by_champions.sort_values(['games'],ascending=False).iloc[0]['champion'], " -> ", soloq_games_by_champions.sort_values(['games'],ascending=False).iloc[0]['games'])
# print("Least amount of games: ", soloq_games_by_champions.sort_values(['games'],ascending=False).iloc[-1]['champion'], " -> ", soloq_games_by_champions.sort_values(['games'],ascending=False).iloc[-1]['games'])

print("soloq games with at least 10 games: ", soloq_games_by_champions[soloq_games_by_champions['games'] > 10].shape[0], ", percentage of total champions present: ", round((soloq_games_by_champions[soloq_games_by_champions['games'] > 10].shape[0] / 163) * 100, 2) , "%")

# print(soloq_games_by_champions[soloq_games_by_champions['games'] > 10])

# competitive

competitive_df = pd.read_excel("games/competitive/leaguepedia_games/total_games.xlsx")
print("")
competitive_samples = competitive_df.shape[0]
print("competitive champion samples: ", competitive_df.shape[0])
print("Number of champions in competitive games: ", pd.unique(competitive_df['championName']).shape[0])

champions = []
games = []
for champ in pd.unique(competitive_df['championName']):
    champions.append(champ)
    games.append(competitive_df[competitive_df['championName'] == champ].shape[0])

competitive_games_by_champions = pd.DataFrame({'champion': champions, 'games': games})
# print("Most amount of games: ", competitive_games_by_champions.sort_values(['games'],ascending=False).iloc[0]['champion'], " -> ", competitive_games_by_champions.sort_values(['games'],ascending=False).iloc[0]['games'])
# print("Least amount of games: ", competitive_games_by_champions.sort_values(['games'],ascending=False).iloc[-1]['champion'], " -> ", competitive_games_by_champions.sort_values(['games'],ascending=False).iloc[-1]['games'])


competitive_games_by_champions['percentage'] = competitive_games_by_champions.apply(lambda row: (row['games'] / competitive_samples) * 100, axis=1)

# print(competitive_games_by_champions.sort_values(['percentage'],ascending=False))


print("competitive games with at least 10 games: ", competitive_games_by_champions[competitive_games_by_champions['games'] > 10].shape[0], ", percentage of total champions present: ", round((competitive_games_by_champions[competitive_games_by_champions['games'] > 10].shape[0] / 163) * 100, 2), "%")

# print(competitive_games_by_champions[competitive_games_by_champions['games'] > 10])


print("")
print("")
for col in total_games.columns:
    print(col)
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

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

print(total_games)
# for variable in total_games.columns:
#     print(variable)


### analyze dispersion
# total_games_reduced = total_games.drop(columns=["Unnamed: 0", "game_id", "gameVersion", "gameDuration", "summonerName", "puuid", "participantId", "teamId", "teamPosition", "championName", "championId", "runeStyle", "runeSubStyle", "rune0", "rune1", "rune2", "rune3", "rune4", "rune5", "summoner1Id", "summoner2Id", "item0", "item1", "item2", "item3", "item4", "item5", "item6", "result"])
# for variable in total_games_reduced.columns:
#     print(variable, " - ", total_games_reduced[variable].std())
# bool_columns = total_games_reduced.select_dtypes(include=bool).columns
# total_games_reduced[bool_columns] = total_games_reduced[bool_columns].applymap(int)

# std_values = total_games_reduced.std()

# low_dispersion_threshold = 100.0
# low_dispersion_variables = std_values[std_values > low_dispersion_threshold].index

# sorted_values = std_values.sort_values(ascending=False).index
# subset_total_games = total_games_reduced[sorted_values]
# print(low_dispersion_variables)
# heatmap
# plt.figure(figsize=(12, 8))
# sns.heatmap(subset_total_games.T, cmap='YlGnBu', cbar_kws={'label': 'Standard Deviation'}, linewidths=0.5)
# plt.title('Heatmap of Standard Deviation Values')
# plt.xlabel('Variables')
# plt.ylabel('Data Instances')
# plt.xticks(rotation=90)  # Rotate x-axis labels for better visibility
# plt.show()

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from sklearn.preprocessing import Normalizer
from scipy.stats import ttest_ind

Europe_df = pd.read_csv("games/soloq/Europe_stats.csv")
Asia_df = pd.read_csv("games/soloq/Asia_stats.csv")
total_games = pd.concat([Europe_df, Asia_df])

total_games['creepScore'] = total_games['neutralMinionsKilled'] + total_games['totalMinionsKilled']

clusters_df = pd.read_excel("games/soloq/clusters/clusters.xlsx")
clusters_df = clusters_df[['role', 'group', 'championId']]


top_clusters = clusters_df[clusters_df['role'] == "utility"]
# normalize this data before using it
columns_to_normalize = [
                            'damageDealtToObjectives',
                            'damageDealtToTurrets',
                            'goldEarned',
                            'kills',
                            'assists',
                            'deaths',
                            'magicDamageDealtToChampions',
                            'physicalDamageDealtToChampions',
                            'timeCCingOthers',
                            'totalDamageDealtToChampions',
                            'totalDamageTaken',
                            'totalHealsOnTeammates',
                            'creepScore',
                            'visionScore',
                            'lane_proximity',
                            'jungle_proximity'
                        ]
scaler = Normalizer()
total_games_normalized = total_games
total_games_normalized[columns_to_normalize] = scaler.fit_transform(total_games_normalized[columns_to_normalize])

print(total_games_normalized)
# def visualize_clusters():
# Create a figure to store all the plots
fig = plt.figure(figsize=(12, 8))
# Set a global title for all the plots
fig.suptitle('Utility Clusters', fontsize=16)

# Create a list to store the lines for the legend
plot_list = []

for cluster in pd.unique(top_clusters.group):
    print(cluster+1)
    top_champion_ids = pd.unique(top_clusters[top_clusters['group'] == cluster].championId).tolist()
    top_games = total_games_normalized[total_games_normalized['teamPosition'] == "UTILITY"]
    top_games = top_games[top_games['championId'].isin(top_champion_ids)]
    top_games = top_games[[
                            'damageDealtToObjectives',
                            'damageDealtToTurrets',
                            'goldEarned',
                            'kills',
                            'assists',
                            'deaths',
                            'magicDamageDealtToChampions',
                            'physicalDamageDealtToChampions',
                            'timeCCingOthers',
                            'totalDamageDealtToChampions',
                            'totalDamageTaken',
                            'totalHealsOnTeammates',
                            'creepScore',
                            'visionScore',
                            'lane_proximity',
                            'jungle_proximity'
                        ]]
    
    top_games_column_averages = top_games.mean()
    top_games_means = pd.DataFrame(top_games_column_averages, columns=['Averages']).T

    # Calculate angles for each column
    angles = np.linspace(0, 2 * np.pi, len(top_games_means.columns), endpoint=False)

    # Create the polar subplot for the current cluster
    ax = fig.add_subplot(2, 2, cluster+1, polar=True)

    # Plot the line connecting all values
    values = top_games_means.values.flatten()
    values = np.concatenate((values, [values[0]]))  # Close the shape
    angles = np.concatenate((angles, [angles[0]]))  # Close the shape
    line = ax.plot(angles, values, marker='o')[0]

    # Set radial axis labels as numbers
    ax.set_xticks(angles[:-1])  # Exclude the last angle to avoid repetition
    ax.set_xticklabels(range(1, len(top_games_means.columns) + 1))  # Numeric identifiers

    # Set title
    ax.set_title(f'Cluster: {cluster}')

    # Store the line for the legend
    plot_list.append(line)

# Create the custom legend outside the loop
legend_labels = [
    f'{i+1}: {col}' 
    for i, col in enumerate(top_games_means.columns)
]

# Display the plots
plt.tight_layout()

# Display the legend
# plt.legend(labels=legend_labels, loc='lower right')

# Show the plots
plt.show()


# cluster_df_list = []
# for cluster in pd.unique(top_clusters.group):
#     top_champion_ids = pd.unique(top_clusters[top_clusters['group'] == cluster].championId).tolist()
#     top_games = total_games_normalized[total_games_normalized['teamPosition'] == "TOP"]
#     top_games = top_games[top_games['championId'].isin(top_champion_ids)]
#     top_games = top_games[[
#                             'damageDealtToObjectives',
#                             'damageDealtToTurrets',
#                             'goldEarned',
#                             'kills',
#                             'assists',
#                             'deaths',
#                             'magicDamageDealtToChampions',
#                             'physicalDamageDealtToChampions',
#                             'timeCCingOthers',
#                             'totalDamageDealtToChampions',
#                             'totalDamageTaken',
#                             'totalHealsOnTeammates',
#                             'creepScore',
#                             'visionScore',
#                             'lane_proximity',
#                             'jungle_proximity'
#                         ]]
    
#     top_games_column_averages = top_games.mean()
#     top_games_means = pd.DataFrame(top_games_column_averages, columns=['Averages']).T
#     top_games_means['cluster'] = cluster
#     cluster_df_list.append(top_games_means)

# final_df = pd.concat(cluster_df_list, ignore_index=True)

# cluster_column = final_df.pop('cluster')
# final_df.insert(0, 'cluster', cluster_column)

# significant_rows = []

# for idx, row in final_df.iterrows():
#     other_rows = final_df.drop(idx)
#     p_values = []
#     for column in final_df.columns[1:]:
#         stat, p_value = ttest_ind(row[column], other_rows[column])
#         p_values.append(p_value)
#     if any(p_value < 0.05 for p_value in p_values):
#         significant_rows.append(idx)

# significant_df = final_df.loc[significant_rows]
# print(significant_df)

# final_df.to_excel("top_cluster_stats.xlsx")


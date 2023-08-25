import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import pprint
import numpy as np

games_df = pd.read_excel("games/competitive/total_games_clustered.xlsx")

def get_general_cluster(cluster, role):
    if role == "top":
        return cluster
    if role == "jungle":
        return cluster + 4
    elif role == "mid":
        return cluster + 8
    elif role == "bottom":
        return cluster + 12
    elif role == "utility":
        return cluster + 15

for i in ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]:
    games_df['pick'+i] = games_df.apply(lambda row: get_general_cluster(row['cluster'+i], row['role'+i]), axis=1)
    games_df = games_df.drop(columns=["role"+i, "cluster"+i])

# games_df.to_excel("games/competitive/games_general_clustered.xlsx")

# Extract pick columns
pick_columns = ["pick1", "pick2", "pick3", "pick4", "pick5", "pick6", "pick7", "pick8", "pick9", "pick10"]

# Preprocessing
pick_features = games_df[pick_columns]
print(pick_features)

# Calculate cosine similarity matrix
cosine_sim = cosine_similarity(pick_features, pick_features)

# Define a function to determine if a pick should be excluded based on your conditions
def should_exclude_pick(existing_picks, pick):
    # Define the ranges and their corresponding conditions
    ranges = [(0, 3), (4, 7), (8, 10), (11, 13)]
    conditions = [
        (len(existing_picks) % 2 == 1 and pick % 2 == 1),
        (len(existing_picks) % 2 == 1 and pick % 2 == 0),
        (len(existing_picks) % 2 == 0 and pick % 2 == 1),
        (len(existing_picks) % 2 == 0 and pick % 2 == 0)
    ]
    
    for i, (start, end) in enumerate(ranges):
        if start <= pick <= end and conditions[i]:
            return True
    return False

# Define a function to recommend the next champion pick
def recommend_next_pick(target_draft, existing_picks, pick_features, requested_pick):
    pick_indices = existing_picks

    # prepara los drafts almacenamos
    drafts = []
    for index, picks in pick_features.iterrows():
        draft_instance = [picks['pick1'], picks['pick2'], picks['pick3'], picks['pick4'], picks['pick5'], picks['pick6'], picks['pick7'], picks['pick8'], picks['pick9'], picks['pick10']]        
        drafts.append(draft_instance)


    # agrupa todos los drafts junto con el que se busca recomendación
    drafts.append(pick_indices)
    drafts_array = np.array(drafts)

    # se genera la matriz de similaridad del coseno, esta es cuadrada y muestra la similitud de todos con todos los drafts
    cosine_sim = cosine_similarity(drafts_array)

    # se genera un df con los valores de similitud, indice del draft con similitud hacia el draft a recomendar y la recomendación de siguiente pick
    cosine_sim = cosine_sim[:-1, -1]
    similarity_json = {
        "cosine_similarity": cosine_sim
    }
    similarity_df = pd.DataFrame(similarity_json)
    similarity_df = similarity_df.sort_values(by=['cosine_similarity'], ascending=False)
    similarity_df['recommendation'] = similarity_df.apply(lambda row: drafts[row.name][requested_pick], axis=1)
    print(similarity_df)

    # se obtienen todos los posibles clusters a elegir
    available_picks = [pick for pick in range(1, 18) if pick not in target_draft and not should_exclude_pick(target_draft, pick)]

    # se busca, de mayor a menor similitud el cluster que pueda seleccionarse y se devuelve
    for index, recommendation in similarity_df.iterrows():
        if recommendation['recommendation'] in available_picks:
            return int(recommendation['recommendation'])
        

# Example usage
target_draft = [16, 12, 2]
current_picks = [16, 12, 2]

# rellena el draft solicitado para que coincida con el tamaño de los demás drafts
for x in range(1, 11 - len(current_picks)):
    current_picks.append(-99)

requested_pick = (len(target_draft)+1)
recommended_pick = recommend_next_pick(target_draft, current_picks, pick_features, requested_pick)

print("Recommended next champion pick:", recommended_pick)

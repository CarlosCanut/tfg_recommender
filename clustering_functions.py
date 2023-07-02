from sklearn.datasets import load_digits
from sklearn.model_selection import train_test_split
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.cluster import OPTICS
from sklearn.metrics import silhouette_samples, silhouette_score
from sklearn import metrics
from sklearn.metrics import davies_bouldin_score
import umap.umap_ as umap

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import seaborn as sns
import pandas as pd
from functools import reduce


##############################################################
# Data Cleaning
##############################################################
def clean_data(df, role = "None", patch = "All", stratified_sampling = False):
    games_df = df
    if patch != "All":
        games_df = games_df[ games_df['patch'] == patch]
    else:
        games_df = games_df
        
    if role != "None":
        games_df = games_df[ games_df['teamPosition'] == role ]
    else:
        games_df = games_df
    # list of champions with more than 100 games played
    top_champs = [i for i, x in games_df.championName.value_counts().to_dict().items() if x > 100]
    games_df = games_df[games_df['championName'].isin(top_champs)]
    if stratified_sampling:
        games_df = games_df.groupby(by='championName').apply(lambda x: x.sample(n=100)).reset_index(level=1, drop=True).drop(['championName'], axis=1).reset_index()
    try:
        games_df = games_df.drop(['teamPosition'], axis=1)
        games_df = games_df.drop(['patch'], axis=1)
        games_df = games_df.drop(['Unnamed: 0'], axis=1)
    except Exception as e:
        print(e)
    return games_df

def clean_data_clustering(soloq_games):
    # clean the dataset
    soloq_games = soloq_games.dropna()
    soloq_games = soloq_games[soloq_games['gameEndedInEarlySurrender'] == False]
    soloq_games['patch'] = soloq_games.apply(lambda x: str(x['gameVersion'].split('.')[0] + '.' + x['gameVersion'].split('.')[1]), axis=1 )
    relevant_cols = [
        "patch", "teamPosition", "championId", "championName", "gameDuration", "win",
        "neutralMinionsKilled", "totalMinionsKilled", "cs_diff_at_15",
        "champExperience", "xp_diff", "xp_diff_per_min", "xp_per_min_3_15",
        "damageDealtToBuildings", "damageDealtToObjectives", "damageDealtToTurrets", "damageSelfMitigated", "magicDamageDealt", "magicDamageDealtToChampions", "magicDamageTaken",
        "physicalDamageDealt", "physicalDamageDealtToChampions", "physicalDamageTaken", "totalDamageDealt", "totalDamageDealtToChampions", "totalDamageShieldedOnTeammates",
        "totalDamageTaken", "totalHeal", "totalHealsOnTeammates", "totalUnitsHealed", "trueDamageDealt", "trueDamageDealtToChampions", "trueDamageTaken",
        "totalTimeCCDealt", "timeCCingOthers", "totalTimeSpentDead", "dmg_per_minute_diff", "dmg_per_minute_diff_15", "kills", "deaths", "assists", "kill_share", "kill_participation",
        "doubleKills", "tripleKills", "quadraKills", "pentaKills", "firstBloodAssist", "firstBloodKill", "killingSprees", "largestKillingSpree", "largestMultiKill",
        "goldEarned", "goldSpent", "gold_share", "gold_earned_per_min", "gold_diff_15", "gold_10k_time",
        "inhibitorKills", "inhibitorTakedowns", "inhibitorsLost", 
        "itemsPurchased", "consumablesPurchased",
        "largestCriticalStrike", "longestTimeSpentLiving",
        "firstTowerAssist", "firstTowerKill", "objectivesStolen", "objectivesStolenAssists", "turretKills", "turretTakedowns", "turretsLost",
        "sightWardsBoughtInGame", "visionScore", "visionWardsBoughtInGame", "detectorWardsPlaced", "wardsKilled", "wardsPlaced",
        "spell1Casts", "spell2Casts", "spell3Casts", "spell4Casts", "summoner1Casts", "summoner2Casts",
        "lane_proximity", "jungle_proximity", "percent_mid_lane", "percent_side_lanes", "forward_percentage", "counter_jungle_time_percentage",
    ]
    
    # select only relevant cols
    soloq = soloq_games[ relevant_cols ]
    return soloq


##############################################################
# Clustering
##############################################################
def group_by_champions(df):
    df_champs = df.drop(['championName'], axis=1).groupby("championId").mean().reset_index(level=0)
    x = df_champs.iloc[:,1:]
    y = df_champs.iloc[:,:1]
    return x, y

def standarize_df(df):
    x_role, y_role = group_by_champions(df)
    ## standarize
    role_std_model = StandardScaler()
    x_role_std = role_std_model.fit_transform(x_role)
    
    return x_role_std, y_role

def kmeans_clustering_elbow(df, role="general", total_k = 20):
    distorsions = []
    K = range(1, total_k)
    for k in K:
        kmean_model = KMeans(n_clusters=k)
        kmean_model.fit(df)
        distorsions.append(kmean_model.inertia_)
        
    plt.figure(figsize=(16,8))
    plt.plot(K, distorsions, 'bx-')
    plt.xlabel('k')
    plt.ylabel('Distortion')
    plt.title('The Elbow Method showing the optimal k for ' + role)
    plt.show()
    
def apply_pca(df, variance_explained_specified):
    pca = PCA( variance_explained_specified )
    df = pca.fit_transform(df)
    return df

def apply_umap(df, n_components):
    reducer = umap.UMAP(n_components= n_components)
    df = reducer.fit_transform(df)
    return df

def apply_kmeans(df, k=2):
    kmeans = KMeans(n_clusters = k)
    cluster_labels = kmeans.fit_predict(df)
    
    labels = kmeans.labels_
    silhouette_avg = silhouette_score(df, cluster_labels)
    calinski_harabasz = metrics.calinski_harabasz_score(df, labels)
    davies_bouldin = davies_bouldin_score(df, labels)
    
    return cluster_labels, silhouette_avg, calinski_harabasz, davies_bouldin

def apply_optics(df, min_samples=3):
    optics = OPTICS(min_samples=min_samples)
    cluster_labels = optics.fit_predict(df)
    
    labels = optics.labels_
    silhouette_avg = silhouette_score(df, cluster_labels)
    calinski_harabasz = metrics.calinski_harabasz_score(df, labels)
    davies_bouldin = davies_bouldin_score(df, labels)
    
    return cluster_labels, silhouette_avg, calinski_harabasz, davies_bouldin

def pca_kmeans(x_role, y_role, variance_explained_specified=0.85, k = 2 ):
    ## pca
    pca = PCA( variance_explained_specified )
    role_principal_components = pca.fit_transform(x_role)
    ## k-means
    # kmeans_clustering_elbow(role_principal_components, total_k = 20)
    role_kmeans_model = KMeans(n_clusters= k ).fit(role_principal_components)
    y_role['group'] = role_kmeans_model.predict(role_principal_components)
    y_role
    role_champions_list = y_role.groupby('group')['championId'].apply(list).to_dict()
    
    return y_role, role_champions_list, role_principal_components

def umap_kmeans(x_role, y_role, n_comps= 2 , k = 2 ):
    ## umap
    reducer = umap.UMAP(n_components= n_comps)
    role_umap = reducer.fit_transform(x_role)
    ## k-means
    # kmeans_clustering_elbow(role_umap, total_k = 20)
    role_kmeans_model = KMeans(n_clusters= k ).fit(role_umap)
    y_role['group'] = role_kmeans_model.predict(role_umap)
    role_champions_list = y_role.groupby('group')['championId'].apply(list).to_dict()
    
    return y_role, role_champions_list, role_umap

def umap_optics(x_role, y_role, n_comps= 2 , min_samples = 2 ):
    ## umap
    reducer = umap.UMAP(n_components= n_comps)
    role_umap = reducer.fit_transform(x_role)
    ## optics
    role_optics_model = OPTICS(min_samples=min_samples)
    y_role['group'] = role_optics_model.fit_predict(role_umap)
    role_champions_list = y_role.groupby('group')['championId'].apply(list).to_dict()
    
    return y_role, role_champions_list, role_umap


def get_best_clustering(x_general, pca_params, umap_params, kmeans_params, optics_params):
    results = {"pca": {"kmeans": [], "optics": []}, "umap": {"kmeans": [], "optics": []}}
    try:
        for pca_param in pca_params:
            try:
                x_general_pca = apply_pca(x_general, pca_param)
            except Exception as e:
                print(e)
                continue
            for kmeans_param in kmeans_params:
                try:
                    cluster_labels, silhouette_avg, calinski_harabasz, davies_bouldin = apply_kmeans(x_general_pca, k=kmeans_param)
                    results['pca']['kmeans'].append({
                        "dimentionality reduction": "pca",
                        "clustering": "kmeans",
                        "dimentionality reduction param": pca_param,
                        "clustering param": kmeans_param,
                        "silhouette_avg": silhouette_avg,
                        "calinski_harabasz": calinski_harabasz,
                        "davies_bouldin": davies_bouldin,
                    })
                except Exception as e:
                    print(e)
            for optics_param in optics_params:
                try:
                    cluster_labels, silhouette_avg, calinski_harabasz, davies_bouldin = apply_optics(x_general_pca, min_samples=optics_param)
                    results['pca']['optics'].append({
                        "dimentionality reduction": "pca",
                        "clustering": "optics",
                        "dimentionality reduction param": pca_param,
                        "clustering param": optics_param,
                        "silhouette_avg": silhouette_avg,
                        "calinski_harabasz": calinski_harabasz,
                        "davies_bouldin": davies_bouldin,
                    })
                except Exception as e:
                    print(e)
        for umap_param in umap_params:
            try:
                x_general_umap = apply_umap(x_general, umap_param)
            except Exception as e:
                print(e)
                continue
            for kmeans_param in kmeans_params:
                try:
                    cluster_labels, silhouette_avg, calinski_harabasz, davies_bouldin = apply_kmeans(x_general_umap, k=kmeans_param)
                    results['umap']['kmeans'].append({
                        "dimentionality reduction": "umap",
                        "clustering": "kmeans",
                        "dimentionality reduction param": umap_param,
                        "clustering param": kmeans_param,
                        "silhouette_avg": silhouette_avg,
                        "calinski_harabasz": calinski_harabasz,
                        "davies_bouldin": davies_bouldin,
                    })
                except Exception as e:
                    print(e)
            for optics_param in optics_params:
                try:
                    cluster_labels, silhouette_avg, calinski_harabasz, davies_bouldin = apply_optics(x_general_umap, min_samples=optics_param)
                    results['umap']['optics'].append({
                        "dimentionality reduction": "umap",
                        "clustering": "optics",
                        "dimentionality reduction param": umap_param,
                        "clustering param": optics_param,
                        "silhouette_avg": silhouette_avg,
                        "calinski_harabasz": calinski_harabasz,
                        "davies_bouldin": davies_bouldin,
                    })
                except Exception as e:
                    print(e)


        pca_kmeans_dict = pd.DataFrame.from_dict(results['pca']['kmeans'])
        pca_optics_dict = pd.DataFrame.from_dict(results['pca']['optics'])
        umap_kmeans_dict = pd.DataFrame.from_dict(results['umap']['kmeans'])
        umap_optics_dict = pd.DataFrame.from_dict(results['umap']['optics'])

        results_dict = pd.concat([pca_kmeans_dict, pca_optics_dict, umap_kmeans_dict, umap_optics_dict])
        results_dict = results_dict.sort_values(by=["silhouette_avg", "davies_bouldin", "calinski_harabasz"], ascending=[False, False, True])
        
        return results_dict
    except Exception as e:
        return pd.DataFrame()





##############################################################
# Predictions
##############################################################


def return_champion(row, champions):
    for i, x in champions['championName'].items():
        if x == row['championName']:
            return champions['group'][i]
    return 999

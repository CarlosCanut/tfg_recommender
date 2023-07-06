import pandas as pd
import json
import os
from dotenv import load_dotenv
from riotwatcher import LolWatcher
from soloq_extraction_functions import *
from clustering_functions import *
from competitive_extraction_functions import *
import mwclient
from datetime import datetime
import numpy as np


####################################################################################################################################
### riot API_KEY ->
### extract_soloq_games
### -> [region]_stats.csv
####################################################################################################################################
def extract_soloq_games():
    load_dotenv()
    API_KEY = os.getenv('API_KEY')
    watcher = LolWatcher(API_KEY)

    players = {}
    for region in [{"name": "Europe", "id": "EUW1"}, {"name": "Asia", "id": "KR"}]:
        challenger_league = watcher.league.challenger_by_queue(region['id'], "RANKED_SOLO_5x5")
        challenger_players = [entry['summonerName'] for entry in challenger_league['entries']]
        players[region['name']] = {}
        players[region['name']]['Challenger'] = {}
        for player in challenger_players:
            players[region['name']]['Challenger'][player] = [player]

    try:

        # store players
        with open("games/soloq/players.json", 'w', encoding="utf-8") as outfile:
            json.dump(players, outfile, indent=4, ensure_ascii=False)
            
        # get player's puuids
        puuids = get_puuids(watcher, players)

        # store puuids
        with open("games/soloq/puuids.json", 'w', encoding="utf-8") as outfile:
            json.dump(puuids, outfile, indent=4, ensure_ascii=False)
        
        # get player's games played
        game_ids = get_puuids_games(watcher, puuids)


        # store game ids
        with open("games/soloq/game_ids.json", 'w', encoding="utf-8") as outfile:
            json.dump(game_ids, outfile, indent=4, ensure_ascii=False)

        # extract soloq stats and store them in pandas dfs
        extract_and_store_games_stats(watcher, game_ids, path="games/soloq/")
        
    except Exception as e:
        print(e)
####################################################################################################################################
####################################################################################################################################


####################################################################################################################################
### [region]_stats.csv -> 
### clustering_testing_soloq_games 
### -> games/soloq/clustering_tests/[role]_clustering_[patch]_cluster_size_[k_clusters].xlsx
####################################################################################################################################
def clustering_testing_soloq_games():
    soloq_games_euw = pd.read_csv("games/soloq/Europe_stats.csv")
    soloq_games_kr = pd.read_csv("games/soloq/Asia_stats.csv")

    # group all soloq games
    soloq_games = pd.concat([soloq_games_euw, soloq_games_kr])
    soloq = clean_data_clustering(soloq_games)

    # generate datasets for each role
    patch = "13.10"
    top_soloq = clean_data(soloq, role="TOP", patch=patch, stratified_sampling = False)
    jungle_soloq = clean_data(soloq, role="JUNGLE", patch=patch, stratified_sampling = False)
    mid_soloq = clean_data(soloq, role="MIDDLE", patch=patch, stratified_sampling = False)
    bottom_soloq = clean_data(soloq, role="BOTTOM", patch=patch, stratified_sampling = False)
    utility_soloq = clean_data(soloq, role="UTILITY", patch=patch, stratified_sampling = False)
    
    ####################################################################################################
    # Clustering
    ####################################################################################################

    
    ########### Top stats ###########
    x_top, y_top = standarize_df(top_soloq)
    # kmeans_clustering_elbow(x_top, role="top", total_k=40)
    # test based on each patch, and also try different dimensionality reduction combinations for different cluster numbers
    results_dict = get_best_clustering(
                                        x_top,
                                        pca_params = [0.95, 0.90, 0.85, 0.80],
                                        umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10],
                                        kmeans_params = [3, 4, 5, 6],
                                        optics_params = [2, 3, 4, 5, 6])
    results_dict.to_excel("games/soloq/clustering_tests/top_clustering.xlsx")
    
    
    ########### Jungle stats ###########
    x_jungle, y_jungle = standarize_df(jungle_soloq)
    # kmeans_clustering_elbow(x_jungle, role="jungle", total_k=36)
    # test based on each patch, and also try different dimensionality reduction combinations for different cluster numbers
    results_dict = get_best_clustering(
                                        x_jungle,
                                        pca_params = [0.95, 0.90, 0.85, 0.80],
                                        umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10],
                                        kmeans_params = [3, 4, 5, 6, 7, 8, 9],
                                        optics_params = [2, 3, 4, 5, 6])
    results_dict.to_excel("games/soloq/clustering_tests/jungle_clustering.xlsx")
    

    ########### Mid stats ###########
    x_mid, y_mid = standarize_df(mid_soloq)
    # kmeans_clustering_elbow(x_mid, role="mid", total_k=40)
    # test based on each patch, and also try different dimensionality reduction combinations for different cluster numbers
    results_dict = get_best_clustering(
                                        x_mid,
                                        pca_params = [0.95, 0.90, 0.85, 0.80],
                                        umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10],
                                        kmeans_params = [3, 4, 5, 6],
                                        optics_params = [2, 3, 4, 5, 6])
    results_dict.to_excel("games/soloq/clustering_tests/mid_clustering.xlsx")
    

    ########### Adc stats ###########
    x_bottom, y_bottom = standarize_df(bottom_soloq)
    # kmeans_clustering_elbow(x_bottom, role="bottom", total_k=24)
    # test based on each patch, and also try different dimensionality reduction combinations for different cluster numbers
    results_dict = get_best_clustering(
                                        x_bottom,
                                        pca_params = [0.95, 0.90, 0.85, 0.80],
                                        umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10],
                                        kmeans_params = [3, 4, 5, 6],
                                        optics_params = [2, 3, 4, 5, 6])
    results_dict.to_excel("games/soloq/clustering_tests/bottom_clustering.xlsx")
    
    
    ########### Support stats ###########
    x_utility, y_utility = standarize_df(utility_soloq)
    # kmeans_clustering_elbow(x_utility, role="utility", total_k=26)
    # test based on each patch, and also try different dimensionality reduction combinations for different cluster numbers
    results_dict = get_best_clustering(
                                        x_utility,
                                        pca_params = [0.95, 0.90, 0.85, 0.80],
                                        umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10],
                                        kmeans_params = [3, 4, 5, 6],
                                        optics_params = [2, 3, 4, 5, 6])
    results_dict.to_excel("games/soloq/clustering_tests/utility_clustering.xlsx")

    


####################################################################################################################################
### [region]_stats.csv -> 
### general_clustering 
### -> games/soloq/general_groups.xlsx, games/soloq/general_clusters.xlsx
####################################################################################################################################
def general_clustering():
    soloq_games_euw = pd.read_csv("games/soloq/Europe_stats.csv")
    soloq_games_kr = pd.read_csv("games/soloq/Asia_stats.csv")

    # group all soloq games
    soloq_games = pd.concat([soloq_games_euw, soloq_games_kr])
    # soloq_games = soloq_games_euw
    soloq = clean_data_clustering(soloq_games)

    # patches = {'patch': [], 'games': []}
    # for x in pd.unique(soloq.patch):
    #     patches['patch'].append(x)
    #     patches['games'].append(soloq[soloq['patch'] == x].shape[0])
    # patches_df = pd.DataFrame(patches)
    # print(patches_df.sort_values(["games"], ascending=False))

    patch = "13.10"
    general_soloq = clean_data(soloq, role="None", patch=patch, stratified_sampling = False)

    # create a new df where only the games with the main role for each champion are considered
    def filter_most_common(group):
        mode_value = group['teamPosition'].value_counts().idxmax()
        return group[group['teamPosition'] == mode_value]


    soloq = soloq.groupby('championId').apply(filter_most_common).reset_index(drop=True)

    x_general, y_general = standarize_df(general_soloq)



    ########### General stats ###########
    # kmeans_clustering_elbow(x_general, "general", total_k=50)
    results_dict = get_best_clustering(
                                        x_general,
                                        pca_params = [0.95, 0.90, 0.85, 0.80],
                                        umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10],
                                        kmeans_params = [3, 4, 5, 6, 7, 8, 9, 10, 11],
                                        optics_params = [2, 3, 4, 5, 6])
    results_dict.to_excel("games/soloq/clustering_tests/general_clustering.xlsx")

    ########### General stats ###########
    y_general, general_champions_list, general_principal_components = umap_kmeans(x_general, y_general, n_comps=2, k = 5 )
    # print(general_soloq[general_soloq['championId'] == 1])

    def get_max_role(df, championId):
        roles = {}
        for role in pd.unique(df[df['championId'] == championId]['teamPosition']):
            champ_df = df[df['championId'] == championId]
            roles[role] = champ_df[champ_df['teamPosition'] == role].shape[0]
            return max(roles, key=roles.get)

    y_general['role'] = y_general.apply(lambda row: get_max_role(soloq, row['championId']), axis=1)

    groups = {}
    for group in pd.unique(y_general['group']):
        total_group_roles = {}
        print(group)
        role_df = y_general[y_general['group'] == group]
        for group_role in pd.unique(y_general[y_general['group'] == group]['role']):
           total_group_roles[group_role] = role_df[role_df['role'] == group_role].shape[0]
        
        print(total_group_roles)
        groups[group] = total_group_roles
        print("     ", max(total_group_roles, key=total_group_roles.get))
        print("")
    groups_df = pd.DataFrame(groups)
    groups_df = groups_df.fillna(0)
    groups_df['most_common_group'] = groups_df[np.array(groups_df.columns)].idxmax(axis=1)
    print(groups_df)
    groups_df.to_excel("games/soloq/general_groups.xlsx")
        
    y_general['role'] = 'general'
    y_general.to_excel("games/soloq/clusters/general_clusters.xlsx")


####################################################################################################################################
### [region]_stats.csv -> 
### clustering_soloq_games 
### -> games/soloq/general_groups.xlsx, games/soloq/general_clusters.xlsx
####################################################################################################################################
def clustering_soloq_games():
    soloq_games_euw = pd.read_csv("games/soloq/Europe_stats.csv")
    soloq_games_kr = pd.read_csv("games/soloq/Asia_stats.csv")

    # group all soloq games
    soloq_games = pd.concat([soloq_games_euw, soloq_games_kr])
    # soloq_games = soloq_games_euw
    soloq = clean_data_clustering(soloq_games)
        
    # generate datasets for each role
    patch = "13.10"
    top_soloq = clean_data(soloq, role="TOP", patch=patch, stratified_sampling = False)
    jungle_soloq = clean_data(soloq, role="JUNGLE", patch=patch, stratified_sampling = False)
    mid_soloq = clean_data(soloq, role="MIDDLE", patch=patch, stratified_sampling = False)
    bottom_soloq = clean_data(soloq, role="BOTTOM", patch=patch, stratified_sampling = False)
    utility_soloq = clean_data(soloq, role="UTILITY", patch=patch, stratified_sampling = False)

    ########### Top stats ###########
    x_top, y_top = standarize_df(top_soloq)
    y_top, top_champions_list, top_principal_components = umap_kmeans(x_top, y_top, n_comps=2, k = 4 )
    y_top['role'] = 'top'
    y_top.to_excel("games/soloq/clusters/top_clusters.xlsx")

    ########### Jungle stats ###########
    x_jungle, y_jungle = standarize_df(jungle_soloq)
    y_jungle, jungle_champions_list, jungle_principal_components = umap_kmeans(x_jungle, y_jungle, n_comps=2, k = 4 )
    y_jungle['role'] = 'jungle'
    y_jungle.to_excel("games/soloq/clusters/jungle_clusters.xlsx")

    ########### Mid stats ###########
    x_mid, y_mid = standarize_df(mid_soloq)
    y_mid, mid_champions_list, mid_principal_components = umap_kmeans(x_mid, y_mid, n_comps=2, k = 4 )
    y_mid['role'] = 'mid'
    y_mid.to_excel("games/soloq/clusters/mid_clusters.xlsx")

    ########### Bottom stats ###########
    x_bottom, y_bottom = standarize_df(bottom_soloq)
    y_bottom, bottom_champions_list, bottom_principal_components = umap_kmeans(x_bottom, y_bottom, n_comps = 2, k = 3 )
    y_bottom['role'] = 'bottom'
    y_bottom.to_excel("games/soloq/clusters/bottom_clusters.xlsx")

    ########### Utility stats ###########
    x_utility, y_utility = standarize_df(utility_soloq)
    y_utility, utility_champions_list, utility_principal_components = umap_kmeans(x_utility, y_utility, n_comps=10, k = 3 )
    y_utility['role'] = 'utility'
    y_utility.to_excel("games/soloq/clusters/utility_clusters.xlsx")

    y_general = pd.read_excel("games/soloq/clusters/general_clusters.xlsx")
    clusters_by_role = pd.concat([y_general, y_top, y_jungle, y_mid, y_bottom, y_utility], ignore_index=True)

    clusters_by_role.to_excel("games/soloq/clusters/clusters.xlsx")


    # clean_soloq_games = soloq_games[["game_id", "teamId", "teamPosition", "win", "championName", "championId"]]


####################################################################################################################################
### ->
### extract_competitive_games
### -> games/competitive/leaguepedia_games/[league].xlsx, games/competitive/total_leaguepedia_games.xlsx
####################################################################################################################################
def extract_competitive_games():
    site = mwclient.Site('lol.fandom.com',path='/')
    ##############################


    #######################################################
    ###################### Leagues ########################
    #######################################################
    current_date = datetime.now().strftime("%Y-%m-%d")
    leagues_query = site.api('cargoquery',
        limit='max',
        tables='Tournaments=T',
        fields='T.Name, T.OverviewPage, T.League, T.Year, T.LeagueIconKey, T.Region, T.TournamentLevel, T.IsOfficial, T.DateStart',
        where="Year = 2023 AND IsOfficial = 1 AND TournamentLevel = 'Primary' AND DateStart <= '" + current_date + "'")
    leagues = [item['title']['Name'] for item in leagues_query['cargoquery']]

    ############################################################
    ###################### Champions ID ########################
    ############################################################
    champions_response = site.api('cargoquery',
        limit = 'max',
        tables = "Champions=C",
        fields = "C.Name , C.KeyInteger"
    )
    champions = {}
    for champion in champions_response['cargoquery']:
        champions[champion['title']['Name']] = champion['title']['KeyInteger']

    def get_champ_id(row, column):
        try:
            return champions[row[column]]
        except:
            return ""

    total_leagues_games = []
    drafts_list = []
    for league in leagues:
        ############################################################
        ######################## Patches ###########################
        ############################################################
        try:
            print(league)
            last_date = "2022-11-01"
            old_date = "0"

            patch_response = []
            while True:
                new_response = site.api('cargoquery',
                    limit = 'max',
                    tables = "Tournaments=T, ScoreboardTeams=ST, ScoreboardGames=SG",
                    join_on = "ST.OverviewPage = T.OverviewPage, ST.GameId = SG.GameId",
                    fields = "ST.GameId, SG.Patch, SG.DateTime_UTC",
                    where = "SG.DateTime_UTC > '" + str(last_date) + "' AND T.Name = '" + league + "'",
                    order_by = "SG.DateTime_UTC"
                )
                if old_date == last_date:
                    break
                old_date = last_date
                patch_response = (patch_response + list(new_response['cargoquery']))
                last_date = str(new_response['cargoquery'][-1]['title']['DateTime UTC']).split(" ")[0]


            patches = {}
            for match in patch_response:
                new_patch = str(match['title']['Patch']).replace(",",".")
                patches[match['title']['GameId']] = new_patch


            def get_patch(row):
                return patches[row['GameId']]

            ############################################################
            ############### Player's data extraction ###################
            ############################################################
            last_date = "2022-11-01"
            old_date = "0"

            # player's query
            total_postgame_data = []
            players_response = []
            for x in range(25):
                new_response = site.api('cargoquery',
                    limit = 'max',
                    tables = "Tournaments=T, ScoreboardPlayers=SP",
                    join_on = "SP.OverviewPage = T.OverviewPage",
                    fields = "SP.OverviewPage, SP.GameTeamId, SP.GameId, SP.DateTime_UTC, SP.Link, SP.PlayerWin, SP.Champion, SP.Role, SP.Side , SP.Runes, SP.Team, SP.TeamVs, SP.KeystoneMastery, SP.KeystoneRune, SP.PrimaryTree, SP.SecondaryTree, SP.Items, SP.Trinket, SP.SummonerSpells",
                    where = "SP.DateTime_UTC > '" + str(last_date) + "' AND T.name='" + str(league) + "'",
                    order_by = "SP.DateTime_UTC"
                )
                if old_date == last_date:
                    break
                old_date = last_date
                players_response = (players_response + list(new_response['cargoquery']))
                last_date = str(new_response['cargoquery'][-1]['title']['DateTime UTC']).split(" ")[0]


            total_games = [ game['title'] for game in players_response]
            league_games_df = pd.DataFrame(total_games)



            stats_page_games = []
            unique_league_games = pd.unique(league_games_df['GameId'])
            unique_drafts_picks = getDrafts(league)
            drafts_list.append(pd.DataFrame(unique_drafts_picks))

            # get all drafts

                        
            for game_id in unique_league_games:
                
                match_response = site.api(
                    action = 'cargoquery',
                    limit = 'max',
                    tables = "MatchScheduleGame=MSG, PostgameJsonMetadata=PJM",
                    fields = "MSG.RiotPlatformGameId, MSG.GameId, MSG.Blue, MSG.Red, PJM.StatsPage, PJM.TimelinePage",
                    where= "MSG.GameId='" + game_id + "'",
                    join_on = "MSG.RiotPlatformGameId = PJM.RiotPlatformGameId"
                )
                
                
                
                game_info = match_response['cargoquery'][0]
                stats_page_games.append(game_info)

            stats_games = [ game['title'] for game in stats_page_games]
            stats_page_games_df = pd.DataFrame(stats_games)
                
            games_df = stats_page_games_df.merge(league_games_df, how="inner", on="GameId")
            # games_df.to_excel("test.xlsx")

            for index, unique_game in (games_df.drop_duplicates(subset=["GameId"])).iterrows():
                query_titles = (unique_game['TimelinePage'] + "|" + unique_game['StatsPage'])
                stats = site.api(
                    action = "query",
                    format = "json",
                    prop = "revisions",
                    titles = query_titles,
                    rvprop = "content",
                    rvslots = "main"
                )
                timeline_data = {}
                postgame_data = {}
                for page in stats['query']['pages']:
                    pages = stats['query']['pages']
                    title = pages[page]['title']
                    try:
                        data = json.loads(pages[page]['revisions'][0]['slots']['main']['*'])
                        data['blue_team'] = unique_game['Blue']
                        data['red_team'] = unique_game['Red']
                        data['game_id'] = unique_game['RiotPlatformGameId']
                        data['league'] = unique_game['OverviewPage']
                        if "Timeline" in title:
                            timeline_data = get_timeline_stats(data)
                        else:
                            if "V5" in title:
                                postgame_data = get_postgame_stats_v5(data)
                            elif "V4" in title:
                                postgame_data = get_postgame_stats_v4(data)
                    except Exception as e:
                        print("Error: ",e)
                
                
                final_data = []
                if timeline_data:
                    for post_game_stats_participant in postgame_data:
                        participant_stats = post_game_stats_participant
                        participant_stats['cs_diff_15'] = timeline_data[str(post_game_stats_participant['participantId'])]['cs_diff_15']
                        participant_stats['dmg_diff_15'] = timeline_data[str(post_game_stats_participant['participantId'])]['dmg_diff_15']
                        participant_stats['gold_diff_15'] = timeline_data[str(post_game_stats_participant['participantId'])]['gold_diff_15']
                        participant_stats['xp_diff_15'] = timeline_data[str(post_game_stats_participant['participantId'])]['xp_diff_15']
                        
                        participant_stats['cs_team_15'] = timeline_data[str(post_game_stats_participant['participantId'])]['cs_team_15']
                        participant_stats['cs_share_15'] = timeline_data[str(post_game_stats_participant['participantId'])]['cs_share_15']
                        
                        participant_stats['gold_team_15'] = timeline_data[str(post_game_stats_participant['participantId'])]['gold_team_15']
                        participant_stats['gold_share_15'] = timeline_data[str(post_game_stats_participant['participantId'])]['gold_share_15']
                        
                        participant_stats['dmg_team_15'] = timeline_data[str(post_game_stats_participant['participantId'])]['dmg_team_15']
                        participant_stats['dmg_share_15'] = timeline_data[str(post_game_stats_participant['participantId'])]['dmg_share_15']
                        
                        final_data.append(participant_stats)
                else:
                    final_data = postgame_data
                    
                total_postgame_data = total_postgame_data + final_data
                total_leagues_games = total_leagues_games + final_data
                    
                    
            post_game_player_df = pd.DataFrame(total_postgame_data)
            post_game_player_df.sort_values(['game_id', 'participantId'])
            output = league
            # print(post_game_player_df)
            # print()
            post_game_player_df.to_excel("games/competitive/leaguepedia_games/" + output +".xlsx")
        except Exception as e:
            print(league + " - ERROR: ", e)
            print()

    total_leagues_games_df = pd.DataFrame(total_leagues_games)
    
    drafts_df = pd.concat(drafts_list, ignore_index=True)
    drafts_df = drafts_df.drop_duplicates(subset=["GameId"], ignore_index=True)
    drafts_df = drafts_df.rename(columns={"riotPlatformGameId": "game_id"})
    drafts_df = drafts_df.drop(['OverviewPage', 'Team', 'Team1', 'Team2', 'Side', 'GameId', 'IsWinner', 'patch', 'date', 'vod', 'timestamp'], axis=1)
            
    total_leagues_games_df = total_leagues_games_df.merge(drafts_df, how='inner', on='game_id')
    total_leagues_games_df['orderPickPhase'] = total_leagues_games_df.apply(lambda row: orderPickPhase(row['championId'],row['Team1Pick1Id'], row['Team1Pick2Id'], row['Team1Pick3Id'], row['Team1Pick4Id'], row['Team1Pick5Id'], row['Team2Pick1Id'], row['Team2Pick2Id'], row['Team2Pick3Id'], row['Team2Pick4Id'], row['Team2Pick5Id']), axis=1)
    total_leagues_games_df['orderPick'] = total_leagues_games_df.apply(lambda row: orderPick(row['championId'],row['Team1Pick1Id'], row['Team1Pick2Id'], row['Team1Pick3Id'], row['Team1Pick4Id'], row['Team1Pick5Id'], row['Team2Pick1Id'], row['Team2Pick2Id'], row['Team2Pick3Id'], row['Team2Pick4Id'], row['Team2Pick5Id']), axis=1)
  
    print(total_leagues_games_df[['championId', 'championName', 'win', 'orderPickPhase']])
    
    total_leagues_games_df.sort_values(['game_id', 'participantId'])
    total_leagues_games_df.to_excel("games/competitive/total_leaguepedia_games.xlsx")

####################################################################################################################################
####################################################################################################################################


####################################################################################################################################
### games/competitive/total_leaguepedia_games.xlsx, games/soloq/clusters.xlsx ->
### generate_competitive_clustered_dataset_by_order_pick
### -> games/competitive/total_games_clustered.xlsx
####################################################################################################################################
def generate_competitive_clustered_dataset_by_order_pick():
    # open competitive dataset
    competitive_games = pd.read_excel('games/competitive/total_leaguepedia_games.xlsx')
    competitive_games = competitive_games[['game_id', 'teamPosition', 'teamId', 'championName', 'championId', 'win', 'orderPickPhase', 'orderPick']]

    competitive_games['side_winner'] = competitive_games.apply(lambda row: 100 if (row['teamId'] == 100 and row['win'] == 1) or (row['teamId'] == 200 and row['win'] == 0) else 200, axis=1)
    new_role_names = {
        'Top': 'top',
        'Jungle': 'jungle',
        'Mid': 'mid',
        'Adc': 'bottom',
        'Support': 'utility'
    }
    competitive_games['teamPosition'] = competitive_games['teamPosition'].replace(new_role_names)
    # translate champions into clusters
    clusters = pd.read_excel('games/soloq/clusters/clusters.xlsx')

    competitive_games = competitive_games[competitive_games['championId'].isin(clusters['championId'].values)]

    def get_clusters(row, clusters):
        if row['championId'] in clusters['championId'].values:
            if row['teamPosition'] in clusters[clusters['championId'] == row['championId']]['role'].values:
                return clusters[(clusters['championId'] == row['championId']) & (clusters['role'] == row['teamPosition'])]['group'].values[0]
            return clusters[(clusters['championId'] == row['championId']) & (clusters['role'] == 'general')]['group'].values[0]
        else:
            return None
        
    competitive_games['cluster'] = competitive_games.apply(lambda row: get_clusters(row, clusters), axis=1)



    ### create columns for each cluster by pickOrder
    # Group by 'game_id' and apply a lambda function to aggregate the 'cluster' values
    competitive_clusters = competitive_games.groupby('game_id').apply(lambda x: x.sort_values('orderPick')['cluster'].tolist()).reset_index()
    # Rename the aggregated column
    competitive_clusters.columns = ['game_id', 'merged_clusters']
    # Separate the merged_clusters column into individual columns
    competitive_clusters = pd.concat([competitive_clusters['game_id'], competitive_clusters['merged_clusters'].apply(pd.Series)], axis=1)
    # Rename the individual columns
    competitive_clusters.columns = ['game_id', 'cluster1', 'cluster2', 'cluster3', 'cluster4', 'cluster5', 'cluster6', 'cluster7', 'cluster8', 'cluster9', 'cluster10']



    ### create columns for each role by pickOrder
    # Group by 'game_id' and apply a lambda function to aggregate the 'teamPosition' values
    competitive_roles_df = competitive_games.groupby('game_id').apply(lambda x: x.sort_values('orderPick')['teamPosition'].tolist()).reset_index()
    # Rename the aggregated column
    competitive_roles_df.columns = ['game_id', 'merged_roles']
    # Separate the merged_roles column into individual columns
    competitive_roles_df = pd.concat([competitive_roles_df['game_id'], competitive_roles_df['merged_roles'].apply(pd.Series)], axis=1)
    # Rename the individual columns
    competitive_roles_df.columns = ['game_id', 'role1', 'role2', 'role3', 'role4', 'role5', 'role6', 'role7', 'role8', 'role9', 'role10']



    # Merge 'aggregated_df' with the original 'df' based on 'game_id'
    merged_df = pd.merge(competitive_games, competitive_clusters, on='game_id')
    merged_df = pd.merge(merged_df, competitive_roles_df, on='game_id')

    # Remove the index column generated by groupby
    merged_df = merged_df.drop(['teamPosition', 'championName', 'championId', 'orderPickPhase', 'orderPick', 'teamId', 'win', 'cluster'], axis=1)

    merged_df = merged_df.drop_duplicates(subset=['game_id'])


    merged_df = merged_df.dropna()
    
    merged_df['cluster1'] = merged_df['cluster1'].fillna(-1)
    merged_df['cluster2'] = merged_df['cluster2'].fillna(-1)
    merged_df['cluster3'] = merged_df['cluster3'].fillna(-1)
    merged_df['cluster4'] = merged_df['cluster4'].fillna(-1)
    merged_df['cluster5'] = merged_df['cluster5'].fillna(-1)
    merged_df['cluster6'] = merged_df['cluster6'].fillna(-1)
    merged_df['cluster7'] = merged_df['cluster7'].fillna(-1)
    merged_df['cluster8'] = merged_df['cluster8'].fillna(-1)
    merged_df['cluster9'] = merged_df['cluster9'].fillna(-1)
    merged_df['cluster10'] = merged_df['cluster10'].fillna(-1)

    merged_df['role1'] = merged_df['role1'].fillna("None")
    merged_df['role2'] = merged_df['role2'].fillna("None")
    merged_df['role3'] = merged_df['role3'].fillna("None")
    merged_df['role4'] = merged_df['role4'].fillna("None")
    merged_df['role5'] = merged_df['role5'].fillna("None")
    merged_df['role6'] = merged_df['role6'].fillna("None")
    merged_df['role7'] = merged_df['role7'].fillna("None")
    merged_df['role8'] = merged_df['role8'].fillna("None")
    merged_df['role9'] = merged_df['role9'].fillna("None")
    merged_df['role10'] = merged_df['role10'].fillna("None")

    merged_df.to_excel("games/competitive/total_games_clustered.xlsx")

####################################################################################################################################
####################################################################################################################################


    
    
if __name__ == '__main__':
    # extract_soloq_games()
    # general_clustering()
    # clustering_testing_soloq_games()
    # clustering_soloq_games()
    # extract_competitive_games()
    generate_competitive_clustered_dataset_by_order_pick()

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


####################################################################################################################################
####################################################################################################################################
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
####################################################################################################################################
def clustering_testing_soloq_games():
    soloq_games_euw = pd.read_csv("games/soloq/Europe_stats.csv")
    # soloq_games_kr = pd.read_csv("games/soloq/Asia_stats.csv")

    # group all soloq games
    # soloq_games = pd.concat([soloq_games_euw, soloq_games_kr])
    soloq_games = soloq_games_euw
    soloq = clean_data_clustering(soloq_games)

    for patch in pd.unique(soloq['patch']):
        
        # generate datasets for each role
        general_soloq = clean_data(soloq, role="None", patch=patch, stratified_sampling = False)
        top_soloq = clean_data(soloq, role="TOP", patch=patch, stratified_sampling = False)
        jungle_soloq = clean_data(soloq, role="JUNGLE", patch=patch, stratified_sampling = False)
        mid_soloq = clean_data(soloq, role="MIDDLE", patch=patch, stratified_sampling = False)
        bottom_soloq = clean_data(soloq, role="BOTTOM", patch=patch, stratified_sampling = False)
        utility_soloq = clean_data(soloq, role="UTILITY", patch=patch, stratified_sampling = False)
        
        ####################################################################################################
        # Clustering
        ####################################################################################################
        
        ########### General stats ###########
        x_general, y_general = standarize_df(general_soloq)
        # test based on each patch, and also try different dimensionality reduction combinations for different cluster numbers
        results_dict = get_best_clustering(
                                            x_general,
                                            pca_params = [0.95, 0.90, 0.85, 0.80],
                                            umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10],
                                            kmeans_params = [2, 3, 4, 5, 6],
                                            optics_params = [2, 3, 4, 5, 6])
        results_dict.to_excel("games/soloq/clustering_tests/general_clustering_" + patch + ".xlsx")

        # test for different clusters
        for cluster_n in [2, 3, 4, 5, 6]:
            results_dict = get_best_clustering(
                                                x_general, 
                                                pca_params = [0.95, 0.90, 0.85, 0.80], 
                                                umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10], 
                                                kmeans_params = [cluster_n], 
                                                optics_params = [cluster_n])
            results_dict.to_excel("games/soloq/clustering_tests/general_clustering_" + patch + "_cluster_size_" + cluster_n + ".xlsx")
        

        
        ########### Top stats ###########
        x_top, y_top = standarize_df(top_soloq)
        # test based on each patch, and also try different dimensionality reduction combinations for different cluster numbers
        results_dict = get_best_clustering(
                                            x_top,
                                            pca_params = [0.95, 0.90, 0.85, 0.80],
                                            umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10],
                                            kmeans_params = [2, 3, 4, 5, 6],
                                            optics_params = [2, 3, 4, 5, 6])
        results_dict.to_excel("games/soloq/clustering_tests/top_clustering_" + patch + ".xlsx")

        # test for different clusters
        for cluster_n in [2, 3, 4, 5, 6]:
            results_dict = get_best_clustering(
                                                x_top, 
                                                pca_params = [0.95, 0.90, 0.85, 0.80], 
                                                umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10], 
                                                kmeans_params = [cluster_n], 
                                                optics_params = [cluster_n])
            results_dict.to_excel("games/soloq/clustering_tests/top_clustering_" + patch + "_cluster_size_" + cluster_n + ".xlsx")
        
        
        ########### Jungle stats ###########
        x_jungle, y_jungle = standarize_df(jungle_soloq)
        # test based on each patch, and also try different dimensionality reduction combinations for different cluster numbers
        results_dict = get_best_clustering(
                                            x_jungle,
                                            pca_params = [0.95, 0.90, 0.85, 0.80],
                                            umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10],
                                            kmeans_params = [2, 3, 4, 5, 6],
                                            optics_params = [2, 3, 4, 5, 6])
        results_dict.to_excel("games/soloq/clustering_tests/jungle_clustering_" + patch + ".xlsx")

        # test for different clusters
        for cluster_n in [2, 3, 4, 5, 6]:
            results_dict = get_best_clustering(
                                                x_jungle, 
                                                pca_params = [0.95, 0.90, 0.85, 0.80], 
                                                umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10], 
                                                kmeans_params = [cluster_n], 
                                                optics_params = [cluster_n])
            results_dict.to_excel("games/soloq/clustering_tests/jungle_clustering_" + patch + "_cluster_size_" + cluster_n + ".xlsx")
        

        ########### Mid stats ###########
        x_mid, y_mid = standarize_df(mid_soloq)
        # test based on each patch, and also try different dimensionality reduction combinations for different cluster numbers
        results_dict = get_best_clustering(
                                            x_mid,
                                            pca_params = [0.95, 0.90, 0.85, 0.80],
                                            umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10],
                                            kmeans_params = [2, 3, 4, 5, 6],
                                            optics_params = [2, 3, 4, 5, 6])
        results_dict.to_excel("games/soloq/clustering_tests/mid_clustering_" + patch + ".xlsx")

        # test for different clusters
        for cluster_n in [2, 3, 4, 5, 6]:
            results_dict = get_best_clustering(
                                                x_mid, 
                                                pca_params = [0.95, 0.90, 0.85, 0.80], 
                                                umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10], 
                                                kmeans_params = [cluster_n], 
                                                optics_params = [cluster_n])
            results_dict.to_excel("games/soloq/clustering_tests/mid_clustering_" + patch + "_cluster_size_" + cluster_n + ".xlsx")
        

        ########### Adc stats ###########
        x_bottom, y_bottom = standarize_df(bottom_soloq)
        # test based on each patch, and also try different dimensionality reduction combinations for different cluster numbers
        results_dict = get_best_clustering(
                                            x_bottom,
                                            pca_params = [0.95, 0.90, 0.85, 0.80],
                                            umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10],
                                            kmeans_params = [2, 3, 4, 5, 6],
                                            optics_params = [2, 3, 4, 5, 6])
        results_dict.to_excel("games/soloq/clustering_tests/bottom_clustering_" + patch + ".xlsx")

        # test for different clusters
        for cluster_n in [2, 3, 4, 5, 6]:
            results_dict = get_best_clustering(
                                                x_bottom, 
                                                pca_params = [0.95, 0.90, 0.85, 0.80], 
                                                umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10], 
                                                kmeans_params = [cluster_n], 
                                                optics_params = [cluster_n])
            results_dict.to_excel("games/soloq/clustering_tests/bottom_clustering_" + patch + "_cluster_size_" + cluster_n + ".xlsx")
        
        
        ########### Support stats ###########
        x_utility, y_utility = standarize_df(utility_soloq)
        # test based on each patch, and also try different dimensionality reduction combinations for different cluster numbers
        results_dict = get_best_clustering(
                                            x_utility,
                                            pca_params = [0.95, 0.90, 0.85, 0.80],
                                            umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10],
                                            kmeans_params = [2, 3, 4, 5, 6],
                                            optics_params = [2, 3, 4, 5, 6])
        results_dict.to_excel("games/soloq/clustering_tests/utility_clustering_" + patch + ".xlsx")

        # test for different clusters
        for cluster_n in [2, 3, 4, 5, 6]:
            results_dict = get_best_clustering(
                                                x_utility, 
                                                pca_params = [0.95, 0.90, 0.85, 0.80], 
                                                umap_params = [2, 3, 4, 5, 6, 7, 8, 9, 10], 
                                                kmeans_params = [cluster_n], 
                                                optics_params = [cluster_n])
            results_dict.to_excel("games/soloq/clustering_tests/utility_clustering_" + patch + "_cluster_size_" + cluster_n + ".xlsx")
        
    
    ####################################################################################################
    # Grouped data
    ####################################################################################################
    clean_soloq_games = soloq_games[["game_id", "teamId", "teamPosition", "win", "championName", "championId"]]


####################################################################################################################################
####################################################################################################################################

def clustering_soloq_games():
    soloq_games_euw = pd.read_csv("games/soloq/Europe_stats.csv")
    # soloq_games_kr = pd.read_csv("games/soloq/Asia_stats.csv")

    # group all soloq games
    # soloq_games = pd.concat([soloq_games_euw, soloq_games_kr])
    soloq_games = soloq_games_euw
    soloq = clean_data_clustering(soloq_games)

    general_cases = {
        "13.7": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 6 ),
        "13.8": lambda: umap_kmeans(x_general, y_general, n_comps=8, k = 5 ),
        "13.9": lambda: umap_kmeans(x_general, y_general, n_comps=4, k = 5 ),
        "13.10": lambda: umap_kmeans(x_general, y_general, n_comps=4, k = 5 )
    }

    top_cases = {
        "13.7": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 3 ),
        "13.8": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 2 ),
        "13.9": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 2 ),
        "13.10": lambda: umap_optics(x_top, y_top, n_comps=10, min_samples = 4 )
    }

    jungle_cases = {
        "13.7": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 3 ),
        "13.8": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 2 ),
        "13.9": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 2 ),
        "13.10": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 2 )
    }

    mid_cases = {
        "13.7": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 2 ),
        "13.8": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 3 ),
        "13.9": lambda: umap_kmeans(x_general, y_general, n_comps=3, k = 3 ),
        "13.10": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 2 )
    }

    bottom_cases = {
        "13.7": lambda: pca_kmeans(x_general, y_general, variance_explained_specified=0.80, k = 2 ),
        "13.8": lambda: pca_kmeans(x_general, y_general, variance_explained_specified=0.80, k = 2 ),
        "13.9": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 2 ),
        "13.10": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 2 )
    }

    utility_cases = {
        "13.7": lambda: pca_kmeans(x_general, y_general, variance_explained_specified=0.90, k = 2 ),
        "13.8": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 2 ),
        "13.9": lambda: umap_kmeans(x_general, y_general, n_comps=3, k = 2 ),
        "13.10": lambda: umap_kmeans(x_general, y_general, n_comps=2, k = 3 )
    }

    # for patch in pd.unique(soloq['patch']):
        
    # generate datasets for each role
    patch = "13.10"
    general_soloq = clean_data(soloq, role="None", patch=patch, stratified_sampling = False)
    top_soloq = clean_data(soloq, role="TOP", patch=patch, stratified_sampling = False)
    jungle_soloq = clean_data(soloq, role="JUNGLE", patch=patch, stratified_sampling = False)
    mid_soloq = clean_data(soloq, role="MIDDLE", patch=patch, stratified_sampling = False)
    bottom_soloq = clean_data(soloq, role="BOTTOM", patch=patch, stratified_sampling = False)
    utility_soloq = clean_data(soloq, role="UTILITY", patch=patch, stratified_sampling = False)
    


    ########### General stats ###########
    x_general, y_general = standarize_df(general_soloq)
    y_general, general_champions_list, general_principal_components = umap_kmeans(x_general, y_general, n_comps=4, k = 5 )
    y_general.to_excel("games/soloq/general_clusters.xlsx")

    ########### Top stats ###########
    x_top, y_top = standarize_df(top_soloq)
    y_top, top_champions_list, top_principal_components = umap_kmeans(x_top, y_top, n_comps=4, k = 5 )
    y_top.to_excel("games/soloq/top_clusters.xlsx")

    ########### Jungle stats ###########
    x_jungle, y_jungle = standarize_df(jungle_soloq)
    y_jungle, jungle_champions_list, jungle_principal_components = umap_kmeans(x_jungle, y_jungle, n_comps=4, k = 5 )
    y_jungle.to_excel("games/soloq/jungle_clusters.xlsx")

    ########### Mid stats ###########
    x_mid, y_mid = standarize_df(mid_soloq)
    y_mid, mid_champions_list, mid_principal_components = umap_kmeans(x_mid, y_mid, n_comps=4, k = 5 )
    y_mid.to_excel("games/soloq/mid_clusters.xlsx")

    ########### Bottom stats ###########
    x_bottom, y_bottom = standarize_df(bottom_soloq)
    y_bottom, bottom_champions_list, bottom_principal_components = umap_kmeans(x_bottom, y_bottom, n_comps=4, k = 5 )
    y_bottom.to_excel("games/soloq/bottom_clusters.xlsx")

    ########### Utility stats ###########
    x_utility, y_utility = standarize_df(utility_soloq)
    y_utility, utility_champions_list, utility_principal_components = umap_kmeans(x_utility, y_utility, n_comps=4, k = 5 )
    y_utility.to_excel("games/soloq/utility_clusters.xlsx")

    clean_soloq_games = soloq_games[["game_id", "teamId", "teamPosition", "win", "championName", "championId"]]
    print(clean_soloq_games)


####################################################################################################################################
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
    total_leagues_games_df.to_excel("games/competitive/leaguepedia_games/total_games.xlsx")

####################################################################################################################################
####################################################################################################################################


####################################################################################################################################
####################################################################################################################################
def generate_competitive_clustered_dataset_by_order_pick():
    # open competitive dataset
    competitive_games = pd.read_excel('games/competitive/leaguepedia_games/total_games.xlsx')
    competitive_games = competitive_games[['game_id', 'teamPosition', 'teamId', 'championName', 'championId', 'win', 'orderPickPhase', 'orderPick']]

    competitive_games['side_winner'] = competitive_games.apply(lambda row: 100 if (row['teamId'] == 100 and row['win'] == 1) or (row['teamId'] == 200 and row['win'] == 0) else 200, axis=1)


    # translate champions into clusters
    general_clusters = pd.read_excel('games/soloq/general_clusters.xlsx')
    competitive_games = competitive_games[competitive_games['championId'].isin(general_clusters['championId'].values)]
    competitive_games['cluster'] = competitive_games.apply(lambda row: general_clusters[general_clusters['championId'] == row['championId']]['group'].values[0] if row['championId'] in general_clusters['championId'].values else None, axis=1)



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
    
    merged_df.to_excel("games/competitive/total_games_clustered.xlsx")

####################################################################################################################################
####################################################################################################################################


####################################################################################################################################
####################################################################################################################################
def create_prediction_model():
    # open competitive clustered by order pick dataset
    # create function that takes an input (champions in order pick | max 19) 
    # and returns a cluster that will be the most suited option to choose next in the order pick
    pass
####################################################################################################################################
####################################################################################################################################
    
    
if __name__ == '__main__':
    extract_soloq_games()
    # clustering_testing_soloq_games()
    # clustering_soloq_games()
    # extract_competitive_games()
    # generate_competitive_clustered_dataset_by_order_pick()
    # create_prediction_model()

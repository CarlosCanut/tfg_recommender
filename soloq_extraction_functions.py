import math
import numpy as np
import pandas as pd



##################################################################
### being top lane the highest and bottom the lowest values, returns the height in the map given an x, y position
def get_map_height(x, y):
    x = int( x * (512/14990) )
    y = int( y * (512/14990) )
    map_height = float((max(abs(x-256),abs(y-256))*((abs(x+y-512)/math.sqrt(2))*0.001)) * np.sign(x+y-512))
    return map_height
##################################################################


##################################################################
### get side of the map where an x, y position belongs
def get_map_side_position(x, y):
    x = int( x * (512/14990) )
    y = int( y * (512/14990) )
    slope = (y - 0) / (x - 0)
    if (1 > slope):
        return "Blue"
    else:
        return "Red"
##################################################################
    

##################################################################
### unique from list
def get_unique(list):
    return (set(list))
##################################################################



##################################################################
### return riot api matchlist
def get_matchlist_by_patch(watcher, region, puuid):
    return watcher.match.matchlist_by_puuid(region, puuid, count=100)
##################################################################


##################################################################
##### get puuids from players
def get_puuids(watcher, players):
    puuids = {}
    for region in players:
        region_puuids = {}
        for league in players[region]:
            league_puuids = {}
            for player in players[region][league]:
                player_puuids = []
                for ign in players[region][league][player]:
                    puuid = ""
                    try:
                        if region == "Europe":
                            puuid = watcher.summoner.by_name("euw1", ign)['puuid']
                        elif region == "Asia":
                            puuid = watcher.summoner.by_name("kr", ign)['puuid']
                        elif region == "America":
                            puuid = watcher.summoner.by_name("na1", ign)['puuid']
                    except:
                        pass
                    player_puuids.append(puuid)
                league_puuids.update({player: player_puuids})
            region_puuids.update({league: league_puuids})
        puuids.update({region: region_puuids})
    return puuids
##################################################################


##################################################################
##### get list of games played by player's puuids
def get_puuids_games(watcher, puuids):
    total_match_history = {}
    for region in puuids:
        region_puuids = {}
        region_match_history = []
        for league in puuids[region]:
            league_puuids = {}
            for player in puuids[region][league]:
                player_puuids = []
                for puuid in puuids[region][league][player]:
                    # get player match history
                    try:
                        player_match_history = get_matchlist_by_patch(watcher, region, puuid)
                        region_match_history = region_match_history + player_match_history
                    except Exception as e:
                        print(e)
                        
        region_match_history = get_unique(region_match_history)
        total_match_history[region] = list(region_match_history)
    return total_match_history
##################################################################


##################################################################
##### extract soloq stats from games and store them in pandas dfs
def extract_and_store_games_stats(watcher, game_ids, path):
    for region in game_ids:
        print(region)
        game_region_stats = []
        for game_id in game_ids[region]:
            
            try:
                # game_data = requests.get("https://europe.api.riotgames.com/lol/match/v5/matches/" + game_id + "?api_key=" + API_KEY).json()
                game_data = watcher.match.by_id(region, game_id)
                timeline_data = watcher.match.timeline_by_match(region, game_id)
            except:
                # time.sleep(1)
                continue
            
            game_stats = get_postgame_data(game_id, game_data, timeline_data)
            
            game_region_stats = game_region_stats + game_stats
        
        region_df = pd.DataFrame(game_region_stats)
        region_df.to_csv( path + region + "_stats.csv" )
##################################################################
    
def get_postgame_data(game_id, game_data, timeline_data):
    
    
    total_games_stats = []
    for participant in game_data['info']['participants']:
        try:
            summonerName = ""
            gameVersion = ""
            participantId = ""
            gameDuration = ""
            puuid = ""
            teamId = ""
            teamPosition = ""
            win = ""
            result = ""
            championName = ""
            championId = ""
            champExperience = ""
            assists = ""
            baronKills = ""
            bountyLevel = ""
            champLevel = ""
            consumablesPurchased = ""
            damageDealtToBuildings = ""
            damageDealtToObjectives = ""
            damageDealtToTurrets = ""
            damageSelfMitigated = ""
            deaths = ""
            detectorWardsPlaced = ""
            doubleKills = ""
            dragonKills = ""
            firstBloodAssist = ""
            firstBloodKill = ""
            firstTowerAssist = ""
            firstTowerKill = ""
            gameEndedInEarlySurrender = ""
            gameEndedInSurrender = ""
            goldEarned = ""
            goldSpent = ""
            inhibitorKills = ""
            inhibitorTakedowns = ""
            inhibitorsLost = ""
            item0 = ""
            item1 = ""
            item2 = ""
            item3 = ""
            item4 = ""
            item5 = ""
            item6 = ""
            itemsPurchased = ""
            killingSprees = ""
            kills = ""
            largestCriticalStrike = ""
            largestKillingSpree = ""
            largestMultiKill = ""
            longestTimeSpentLiving = ""
            magicDamageDealt = ""
            magicDamageDealtToChampions = ""
            magicDamageTaken = ""
            neutralMinionsKilled = ""
            nexusKills = ""
            nexusLost = ""
            nexusTakedowns = ""
            objectivesStolen = ""
            objectivesStolenAssists = ""
            pentaKills = ""
            runeStyle = ""
            runeSubStyle = ""
            rune0 = ""
            rune1 = ""
            rune2 = ""
            rune3 = ""
            rune4 = ""
            rune5 = ""
            physicalDamageDealt = ""
            physicalDamageDealtToChampions = ""
            physicalDamageTaken = ""
            quadraKills = ""
            sightWardsBoughtInGame = ""
            spell1Casts = ""
            spell2Casts = ""
            spell3Casts = ""
            spell4Casts = ""
            summoner1Casts = ""
            summoner1Id = ""
            summoner2Casts = ""
            summoner2Id = ""
            teamEarlySurrendered = ""
            timeCCingOthers = ""
            timePlayed = ""
            totalDamageDealt = ""
            totalDamageDealtToChampions = ""
            totalDamageShieldedOnTeammates = ""
            totalDamageTaken = ""
            totalHeal = ""
            totalHealsOnTeammates = ""
            totalMinionsKilled = ""
            totalTimeCCDealt = ""
            totalTimeSpentDead = ""
            totalUnitsHealed = ""
            tripleKills = ""
            trueDamageDealt = ""
            trueDamageDealtToChampions = ""
            trueDamageTaken = ""
            turretKills = ""
            turretTakedowns = ""
            turretsLost = ""
            visionScore = ""
            visionWardsBoughtInGame = ""
            wardsKilled = ""
            wardsPlaced = ""
            
            matchupId = ""
            
            # timeline
            
            
            counter_jungle_time_percentage = 0
            lane_proximity = 0
            jungle_proximity = 0
            forward_percentage = 0
            percent_mid_lane = 0
            percent_side_lanes = 0
            
            
            gold_10k_time = 0
            cs_share_3_15 = 0
            cs_diff_at_15 = 0
            dmg_per_gold_15 = 0
            dmg_per_minute_diff_15 = 0
            gold_diff_15 = 0
            isolated_deaths = "" # Deaths isolated from teammates (i.e. with none of their own teammates nearby).
            kill_participation_15 = ""
            average_mins_between_deaths = ""
            average_mins_between_kills = ""
            num_of_recalls = ""
            num_of_recalls_3_15 = ""
            solo_kills = "" # Kills that are unassisted (no assists on them in-game)
            xp_per_min_3_15 = 0
            gold_xp_diff_15 = 0


            
            dmg_per_gold = 0
            dmg_per_minute_diff = 0
            gold_share = 0
            gold_earned_per_min = 0
            kill_share = 0
            kill_participation = 0
            xp_diff = 0
            xp_diff_per_min = 0
            
            # total team post game
            teamIds = []
            total_team_gold = 0
            total_team_dmg_dealt = 0
            total_team_kills = 0
            
            # matchup post game
            matchup_dmg = 0
            matchup_xp = 0

            # timeline stats
            gold_10k_flag = False
            total_frames = 0
            total_counter_jungle_time_percentage = 0
            total_lane_proximity = 0
            total_jungle_proximity = 0
            total_percent_mid_lane = 0
            total_percent_side_lanes = 0
            total_forward_percentage = 0
            
            total_team_cs_3_15 = 0
            
            cs_3_15 = 0
            cs_15 = 0
            gold_15 = 0
            xp_3_15 = 0
            gold_xp_3_15 = 0
            
            matchup_cs_15 = 0
            matchup_dmg_per_gold_15 = 0
            matchup_gold = 0
            matchup_gold_xp_3_15 = 0
                
                    
                    
            game_id = game_id
            gameVersion = game_data['info']['gameVersion']
            gameDuration = game_data['info']['gameDuration']
            participantId = participant['participantId']
            summonerName = participant['summonerName']
            puuid = participant['puuid']
            teamId = "Blue" if participant['teamId'] == 100 else "Red"
            teamPosition = participant['teamPosition']
            win = participant['win']
            result = "Win" if participant['win'] == True else "Lose"
            championName = participant['championName']
            championId = participant['championId']
            champExperience = participant['champExperience']
            assists = participant['assists']
            baronKills = participant['baronKills']
            bountyLevel = participant['bountyLevel']
            champLevel = participant['champLevel']
            consumablesPurchased = participant['consumablesPurchased']
            damageDealtToBuildings = participant['damageDealtToBuildings']
            damageDealtToObjectives = participant['damageDealtToObjectives']
            damageDealtToTurrets = participant['damageDealtToTurrets']
            damageSelfMitigated = participant['damageSelfMitigated']
            deaths = participant['deaths']
            detectorWardsPlaced = participant['detectorWardsPlaced']
            doubleKills = participant['doubleKills']
            dragonKills = participant['dragonKills']
            firstBloodAssist = participant['firstBloodAssist']
            firstBloodKill = participant['firstBloodKill']
            firstTowerAssist = participant['firstTowerAssist']
            firstTowerKill = participant['firstTowerKill']
            gameEndedInEarlySurrender = participant['gameEndedInEarlySurrender']
            gameEndedInSurrender = participant['gameEndedInSurrender']
            goldEarned = participant['goldEarned']
            goldSpent = participant['goldSpent']
            inhibitorKills = participant['inhibitorKills']
            inhibitorTakedowns = participant['inhibitorTakedowns']
            inhibitorsLost = participant['inhibitorsLost']
            item0 = participant['item0']
            item1 = participant['item1']
            item2 = participant['item2']
            item3 = participant['item3']
            item4 = participant['item4']
            item5 = participant['item5']
            item6 = participant['item6']
            itemsPurchased = participant['itemsPurchased']
            killingSprees = participant['killingSprees']
            kills = participant['kills']
            largestCriticalStrike = participant['largestCriticalStrike']
            largestKillingSpree = participant['largestKillingSpree']
            largestMultiKill = participant['largestMultiKill']
            longestTimeSpentLiving = participant['longestTimeSpentLiving']
            magicDamageDealt = participant['magicDamageDealt']
            magicDamageDealtToChampions = participant['magicDamageDealtToChampions']
            magicDamageTaken = participant['magicDamageTaken']
            neutralMinionsKilled = participant['neutralMinionsKilled']
            nexusKills = participant['nexusKills']
            nexusLost = participant['nexusLost']
            nexusTakedowns = participant['nexusTakedowns']
            objectivesStolen = participant['objectivesStolen']
            objectivesStolenAssists = participant['objectivesStolenAssists']
            pentaKills = participant['pentaKills']
            runeStyle = participant['perks']['styles'][0]['style']
            runeSubStyle = participant['perks']['styles'][1]['style']
            rune0 = participant['perks']['styles'][0]['selections'][0]['perk']
            rune1 = participant['perks']['styles'][0]['selections'][1]['perk']
            rune2 = participant['perks']['styles'][0]['selections'][2]['perk']
            rune3 = participant['perks']['styles'][0]['selections'][3]['perk']
            rune4 = participant['perks']['styles'][1]['selections'][0]['perk']
            rune5 = participant['perks']['styles'][1]['selections'][1]['perk']
            physicalDamageDealt = participant['physicalDamageDealt']
            physicalDamageDealtToChampions = participant['physicalDamageDealtToChampions']
            physicalDamageTaken = participant['physicalDamageTaken']
            quadraKills = participant['quadraKills']
            sightWardsBoughtInGame = participant['sightWardsBoughtInGame']
            spell1Casts = participant['spell1Casts']
            spell2Casts = participant['spell2Casts']
            spell3Casts = participant['spell3Casts']
            spell4Casts = participant['spell4Casts']
            summoner1Casts = participant['summoner1Casts']
            summoner1Id = participant['summoner1Id']
            summoner2Casts = participant['summoner2Casts']
            summoner2Id = participant['summoner2Id']
            teamEarlySurrendered = participant['teamEarlySurrendered']
            timeCCingOthers = participant['timeCCingOthers']
            timePlayed = participant['timePlayed']
            totalDamageDealt = participant['totalDamageDealt']
            totalDamageDealtToChampions = participant['totalDamageDealtToChampions']
            totalDamageShieldedOnTeammates = participant['totalDamageShieldedOnTeammates']
            totalDamageTaken = participant['totalDamageTaken']
            totalHeal = participant['totalHeal']
            totalHealsOnTeammates = participant['totalHealsOnTeammates']
            totalMinionsKilled = participant['totalMinionsKilled']
            totalTimeCCDealt = participant['totalTimeCCDealt']
            totalTimeSpentDead = participant['totalTimeSpentDead']
            totalUnitsHealed = participant['totalUnitsHealed']
            tripleKills = participant['tripleKills']
            trueDamageDealt = participant['trueDamageDealt']
            trueDamageDealtToChampions = participant['trueDamageDealtToChampions']
            trueDamageTaken = participant['trueDamageTaken']
            turretKills = participant['turretKills']
            turretTakedowns = participant['turretTakedowns']
            turretsLost = participant['turretsLost']
            visionScore = participant['visionScore']
            visionWardsBoughtInGame = participant['visionWardsBoughtInGame']
            wardsKilled = participant['wardsKilled']
            wardsPlaced = participant['wardsPlaced']
            
            
            
            # get matchup stats
            for matchup in game_data['info']['participants']:
                if (matchup['teamPosition'] == participant['teamPosition'] and matchup['participantId'] != participant['participantId']):
                    matchupId = matchup['participantId']
                    matchup_dmg = matchup['totalDamageDealtToChampions']
                    matchup_xp = matchup['champExperience']
            
            dmg_per_gold = (participant['totalDamageDealtToChampions'] / participant['goldEarned'])
            gold_earned_per_min = (participant['goldEarned'] / gameDuration)
            dmg_per_minute_diff = ((participant['totalDamageDealtToChampions'] / gameDuration) - (matchup_dmg / gameDuration))
            xp_diff = participant['champExperience'] - matchup_xp
            xp_diff_per_min = ((participant['champExperience'] / gameDuration) - (matchup_xp / gameDuration))
                
                    
            # get ally team stats
            for teammate in game_data['info']['participants']:
                if (participant['teamId'] == teammate['teamId']):
                    teamIds.append(teammate['participantId'])
                    total_team_gold = total_team_gold + teammate['goldEarned']
                    total_team_dmg_dealt = total_team_dmg_dealt + teammate['totalDamageDealtToChampions']
                    total_team_kills = total_team_kills + teammate['kills']
            gold_share = (participant['goldEarned'] / total_team_gold) if total_team_gold != 0 else 0
            kill_share = (participant['kills'] / total_team_kills) if total_team_kills != 0 else 0
            kill_participation = ((participant['kills'] + participant['assists']) / total_team_kills) if total_team_kills != 0 else 0
            
            

        
            
            for frame in timeline_data['info']['frames']:
                if (frame['timestamp'] == 0):
                    continue
                else:
                    frame_min = (frame['timestamp'] / 60000)
                    

                    
                for frameParticipantId in frame['participantFrames']:
                    participantStats = frame['participantFrames'][frameParticipantId]
                    
                    if str(frameParticipantId) == str(participantId):
                        
                        # 10k gold
                        if (participantStats['totalGold'] >= 10000 and not gold_10k_flag):
                            gold_10k_time = frame_min
                            gold_10k_flag = True
                            
                    # mins 3 - 15
                    if 3 <= frame_min <= 15:
                                                
                        total_frames = total_frames + 1
                        frame_jungle_pos = False
                        frame_top_lane_pos = False
                        frame_bot_lane_pos = False
                        frame_mid_lane_pos = False
                        frame_enemy_side_pos = False
                        
                        for newFrameParticipantId in frame['participantFrames']:
                            newParticipantStats = frame['participantFrames'][newFrameParticipantId]
                            
                            ##### teammates timeline
                            for teammateIdTimeline in teamIds:
                                if str(teammateIdTimeline) == str(newFrameParticipantId):
                                    total_team_cs_3_15 = total_team_cs_3_15 + (newParticipantStats['minionsKilled'] + newParticipantStats['jungleMinionsKilled'])
                                
                            #####
                            
                            ##### matchup timeline
                            if str(newFrameParticipantId) == str(matchupId):
                                matchup_cs_15 = (newParticipantStats['minionsKilled'] + newParticipantStats['jungleMinionsKilled'])
                                matchup_dmg_per_gold_15 = (newParticipantStats['damageStats']['totalDamageDoneToChampions'] / newParticipantStats['totalGold'])
                                matchup_gold = newParticipantStats['totalGold']
                                matchup_gold_xp_3_15 = matchup_gold_xp_3_15 + (newParticipantStats['totalGold'] + newParticipantStats['xp'])
                                
                            #####
                            
                            
                        ##### player timeline
                        if str(frameParticipantId) == str(participantId):
                            
                            position_x = participantStats['position']['x']
                            position_y = participantStats['position']['y']
                            
                            position_z = get_map_height(position_x, position_y)
                            
                            if ((-30.0) >= position_z):
                                # top
                                frame_top_lane_pos = True
                            elif (-1.8) <= position_z <= 1.6:
                                # mid
                                frame_mid_lane_pos = True
                            elif 30 <= position_z:
                                # bot
                                frame_bot_lane_pos = True
                            else:
                                # jungle
                                frame_jungle_pos = True
                            
                            
                            map_side_position = get_map_side_position(position_x, position_y)
                            
                            if (map_side_position != teamId):
                                # player in enemy map side
                                frame_enemy_side_pos = True
                            
                            
                            if (frame_mid_lane_pos or frame_top_lane_pos or frame_bot_lane_pos):
                                total_lane_proximity = total_lane_proximity + 1
                                
                            if (frame_jungle_pos):
                                total_jungle_proximity = total_jungle_proximity + 1
                                
                            if (frame_mid_lane_pos):
                                total_percent_mid_lane = total_percent_mid_lane + 1
                                
                                
                            if (frame_top_lane_pos or frame_bot_lane_pos):
                                total_percent_side_lanes = total_percent_side_lanes + 1
                                
                                
                            if (frame_enemy_side_pos):
                                total_forward_percentage = total_forward_percentage + 1
                                
                                
                            if (frame_enemy_side_pos and frame_jungle_pos):
                                total_counter_jungle_time_percentage = total_counter_jungle_time_percentage + 1
                                
                            
                                
                            cs_3_15 = cs_3_15 + (participantStats['minionsKilled'] + participantStats['jungleMinionsKilled'])
                            cs_15 = (participantStats['minionsKilled'] + participantStats['jungleMinionsKilled'])
                            gold_15 = participantStats['totalGold']
                            xp_3_15 = xp_3_15 + participantStats['xp']
                            gold_xp_3_15 = gold_xp_3_15 + (participantStats['totalGold'] + participantStats['xp'])
                            
                            dmg_per_gold_15 = (participantStats['damageStats']['totalDamageDoneToChampions'] / participantStats['totalGold'])
                            
                        #####
                        
            ##### calculate timeline stats
            cs_share_3_15 = cs_3_15 / total_team_cs_3_15 if total_team_cs_3_15 != 0 else 0
            cs_diff_at_15 = cs_15 - matchup_cs_15 if matchup_cs_15 != 0 else 0
            dmg_per_minute_diff_15 = dmg_per_gold_15 - matchup_dmg_per_gold_15 if matchup_dmg_per_gold_15 != 0 else 0
            gold_diff_15 = gold_15 - matchup_gold if matchup_gold != 0 else 0
            xp_per_min_3_15 = xp_3_15 / gameDuration if gameDuration != 0 else 0
            gold_xp_diff_15 = gold_xp_3_15 - matchup_gold_xp_3_15 if matchup_gold_xp_3_15 != 0 else 0
            
            counter_jungle_time_percentage = (total_counter_jungle_time_percentage / total_frames) * 10 if total_frames != 0 else 0
            lane_proximity = (total_lane_proximity / total_frames) * 10 if total_frames != 0 else 0
            jungle_proximity = (total_jungle_proximity / total_frames) * 10 if total_frames != 0 else 0
            percent_mid_lane = (total_percent_mid_lane / total_frames) * 10 if total_frames != 0 else 0
            percent_side_lanes = (total_percent_side_lanes / total_frames) * 10 if total_frames != 0 else 0
            forward_percentage = (total_forward_percentage / total_frames) * 10 if total_frames != 0 else 0
                        
            
            game_stats = {
                "game_id": game_id,
                "gameVersion": gameVersion,
                "gameDuration": gameDuration,
                "summonerName": summonerName,
                "puuid": puuid,
                "participantId": participantId,
                "teamId": teamId,
                "teamPosition": teamPosition,
                "win": win,
                "result": result,
                "championName": championName,
                "championId": championId,
                "champExperience": champExperience,
                "assists": assists,
                "baronKills": baronKills,
                "bountyLevel": bountyLevel,
                "champLevel": champLevel,
                "consumablesPurchased": consumablesPurchased,
                "damageDealtToBuildings": damageDealtToBuildings,
                "damageDealtToObjectives": damageDealtToObjectives,
                "damageDealtToTurrets": damageDealtToTurrets,
                "damageSelfMitigated": damageSelfMitigated,
                "deaths": deaths,
                "detectorWardsPlaced": detectorWardsPlaced,
                "doubleKills": doubleKills,
                "dragonKills": dragonKills,
                "firstBloodAssist": firstBloodAssist,
                "firstBloodKill": firstBloodKill,
                "firstTowerAssist": firstTowerAssist,
                "firstTowerKill": firstTowerKill,
                "gameEndedInEarlySurrender": gameEndedInEarlySurrender,
                "gameEndedInSurrender": gameEndedInSurrender,
                "goldEarned": goldEarned,
                "goldSpent": goldSpent,
                "inhibitorKills": inhibitorKills,
                "inhibitorTakedowns": inhibitorTakedowns,
                "inhibitorsLost": inhibitorsLost,
                "item0": item0,
                "item1": item1,
                "item2": item2,
                "item3": item3,
                "item4": item4,
                "item5": item5,
                "item6": item6,
                "itemsPurchased": itemsPurchased,
                "killingSprees": killingSprees,
                "kills": kills,
                "largestCriticalStrike": largestCriticalStrike,
                "largestKillingSpree": largestKillingSpree,
                "largestMultiKill": largestMultiKill,
                "longestTimeSpentLiving": longestTimeSpentLiving,
                "magicDamageDealt": magicDamageDealt,
                "magicDamageDealtToChampions": magicDamageDealtToChampions,
                "magicDamageTaken": magicDamageTaken,
                "neutralMinionsKilled": neutralMinionsKilled,
                "nexusKills": nexusKills,
                "nexusLost": nexusLost,
                "nexusTakedowns": nexusTakedowns,
                "objectivesStolen": objectivesStolen,
                "objectivesStolenAssists": objectivesStolenAssists,
                "pentaKills": pentaKills,
                "runeStyle": runeStyle,
                "runeSubStyle": runeSubStyle,
                "rune0": rune0,
                "rune1": rune1,
                "rune2": rune2,
                "rune3": rune3,
                "rune4": rune4,
                "rune5": rune5,
                "physicalDamageDealt": physicalDamageDealt,
                "physicalDamageDealtToChampions": physicalDamageDealtToChampions,
                "physicalDamageTaken": physicalDamageTaken,
                "quadraKills": quadraKills,
                "sightWardsBoughtInGame": sightWardsBoughtInGame,
                "spell1Casts": spell1Casts,
                "spell2Casts": spell2Casts,
                "spell3Casts": spell3Casts,
                "spell4Casts": spell4Casts,
                "summoner1Casts": summoner1Casts,
                "summoner1Id": summoner1Id,
                "summoner2Casts": summoner2Casts,
                "summoner2Id": summoner2Id,
                "teamEarlySurrendered": teamEarlySurrendered,
                "timeCCingOthers": timeCCingOthers,
                "timePlayed": timePlayed,
                "totalDamageDealt": totalDamageDealt,
                "totalDamageDealtToChampions": totalDamageDealtToChampions,
                "totalDamageShieldedOnTeammates": totalDamageShieldedOnTeammates,
                "totalDamageTaken": totalDamageTaken,
                "totalHeal": totalHeal,
                "totalHealsOnTeammates": totalHealsOnTeammates,
                "totalMinionsKilled": totalMinionsKilled,
                "totalTimeCCDealt": totalTimeCCDealt,
                "totalTimeSpentDead": totalTimeSpentDead,
                "totalUnitsHealed": totalUnitsHealed,
                "tripleKills": tripleKills,
                "trueDamageDealt": trueDamageDealt,
                "trueDamageDealtToChampions": trueDamageDealtToChampions,
                "trueDamageTaken": trueDamageTaken,
                "turretKills": turretKills,
                "turretTakedowns": turretTakedowns,
                "turretsLost": turretsLost,
                "visionScore": visionScore,
                "visionWardsBoughtInGame": visionWardsBoughtInGame,
                "wardsKilled": wardsKilled,
                "wardsPlaced": wardsPlaced,
                "dmg_per_gold": dmg_per_gold,
                "dmg_per_minute_diff": dmg_per_minute_diff,
                "gold_share": gold_share,
                "gold_earned_per_min": gold_earned_per_min,
                "kill_share": kill_share,
                "kill_participation": kill_participation,
                "xp_diff": xp_diff,
                "xp_diff_per_min": xp_diff_per_min,
                "gold_10k_time": gold_10k_time,
                "cs_diff_at_15": cs_diff_at_15,
                "dmg_per_gold_15": dmg_per_gold_15,
                "dmg_per_minute_diff_15": dmg_per_minute_diff_15,
                "gold_diff_15": gold_diff_15,
                "xp_per_min_3_15": xp_per_min_3_15,
                "gold_xp_diff_15": gold_xp_diff_15,
                "lane_proximity": lane_proximity,
                "jungle_proximity": jungle_proximity,
                "percent_mid_lane": percent_mid_lane,
                "percent_side_lanes": percent_side_lanes,
                "forward_percentage": forward_percentage,
                "counter_jungle_time_percentage": counter_jungle_time_percentage
            }
            
            total_games_stats.append(game_stats)
        
        except Exception as e:
            print(e)
            
            
    return total_games_stats

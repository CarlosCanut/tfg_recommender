import mwclient
import pandas as pd
from datetime import date as date_x
from datetime import timedelta
import time
from collections import OrderedDict
import json
import requests


def orderPickPhase(championSelected, Team1Pick1, Team1Pick2, Team1Pick3, Team1Pick4, Team1Pick5, Team2Pick1, Team2Pick2, Team2Pick3, Team2Pick4, Team2Pick5):
    if (str(championSelected) == str(Team1Pick1)):
        return 1
    elif (str(championSelected) == str(Team2Pick1)):
        return 2
    elif (str(championSelected) == str(Team2Pick2)):
        return 2
    elif (str(championSelected) == str(Team1Pick2)):
        return 3
    elif (str(championSelected) == str(Team1Pick3)):
        return 3
    elif (str(championSelected) == str(Team2Pick3)):
        return 4
    elif (str(championSelected) == str(Team2Pick4)):
        return 5
    elif (str(championSelected) == str(Team1Pick4)):
        return 6
    elif (str(championSelected) == str(Team1Pick5)):
        return 6
    elif (str(championSelected) == str(Team2Pick5)):
        return 7
    else:
        return -1
    
def orderPick(championSelected, Team1Pick1, Team1Pick2, Team1Pick3, Team1Pick4, Team1Pick5, Team2Pick1, Team2Pick2, Team2Pick3, Team2Pick4, Team2Pick5):
    if (str(championSelected) == str(Team1Pick1)):
        return 1
    elif (str(championSelected) == str(Team2Pick1)):
        return 2
    elif (str(championSelected) == str(Team2Pick2)):
        return 3
    elif (str(championSelected) == str(Team1Pick2)):
        return 4
    elif (str(championSelected) == str(Team1Pick3)):
        return 5
    elif (str(championSelected) == str(Team2Pick3)):
        return 6
    elif (str(championSelected) == str(Team2Pick4)):
        return 7
    elif (str(championSelected) == str(Team1Pick4)):
        return 8
    elif (str(championSelected) == str(Team1Pick5)):
        return 9
    elif (str(championSelected) == str(Team2Pick5)):
        return 10
    else:
        return -1


def get_timeline_stats(data):
    timeline_data = {
        '1': {},
        '2': {},
        '3': {},
        '4': {},
        '5': {},
        '6': {},
        '7': {},
        '8': {},
        '9': {},
        '10': {}
    }
    
    
    for frame in data['frames']:
        if (frame['timestamp'] == 0):
            continue
        else:
            frame_min = (frame['timestamp'] / 60000)
        
        if 10 <= frame_min <= 15:
                        
            for participantStatsId, participantStats in frame['participantFrames'].items():
                
                participant_cs = participantStats['minionsKilled'] + participantStats['jungleMinionsKilled']
                participant_dmg = participantStats['damageStats']['totalDamageDoneToChampions']
                participant_gold = participantStats['totalGold']
                participant_xp = participantStats['xp']
                
                ##### teammates timeline
                team_cs = 0
                team_gold = 0
                team_dmg = 0
                for teamMateStatsId, teamMateStats in frame['participantFrames'].items():
                    if (int(teamMateStatsId) <= 5 and int(participantStatsId) <= 5) or (int(teamMateStatsId) > 5 and int(participantStatsId) > 5):
                        team_cs = team_cs + (teamMateStats['minionsKilled'] + teamMateStats['jungleMinionsKilled'])
                        team_gold = team_gold + teamMateStats['totalGold']
                        team_dmg = team_dmg + teamMateStats['damageStats']['totalDamageDoneToChampions']
                        
                        
                ##### matchup timeline
                matchup_cs = 0
                matchup_dmg = 0
                matchup_gold = 0
                matchup_xp = 0
                for matchupStatsId, matchupStats in frame['participantFrames'].items():
                    if (int(participantStatsId) + 5 == int(matchupStatsId)) or (int(participantStatsId) - 5 == int(matchupStatsId)):
                        matchup_cs = matchupStats['minionsKilled'] + matchupStats['jungleMinionsKilled']
                        matchup_dmg = matchupStats['damageStats']['totalDamageDoneToChampions']
                        matchup_gold = matchupStats['totalGold']
                        matchup_xp = matchupStats['xp']
                        
                timeline_data[str(participantStatsId)]['cs_diff_15'] = participant_cs - matchup_cs
                timeline_data[str(participantStatsId)]['dmg_diff_15'] = participant_dmg - matchup_dmg
                timeline_data[str(participantStatsId)]['gold_diff_15'] = participant_gold - matchup_gold
                timeline_data[str(participantStatsId)]['xp_diff_15'] = participant_xp - matchup_xp
                
                timeline_data[str(participantStatsId)]['cs_team_15'] = team_cs
                timeline_data[str(participantStatsId)]['cs_share_15'] = participant_cs / team_cs
                timeline_data[str(participantStatsId)]['gold_team_15'] = team_gold
                timeline_data[str(participantStatsId)]['gold_share_15'] = participant_gold / team_gold
                timeline_data[str(participantStatsId)]['dmg_team_15'] = team_dmg
                timeline_data[str(participantStatsId)]['dmg_share_15'] = participant_dmg / team_dmg
                
                
                
        if frame_min > 15:
            break
        
    
    return timeline_data
            


def get_postgame_stats_v4(data):
    
    postgame_data = []
    
    game_id = data['game_id']
    blue_team = data['blue_team']
    red_team = data['red_team']
    league = data['league']
    
    participant_data = {
            "game_id": game_id,
            "league": league,
            "blue_team": blue_team,
            "red_team": red_team,
            "summonerId": "",
            "participantId": "",
            "summonerName": "",
            "teamId": "",
            "team": "",
            "team_vs": "",
            "teamPosition": "",
            "assists": "",
            "baronKills": "",
            "bountyLevel": "",
            "champExperience": "",
            "champLevel": "",
            "championId": "",
            "championName": "",
            "consumablesPurchased": "",
            "damageDealtToBuildings": "",
            "damageDealtToObjectives": "",
            "damageDealtToTurrets": "",
            "damageSelfMitigated": "",
            "deaths": "",
            "detectorWardsPlaced": "",
            "doubleKills": "",
            "dragonKills": "",
            "firstBloodAssist": "",
            "firstBloodKill": "",
            "firstTowerAssist": "",
            "firstTowerKill": "",
            "gameEndedInEarlySurrender": "",
            "gameEndedInSurrender": "",
            "goldEarned": "",
            "goldSpent": "",
            "inhibitorKills": "",
            "inhibitorTakedowns": "",
            "inhibitorsLost": "",
            "item0": "",
            "item1": "",
            "item2": "",
            "item3": "",
            "item4": "",
            "item5": "",
            "item6": "",
            "itemsPurchased": "",
            "killingSprees": "",
            "kills": "",
            "largestCriticalStrike": "",
            "largestKillingSpree": "",
            "largestMultiKill": "",
            "longestTimeSpentLiving": "",
            "magicDamageDealt": "",
            "magicDamageDealtToChampions": "",
            "magicDamageTaken": "",
            "neutralMinionsKilled": "",
            "nexusKills": "",
            "nexusLost": "",
            "nexusTakedowns": "",
            "objectivesStolen": "",
            "objectivesStolenAssists": "",
            "pentaKills": "",
            "physicalDamageDealt": "",
            "physicalDamageDealtToChampions": "",
            "physicalDamageTaken": "",
            "quadraKills": "",
            "sightWardsBoughtInGame": "",
            "spell1Casts": "",
            "spell1Id": "",
            "spell2Casts": "",
            "spell2Id": "",
            "spell3Casts": "",
            "spell4Casts": "",
            "summoner1Casts": "",
            "summoner2Casts": "",
            "timeCCingOthers": "",
            "timePlayed": "",
            "totalDamageDealt": "",
            "totalDamageDealtToChampions": "",
            "totalDamageShieldedOnTeammates": "",
            "totalDamageTaken": "",
            "totalHeal": "",
            "totalHealsOnTeammates": "",
            "totalMinionsKilled": "",
            "totalTimeCCDealt": "",
            "totalTimeSpentDead": "",
            "totalUnitsHealed": "",
            "tripleKills": "",
            "trueDamageDealt": "",
            "trueDamageDealtToChampions": "",
            "trueDamageTaken": "",
            "turretKills": "",
            "turretTakedowns": "",
            "turretsLost": "",
            "unrealKills": "",
            "visionScore": "",
            "visionWardsBoughtInGame": "",
            "wardsKilled": "",
            "wardsPlaced": "",
            "win": "",
            "rune0": "",
            "rune1": "",
            "rune2": "",
            "rune3": "",
            "rune4": "",
            "rune5": "",
        }
    postgame_data.append(participant_data)
        
    return postgame_data
    


def get_postgame_stats_v5(data):
    
    postgame_data = []
    
    game_id = data['game_id']
    blue_team = data['blue_team']
    red_team = data['red_team']
    league = data['league']

    for participant in data['participants']:
        gameDuration = data['gameDuration']
        assists = participant['assists']
        baronKills = participant['baronKills']
        bountyLevel = participant['bountyLevel']
        champExperience = participant['champExperience']
        champLevel = participant['champLevel']
        championId = participant['championId']
        championName = participant['championName']
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
        participantId = participant['participantId']
        pentaKills = participant['pentaKills']
        physicalDamageDealt = participant['physicalDamageDealt']
        physicalDamageDealtToChampions = participant['physicalDamageDealtToChampions']
        physicalDamageTaken = participant['physicalDamageTaken']
        quadraKills = participant['quadraKills']
        sightWardsBoughtInGame = participant['sightWardsBoughtInGame']
        spell1Casts = participant['spell1Casts']
        spell1Id = participant['spell1Id']
        spell2Casts = participant['spell2Casts']
        spell2Id = participant['spell2Id']
        spell3Casts = participant['spell3Casts']
        spell4Casts = participant['spell4Casts']
        summoner1Casts = participant['summoner1Casts']
        summoner2Casts = participant['summoner2Casts']
        summonerName = participant['summonerName']
        teamId = participant['teamId']
        if teamId == 100:
            team = blue_team
            team_vs = red_team
        else:
            team = red_team
            team_vs = blue_team
        if participantId == 1 or participantId == 6:
            teamPosition = "Top"
        elif participantId == 2 or participantId == 7:
            teamPosition = "Jungle"
        elif participantId == 3 or participantId == 8:
            teamPosition = "Mid"
        elif participantId == 4 or participantId == 9:
            teamPosition = "Adc"
        elif participantId == 5 or participantId == 10:
            teamPosition = "Support"
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
        unrealKills = participant['unrealKills']
        visionScore = participant['visionScore']
        visionWardsBoughtInGame = participant['visionWardsBoughtInGame']
        wardsKilled = participant['wardsKilled']
        wardsPlaced = participant['wardsPlaced']
        if participant['win'] == True:
            win = 1
        else:
            win = 0
        rune0 = participant['perks']['styles'][0]['selections'][0]['perk']
        rune1 = participant['perks']['styles'][0]['selections'][1]['perk']
        rune2 = participant['perks']['styles'][0]['selections'][2]['perk']
        rune3 = participant['perks']['styles'][0]['selections'][3]['perk']
        rune4 = participant['perks']['styles'][1]['selections'][0]['perk']
        rune5 = participant['perks']['styles'][1]['selections'][1]['perk']
        summonerId = participant['summonerId']
        
        
        dmg_share = 0
        gold_share = 0
        kill_share = 0
        kp = 0
        
        team_dmg = 0
        team_gold = 0
        team_kills = 0
        for teamMateStats in data['participants']:
            if str(teamMateStats['teamId']) == str(teamId):
                team_dmg = team_dmg + teamMateStats['totalDamageDealtToChampions']
                team_gold = team_gold + teamMateStats['goldEarned']
                team_kills = team_kills + teamMateStats['kills']
                
        dmg_share = totalDamageDealtToChampions / team_dmg
        gold_share = goldEarned / team_gold
        if kills == 0 or team_kills == 0:
            kill_share = 0
        else:
            kill_share = kills / team_kills
            
        if (kills + assists) == 0 or team_kills == 0:
            kp = 0
        else:
            kp = (kills + assists) / team_kills
    
        participant_data = {
            "game_id": game_id,
            "league": league,
            "gameDuration": gameDuration,
            "blue_team": blue_team,
            "red_team": red_team,
            "summonerId": summonerId,
            "participantId": participantId,
            "summonerName": summonerName,
            "teamId": teamId,
            "team": team,
            "team_vs": team_vs,
            "teamPosition": teamPosition,
            "assists": assists,
            "baronKills": baronKills,
            "bountyLevel": bountyLevel,
            "champExperience": champExperience,
            "champLevel": champLevel,
            "championId": championId,
            "championName": championName,
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
            "physicalDamageDealt": physicalDamageDealt,
            "physicalDamageDealtToChampions": physicalDamageDealtToChampions,
            "physicalDamageTaken": physicalDamageTaken,
            "quadraKills": quadraKills,
            "sightWardsBoughtInGame": sightWardsBoughtInGame,
            "spell1Casts": spell1Casts,
            "spell1Id": spell1Id,
            "spell2Casts": spell2Casts,
            "spell2Id": spell2Id,
            "spell3Casts": spell3Casts,
            "spell4Casts": spell4Casts,
            "summoner1Casts": summoner1Casts,
            "summoner2Casts": summoner2Casts,
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
            "unrealKills": unrealKills,
            "visionScore": visionScore,
            "visionWardsBoughtInGame": visionWardsBoughtInGame,
            "wardsKilled": wardsKilled,
            "wardsPlaced": wardsPlaced,
            "win": win,
            "rune0": rune0,
            "rune1": rune1,
            "rune2": rune2,
            "rune3": rune3,
            "rune4": rune4,
            "rune5": rune5,
            "dmg_share": dmg_share,
            "gold_share": gold_share,
            "kill_share": kill_share,
            "kp": kp
        }
        postgame_data.append(participant_data)
        
    return postgame_data


def getDrafts(league):    
    site = mwclient.Site('lol.fandom.com',path='/')
    ############################################################
    #################### Runes & Summoners #####################
    ############################################################
    patch = requests.get("https://ddragon.leagueoflegends.com/api/versions.json").json()[0]

    ### runes
    runes = {}
    keyStones = {}
    
    all_runes = requests.get("https://ddragon.leagueoflegends.com/cdn/" + patch + "/data/en_US/runesReforged.json").json()
    
    for path_rune in all_runes:
        keyStones.update({path_rune['key']: path_rune['icon']})
        for slot in path_rune['slots']:
            for rune in slot['runes']:
                runes.update({rune['name']: rune['icon']})
                
    ### summoners
    summoners = {}
    
    all_summoners = requests.get("https://ddragon.leagueoflegends.com/cdn/" + patch + "/data/en_US/summoner.json").json()
    for summoner in all_summoners['data']:
        summoners.update({all_summoners['data'][summoner]['name']: all_summoners['data'][summoner]['image']['full']})

    def get_tournament_drafts(site, patch, summoners, runes, league):
        
        ############################################################
        ###################### Champions ID ########################
        ############################################################
        champions_response = site.api('cargoquery',
            limit = 'max',
            tables = "Champions=C",
            fields = "C.Name , C.KeyInteger"
        )
        champions = {}
        # sets champion ids
        for champion in champions_response['cargoquery']:
            champions[champion['title']['Name']] = champion['title']['KeyInteger']
            if (champion['title']['Name'] == "Renata Glasc"):
                champions[champion['title']['Name']] = "888"
            if (champion['title']['Name'] == "Zeri"):
                champions[champion['title']['Name']] = "221"
            if (champion['title']['Name'] == "Bel'Veth"):
                champions[champion['title']['Name']] = "200"

        def get_champ_id(row, column):
            try:
                return champions[row[column]]
            except:
                return ""
            
            
        def get_short_date(long_date):
            return str(long_date).split(" ")[0]
            
        
        ############################################################
        ######################## Patches ###########################
        ############################################################
        last_date = "2000-01-01"
        old_date = "0"

        patch_response = []
        while True:
            new_response = site.api('cargoquery',
                limit = 'max',
                tables = "Tournaments=T, ScoreboardTeams=ST, ScoreboardGames=SG",
                join_on = "ST.OverviewPage = T.OverviewPage, ST.GameId = SG.GameId",
                fields = "ST.GameId, SG.Patch, SG.DateTime_UTC, SG.VOD, SG.RiotPlatformGameId",
                where = "SG.DateTime_UTC > '" + str(last_date) + "' AND T.Name = '" + league + "'",
                order_by = "SG.DateTime_UTC"
            )
            if old_date == last_date:
                break
            old_date = last_date
            patch_response = (patch_response + list(new_response['cargoquery']))
            last_date = str(new_response['cargoquery'][-1]['title']['DateTime UTC']).split(" ")[0]


        patches = {}
        dates = {}
        vods = {}
        game_ids = {}
        for match in patch_response:
            new_patch = str(match['title']['Patch']).replace(",",".")
            new_date = str(match['title']['DateTime UTC']).split(" ")[0]
            patches[match['title']['GameId']] = new_patch
            dates[match['title']['GameId']] = new_date
            # vods
            new_vod = str(match['title']['VOD'])
            vods[match['title']['GameId']] = new_vod
            # game ids
            new_game_id = match['title']['RiotPlatformGameId']
            game_ids[match['title']['GameId']] = new_game_id


        def get_patch(row):
            return patches[row['GameId']]
        def get_date(row):
            return dates[row['GameId']]
        def get_vod(row):
            return vods[row['GameId']]
        def get_game_id(row):
            return game_ids[row['GameId']]



        ############################################################
        ################ Team's data extraction ####################
        ############################################################
        last_index = "0"
        old_index = ""

        # team's query
        teams_response = []
        try:
            for index in range(10):
                new_response = site.api('cargoquery',
                    limit = 'max',
                    tables = "Tournaments=T, ScoreboardTeams=ST, PicksAndBansS7=PB",
                    join_on = "ST.OverviewPage = T.OverviewPage, ST.GameId = PB.GameId",
                    fields = "T.OverviewPage, ST.GameTeamId, PB.N_Page, PB.Team1, PB.Team2 ,PB.Team1Ban1,PB.Team1Ban2,PB.Team1Ban3,PB.Team1Ban4,PB.Team1Ban5, PB.Team2Ban1,PB.Team2Ban2,PB.Team2Ban3,PB.Team2Ban4,PB.Team2Ban5, PB.Team1Pick1, PB.Team1Role1, PB.Team1Pick2, , PB.Team1Role2,PB.Team1Pick3, PB.Team1Role3,PB.Team1Pick4, PB.Team1Role4,PB.Team1Pick5, PB.Team1Role5, PB.Team2Pick1, PB.Team2Role1, PB.Team2Pick2, PB.Team2Role2, PB.Team2Pick3, PB.Team2Role3, PB.Team2Pick4, PB.Team2Role4,PB.Team2Pick5, PB.Team2Role5, ST.GameId, ST.Team , ST.Side , ST.IsWinner , ST.Bans , ST.Picks , ST.Roster , ST.Dragons , ST.Barons , ST.Towers , ST.Gold , ST.Kills , ST.RiftHeralds",
                    where = "T.Name = '" + str(league) + "' AND PB.N_Page > '" + str(last_index) + "'",
                    order_by = "PB.N_Page"
                )
                if old_index == last_index or len(new_response['cargoquery']) == 0:
                    break
                old_index = last_index
                teams_response = (teams_response + list(new_response['cargoquery']))
                last_index = str(new_response['cargoquery'][-1]['title']['N Page'])
        except:
            pass
        
        if last_index == "0" and len(new_response['cargoquery']) == 0:
            try:
                new_response = site.api('cargoquery',
                    limit = 'max',
                    tables = "Tournaments=T, ScoreboardTeams=ST, PicksAndBansS7=PB",
                    join_on = "ST.OverviewPage = T.OverviewPage, ST.GameId = PB.GameId",
                    fields = "T.OverviewPage, ST.GameTeamId, PB.N_Page, PB.Team1, PB.Team2 ,PB.Team1Ban1,PB.Team1Ban2,PB.Team1Ban3,PB.Team1Ban4,PB.Team1Ban5, PB.Team2Ban1,PB.Team2Ban2,PB.Team2Ban3,PB.Team2Ban4,PB.Team2Ban5, PB.Team1Pick1, PB.Team1Role1, PB.Team1Pick2, , PB.Team1Role2,PB.Team1Pick3, PB.Team1Role3,PB.Team1Pick4, PB.Team1Role4,PB.Team1Pick5, PB.Team1Role5, PB.Team2Pick1, PB.Team2Role1, PB.Team2Pick2, PB.Team2Role2, PB.Team2Pick3, PB.Team2Role3, PB.Team2Pick4, PB.Team2Role4,PB.Team2Pick5, PB.Team2Role5, ST.GameId, ST.Team , ST.Side , ST.IsWinner , ST.Bans , ST.Picks , ST.Roster , ST.Dragons , ST.Barons , ST.Towers , ST.Gold , ST.Kills , ST.RiftHeralds",
                    where = "T.Name = '" + str(league) + "'",
                    order_by = "PB.N_Page"
                )
                teams_response = (teams_response + list(new_response['cargoquery']))
            except:
                pass

        teams_games = []
        for match in teams_response:

            team_data = {
                "OverviewPage": match['title']['OverviewPage'],
                "query": "team",
                "N_Page": match['title']['N Page'],
                "GameTeamId": match['title']['GameTeamId'],
                "Team": match['title']['Team'],
                "Team1": match['title']['Team1'],
                "Team2": match['title']['Team2'],
                "Side": match['title']['Side'],
                "GameId": match['title']['GameId'],
                "IsWinner": match['title']['IsWinner'],
                "Team1Ban1": match['title']['Team1Ban1'],
                "Team1Ban2": match['title']['Team1Ban2'],
                "Team1Ban3": match['title']['Team1Ban3'],
                "Team1Ban4": match['title']['Team1Ban4'],
                "Team1Ban5": match['title']['Team1Ban5'],
                "Team1Pick1": match['title']['Team1Pick1'],
                "Team1Role1": match['title']['Team1Role1'],
                "Team1Pick2": match['title']['Team1Pick2'],
                "Team1Role2": match['title']['Team1Role2'],
                "Team1Pick3": match['title']['Team1Pick3'],
                "Team1Role3": match['title']['Team1Role3'],
                "Team1Pick4": match['title']['Team1Pick4'],
                "Team1Role4": match['title']['Team1Role4'],
                "Team1Pick5": match['title']['Team1Pick5'],
                "Team1Role5": match['title']['Team1Role5'],
                "Team2Ban1": match['title']['Team2Ban1'],
                "Team2Ban2": match['title']['Team2Ban2'],
                "Team2Ban3": match['title']['Team2Ban3'],
                "Team2Ban4": match['title']['Team2Ban4'],
                "Team2Ban5": match['title']['Team2Ban5'],
                "Team2Pick1": match['title']['Team2Pick1'],
                "Team2Role1": match['title']['Team2Role1'],
                "Team2Pick2": match['title']['Team2Pick2'],
                "Team2Role2": match['title']['Team2Role2'],
                "Team2Pick3": match['title']['Team2Pick3'],
                "Team2Role3": match['title']['Team2Role3'],
                "Team2Pick4": match['title']['Team2Pick4'],
                "Team2Role4": match['title']['Team2Role4'],
                "Team2Pick5": match['title']['Team2Pick5'],
                "Team2Role5": match['title']['Team2Role5'],
                # "Bans": match['title']['Bans'],
                # "Picks": match['title']['Picks'],
                # "Dragons": match['title']['Dragons'],
                # "Barons": match['title']['Barons'],
                # "Towers": match['title']['Towers'],
                # "Gold": match['title']['Gold'],
                # "Kills": match['title']['Kills'],
                # "RiftHeralds": match['title']['RiftHeralds'],
            }
            teams_games.append(team_data)



        team_df = pd.DataFrame(teams_games)
        team_df = team_df.drop_duplicates(["GameTeamId"])
        
        team_df['patch'] = team_df.apply(lambda row: (get_patch(row)), axis=1)
        team_df['date'] = team_df.apply(lambda row: (get_date(row)), axis=1)
        team_df['vod'] = team_df.apply(lambda row: (get_vod(row)), axis=1)

        team_df['Team1Ban1Id'] = team_df.apply(lambda row: get_champ_id(row,'Team1Ban1') if row['Team1Ban1'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team1Ban2Id'] = team_df.apply(lambda row: get_champ_id(row,'Team1Ban2') if row['Team1Ban2'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team1Ban3Id'] = team_df.apply(lambda row: get_champ_id(row,'Team1Ban3') if row['Team1Ban3'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team1Ban4Id'] = team_df.apply(lambda row: get_champ_id(row,'Team1Ban4') if row['Team1Ban4'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team1Ban5Id'] = team_df.apply(lambda row: get_champ_id(row,'Team1Ban5') if row['Team1Ban5'] != (0 or 'None' or 'NoneType') else "", axis=1)

        team_df['Team1Pick1Id'] = team_df.apply(lambda row: get_champ_id(row,'Team1Pick1') if row['Team1Pick1'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team1Pick2Id'] = team_df.apply(lambda row: get_champ_id(row,'Team1Pick2') if row['Team1Pick2'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team1Pick3Id'] = team_df.apply(lambda row: get_champ_id(row,'Team1Pick3') if row['Team1Pick3'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team1Pick4Id'] = team_df.apply(lambda row: get_champ_id(row,'Team1Pick4') if row['Team1Pick4'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team1Pick5Id'] = team_df.apply(lambda row: get_champ_id(row,'Team1Pick5') if row['Team1Pick5'] != (0 or 'None' or 'NoneType') else "", axis=1)


        team_df['Team2Ban1Id'] = team_df.apply(lambda row: get_champ_id(row,'Team2Ban1') if row['Team2Ban1'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team2Ban2Id'] = team_df.apply(lambda row: get_champ_id(row,'Team2Ban2') if row['Team2Ban2'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team2Ban3Id'] = team_df.apply(lambda row: get_champ_id(row,'Team2Ban3') if row['Team2Ban3'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team2Ban4Id'] = team_df.apply(lambda row: get_champ_id(row,'Team2Ban4') if row['Team2Ban4'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team2Ban5Id'] = team_df.apply(lambda row: get_champ_id(row,'Team2Ban5') if row['Team2Ban5'] != (0 or 'None' or 'NoneType') else "", axis=1)

        team_df['Team2Pick1Id'] = team_df.apply(lambda row: get_champ_id(row,'Team2Pick1') if row['Team2Pick1'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team2Pick2Id'] = team_df.apply(lambda row: get_champ_id(row,'Team2Pick2') if row['Team2Pick2'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team2Pick3Id'] = team_df.apply(lambda row: get_champ_id(row,'Team2Pick3') if row['Team2Pick3'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team2Pick4Id'] = team_df.apply(lambda row: get_champ_id(row,'Team2Pick4') if row['Team2Pick4'] != (0 or 'None' or 'NoneType') else "", axis=1)
        team_df['Team2Pick5Id'] = team_df.apply(lambda row: get_champ_id(row,'Team2Pick5') if row['Team2Pick5'] != (0 or 'None' or 'NoneType') else "", axis=1)



        team_df = team_df.drop(["query", "N_Page", "GameTeamId"], axis=1)
        # team_df = team_df.drop_duplicates(['GameId'])
        ############################################################
        ############################################################


        team_df['timestamp'] = pd.to_datetime(team_df['date'])
        team_df['riotPlatformGameId'] = team_df.apply(lambda row: (get_game_id(row)), axis=1)
        team_df = team_df.sort_values(by="timestamp", ascending=False)


        # tournament_parsed = team_df.to_json(orient="records")
        # tournament = json.loads(tournament_parsed)
        team_df = team_df.fillna('')
        
        return team_df

    tournaments_list = []
    tournament = get_tournament_drafts(site, patch, summoners, runes, league)
    tournaments_list.append(tournament)
        
    total_tournaments = pd.concat(tournaments_list, ignore_index=True)
    
    tournament_parsed = total_tournaments.to_json(orient="records")
    tournament = json.loads(tournament_parsed)
    
    return tournament

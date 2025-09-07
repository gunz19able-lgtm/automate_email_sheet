from tools import make_requests, random_useragent, random_interval
from logger import setup_logger
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict
import pandas as pd
import asyncio


logger = asyncio.run(setup_logger('scraper'))


async def load_league_pool_combinations_from_excel(filename: str = 'team_pool_ids.xlsx'):
    """Load league-pool combinations from Excel file"""
    try:
        df = pd.read_excel(filename)
        league_pool_combinations = [(int(row.iloc[0]), int(row.iloc[1])) for _, row in df.iterrows()]
        print(f"Loaded {len(league_pool_combinations)} league-pool combinations from {filename}")
        return league_pool_combinations
    except Exception as e:
        print(f"Error loading league-pool combinations from Excel: {str(e)}")
        return []


async def division_name_call(league_id):
    headers = {
    'User-Agent': await random_useragent(),
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://google.com',
    'Origin': 'https://rankedin.com',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Connection': 'keep-alive',
    'Cookie': '__stripe_mid=6d617d98-f9ca-4fb0-b4f7-dfc799d68f5a760a87; ARRAffinity=bc076499a11c91231753e64e9765ff1ed1ccf1250ac8779f29466c4ddab3cf22; ARRAffinitySameSite=bc076499a11c91231753e64e9765ff1ed1ccf1250ac8779f29466c4ddab3cf22; __stripe_sid=82403c06-b751-47d8-8033-b26d3ee054a914a0bf',
    }
    api_url = f"https://api.rankedin.com/v1/metadata/GetFeatureMetadataAsync?feature=Teamleague&id={league_id}&rankedinId={league_id}&language=en"
    response = await make_requests(api_url, headers = headers)

    division_name = response.json()['featureTitle']
    return division_name


async def get_ranking_position_of_players(player_id):
    headers = {
        'User-Agent': await random_useragent(),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://rankedin.com/',
        'Origin': 'https://rankedin.com',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'Connection': 'keep-alive',
        'Cookie': '__stripe_mid=16e922e5-5bf8-4bc4-bfdc-b3ed123707485e1815; ARRAffinity=bc076499a11c91231753e64e9765ff1ed1ccf1250ac8779f29466c4ddab3cf22; ARRAffinitySameSite=bc076499a11c91231753e64e9765ff1ed1ccf1250ac8779f29466c4ddab3cf22; __stripe_sid=16ce339f-872c-4086-9faa-30e6ed00222635a810',
    }

    api_url = f"https://api.rankedin.com/v1/player/GetHistoricDataAsync?id={player_id}"

    try:
        response = await make_requests(api_url, headers = headers)
        ranking_position = response.json()[0][-1]['Standing']

    except Exception as e:
        ranking_position = ""
    return ranking_position


async def get_players(season_id):
    headers = {
        f'User-Agent': await random_useragent(),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://rankedin.com/en/team/homepage/1764246',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Connection': 'keep-alive',
        'Cookie': 'ai_user=DFzEjMtcUnEUDoZhTMKlbQ^|2025-08-11T18:49:59.686Z; __stripe_mid=6d617d98-f9ca-4fb0-b4f7-dfc799d68f5a760a87; modal-ads={%22_playerId%22:null%2C%22_ads%22:[{%22_id%22:9%2C%22_lastAdDate%22:%220001-01-01%22}%2C{%22_id%22:10%2C%22_lastAdDate%22:%220001-01-01%22}%2C{%22_id%22:4%2C%22_lastAdDate%22:%222025-08-11%22}]}; ARRAffinity=bc076499a11c91231753e64e9765ff1ed1ccf1250ac8779f29466c4ddab3cf22; ARRAffinitySameSite=bc076499a11c91231753e64e9765ff1ed1ccf1250ac8779f29466c4ddab3cf22; language=en',
    }
    url = f"https://rankedin.com/team/tlhomepage/{season_id}"

    try:
        response = await make_requests(url, headers=headers)
        raw_datas = response.json()
        team_club_id = raw_datas['Team']['HomeClub']['Id']
        players_lists = raw_datas['Team']['Players']
        players_listings_dicts = []

        # Extract player IDs for ranking position lookup
        player_ids = [player_data['Id'] for player_data in players_lists]

        # Get ranking positions concurrently
        ranking_map = {}
        if player_ids:
            logger.info(f"Collecting ranking positions for {len(player_ids)} players in season {season_id}...")
            ranking_tasks = [get_ranking_position_of_players(player_id) for player_id in player_ids]
            ranking_results = await asyncio.gather(*ranking_tasks, return_exceptions=True)

            # Create a mapping of player_id to ranking position
            for i, result in enumerate(ranking_results):
                if isinstance(result, Exception):
                    logger.error(f"Error getting ranking for player {player_ids[i]}: {result}")
                    ranking_map[player_ids[i]] = ""
                else:
                    ranking_map[player_ids[i]] = result

        # Build player data with ranking positions
        for idx in range(len(players_lists)):
            player_datas = players_lists[idx]
            player_id = player_datas['Id']

            datas = {
                'Team_ID_Players': season_id,
                'Pool ID': raw_datas['PoolId'],
                'Team League ID': raw_datas['TeamLeagueId'],
                'Team League Name': raw_datas['TeamLeagueName'],
                'State Message': raw_datas['StateMessage'],
                'Player ID': player_id,
                'Ranking Position': ranking_map.get(player_id, ""),  # Use actual ranking position
                'RankedInId': player_datas['RankedinId'],
                'Name': player_datas['FirstName'],
                'Player Order': player_datas['PlayerOrder'],
                'Player Rating': player_datas['RatingBegin'],
                'Team Participant Type': player_datas['TeamParticipantType'],
                'Has License': player_datas['HasLicense'],
                'Player URL': f"https://rankedin.com{player_datas['PlayerUrl']}",
                'Team Club Id': team_club_id,
                'Players Home Club Id': player_datas['HomeClub']['Id'],
                'Home Club Name': player_datas['HomeClub']['Name'],
                'Home Club Country': player_datas['HomeClub']['CountryShort'],
                'Home Club City': player_datas['HomeClub']['City'],
                'Home Club URL': f"https://rankedin.com{player_datas['HomeClub']['Url']}"
            }

            players_listings_dicts.append(datas)

        return players_listings_dicts
    except Exception as e:
        logger.error(f"Error getting players for team {season_id}: {str(e)}")
        return []


async def get_matches(team_home_id, team_away_id, match_id):
    headers = {
    'User-Agent': await random_useragent(),
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en',
    'Referer': 'https://rankedin.com/',
    'Origin': 'https://rankedin.com',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Connection': 'keep-alive',
    'Cookie': '__stripe_mid=6d617d98-f9ca-4fb0-b4f7-dfc799d68f5a760a87; ARRAffinity=bc076499a11c91231753e64e9765ff1ed1ccf1250ac8779f29466c4ddab3cf22; ARRAffinitySameSite=bc076499a11c91231753e64e9765ff1ed1ccf1250ac8779f29466c4ddab3cf22; __stripe_sid=aa3ba6e2-a1c9-455b-a683-db4f8de43628cd5ad6',
    }
    api_url = f"https://api.rankedin.com/v1/teamleague/GetTeamLeagueTeamsMatchesAsync?teamMatchId={str(match_id)}&language=en"

    try:
        response = await make_requests(api_url, headers=headers)
        raw_datas = response.json()[0]['Matches']['Matches']
        matches_listings_dicts = []

        for idx in range(len(raw_datas)):
            matches = raw_datas[idx]

            try:
                first_participant_score = matches['MatchResult']['Score']['FirstParticipantScore']
            except (TypeError, KeyError):
                first_participant_score = ''

            try:
                second_participant_score = matches['MatchResult']['Score']['SecondParticipantScore']
            except (TypeError, KeyError):
                second_participant_score = ''

            try:
                home_player_1_url = matches['Challenger']['Player1Url']
                home_player_1_rankedin = home_player_1_url.split("/")[3]
            except (IndexError, TypeError):
                home_player_1_url = ""
                home_player_1_rankedin = ""

            try:
                home_player_2_url = matches['Challenger']['Player2Url']
                home_player_2_rankedin = matches['Challenger']['Player2Url'].split("/")[3]
            except (IndexError, TypeError):
                home_player_2_url = ""
                home_player_2_rankedin = ""

            try:
                away_player_1_url = matches['Challenged']['Player1Url']
                away_player_1_rankedin = matches['Challenged']['Player1Url'].split("/")[3]
            except (IndexError, TypeError):
                away_player_1_url = ""
                away_player_1_rankedin = ""

            try:
                away_player_2_url = matches['Challenged']['Player2Url']
                away_player_2_rankedin = matches['Challenged']['Player2Url'].split("/")[3]
            except (IndexError, TypeError):
                away_player_2_url = ""
                away_player_2_rankedin = ""

            # Base match data with properly matched team IDs and match ID
            match_data = {
                'Round_ID': match_id,  # Ensure match_id is included and matches
                'Team_Home_ID_Matches': team_home_id,  # Home team ID from parameters
                'Team_Away_ID_Matches': team_away_id,  # Away team ID from parameters
                'Date': matches['Date'],
                'home_player_1_name': matches['Challenger']['Name'],
                'home_player_1_id': matches['Challenger']['Player1Id'],
                'home_player_1_rankedin': home_player_1_rankedin,
                'home_player_1_url': f"https://rankedin.com{home_player_1_url}" if home_player_1_url else "",
                'home_player_2_name': matches['Challenger']['Player2Name'],
                'home_player_2_id': matches['Challenger']['Player2Id'],
                'home_player_2_rankedin': home_player_2_rankedin,
                'home_player_2_url': f"https://rankedin.com{home_player_2_url}" if home_player_2_url else "",
                'away_player_1_name': matches['Challenged']['Name'],
                'away_player_1_id': matches['Challenged']['Player1Id'],
                'away_player_1_rankedin': away_player_1_rankedin,
                'away_player_1_url': f"https://rankedin.com{away_player_1_url}" if away_player_1_url else "",
                'away_player_2_name': matches['Challenged']['Player2Name'],
                'away_player_2_id': matches['Challenged']['Player2Id'],  # Fixed: was using Player1Id
                'away_player_2_rankedin': away_player_2_rankedin,
                'away_player_2_url': f"https://rankedin.com{away_player_2_url}" if away_player_2_url else "",
                'First Participant Score': first_participant_score,
                'Second Participant Score': second_participant_score,
                'Loser Tie Break': ''
            }

            # Initialize set columns - assuming maximum 5 sets
            for set_num in range(1, 6):
                match_data[f'First Participant Set Score {set_num}'] = ''
                match_data[f'Second Participant Set Score {set_num}'] = ''

            # Variable to store First Participant Winner (will be added at the end)
            first_participant_winner = ''

            try:
                participant_set_scores = matches['MatchResult']['Score']['DetailedScoring']

                # Store all set scores in individual Set_1, Set_2, etc. columns
                for gdx in range(min(len(participant_set_scores), 5)):  # Limit to 5 sets max
                    set_number = gdx + 1

                    try:
                        first_participant_set_score = participant_set_scores[gdx]['FirstParticipantScore']
                    except (TypeError, KeyError):
                        first_participant_set_score = ""
                    try:
                        second_participant_set_score = participant_set_scores[gdx]['SecondParticipantScore']
                    except (TypeError, KeyError):
                        second_participant_set_score = ""

                    # Store individual set scores in separate columns
                    match_data[f'First Participant Set Score {set_number}'] = first_participant_set_score
                    match_data[f'Second Participant Set Score {set_number}'] = second_participant_set_score

                    # For the first set, also populate the original columns for backward compatibility
                    if gdx == 0:
                        try:
                            match_data['Loser Tie Break'] = participant_set_scores[gdx]['LoserTiebreak']
                        except (KeyError, TypeError):
                            match_data['Loser Tie Break'] = ''

                        try:
                            first_participant_winner = participant_set_scores[gdx]['IsFirstParticipantWinner']
                        except (KeyError, TypeError):
                            first_participant_winner = ''

            except (TypeError, KeyError):
                # No detailed scoring available - columns remain empty
                pass

            # Add First Participant Winner at the end
            match_data['First Participant Winner'] = first_participant_winner

            matches_listings_dicts.append(match_data)

        # If no matches found, return empty structure with proper team and match IDs
        if matches_listings_dicts == []:
            matches_listings_dicts.append(
                {
                "Round_ID": match_id,
                "Team_Home_ID_Matches": team_home_id,
                "Team_Away_ID_Matches": team_away_id,
                "Date": "",
                "home_player_1_name": "",
                'home_player_1_id': "",
                'home_player_1_rankedin': "",
                'home_player_1_url': "",
                "home_player_2_name": "",
                'home_player_2_id': "",
                'home_player_2_rankedin': "",
                "home_player_2_url": "",
                "away_player_1_name": "",
                'away_player_1_id': "",
                'away_player_1_rankedin': "",
                "away_player_1_url": "",
                "away_player_2_name": "",
                'away_player_2_id': "",
                'away_player_2_rankedin': "",
                "away_player_2_url": "",
                "First Participant Score": "",
                "Second Participant Score": "",
                "Loser Tie Break": "",
                "First Participant Set Score 1": "",
                "Second Participant Set Score 1": "",
                "First Participant Set Score 2": "",
                "Second Participant Set Score 2": "",
                "First Participant Set Score 3": "",
                "Second Participant Set Score 3": "",
                "First Participant Set Score 4": "",
                "Second Participant Set Score 4": "",
                "First Participant Set Score 5": "",
                "Second Participant Set Score 5": "",
                "First Participant Winner": ""
                }
            )
        return matches_listings_dicts

    except Exception as e:
        logger.error(f"Error getting matches for team {team_home_id} and {team_away_id}, match {match_id}: {str(e)}")
        # Return empty match data to maintain structure with proper IDs
        return [{
            "Round_ID": match_id,
            "Team_Home_ID_Matches": team_home_id,
            "Team_Away_ID_Matches": team_away_id,
            "Date": "",
            "home_player_1_name": "",
            'home_player_1_id': "",
            'home_player_1_rankedin': "",
            'home_player_1_url': "",
            "home_player_2_name": "",
            'home_player_2_id': "",
            'home_player_2_rankedin': "",
            "home_player_2_url": "",
            "away_player_1_name": "",
            'away_player_1_id': "",
            'away_player_1_rankedin': "",
            "away_player_1_url": "",
            "away_player_2_name": "",
            'away_player_2_id': "",
            'away_player_2_rankedin': "",
            "away_player_2_url": "",
            "First Participant Score": "",
            "Second Participant Score": "",
            "Loser Tie Break": "",
            "First Participant Set Score 1": "",
            "Second Participant Set Score 1": "",
            "First Participant Set Score 2": "",
            "Second Participant Set Score 2": "",
            "First Participant Set Score 3": "",
            "Second Participant Set Score 3": "",
            "First Participant Set Score 4": "",
            "Second Participant Set Score 4": "",
            "First Participant Set Score 5": "",
            "Second Participant Set Score 5": "",
            "First Participant Winner": ""
        }]


async def get_organisation_id(season_id, org_id, max_admins=None, max_logos=None):
    headers = {
    'User-Agent': await random_useragent(),
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://rankedin.com/',
    'Origin': 'https://rankedin.com',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Connection': 'keep-alive',
    'Cookie': '__stripe_mid=6d617d98-f9ca-4fb0-b4f7-dfc799d68f5a760a87; ARRAffinity=bc076499a11c91231753e64e9765ff1ed1ccf1250ac8779f29466c4ddab3cf22; ARRAffinitySameSite=bc076499a11c91231753e64e9765ff1ed1ccf1250ac8779f29466c4ddab3cf22; __stripe_sid=aa3ba6e2-a1c9-455b-a683-db4f8de43628cd5ad6',
    }
    api_url = f"https://api.rankedin.com/v1/organization/GetOrganizationInfoAsync/?organisationId={str(org_id)}&language=en"

    try:
        response = await make_requests(api_url, headers = headers)
        raw_datas = response.json()

        contact_infos = raw_datas['contact']
        try:
            phone = contact_infos['phone']
        except (Exception, KeyError):
            phone = ""
        try:
            email = contact_infos['email']
        except (Exception, KeyError):
            email = ""
        try:
            website = contact_infos['websiteLink']
        except (Exception, KeyError):
            website = ""

        # Determine max admins dynamically if not provided
        if max_admins is None:
            max_admins = len(contact_infos.get('admins', []))

        # Determine max logos dynamically if not provided
        if max_logos is None:
            logos_list = raw_datas.get('logos', {}).get('logos', [])
            max_logos = len(logos_list) if logos_list else 0

        # Pre-collect admin data with consistent columns
        admin_data = {}
        # Initialize all admin columns with empty values first
        for i in range(1, max_admins + 1):
            admin_data[f'admin_name_{i}'] = ''
            admin_data[f'admin_url_{i}'] = ''

        # Fill in actual admin data
        if 'admins' in contact_infos and contact_infos['admins']:
            for i, admin in enumerate(contact_infos['admins']):
                if i < max_admins:
                    admin_data[f'admin_name_{i + 1}'] = admin.get('name', '')
                    admin_data[f'admin_url_{i + 1}'] = f"https://rankedin.com{admin.get('url', '')}"

        # Pre-collect statistics data
        statistics_data = {}
        if 'statistics' in raw_datas:
            statistics_data = {
                'Members Clubs Total': raw_datas['statistics']['memberClubsTotal'],
                'Member Players Total': raw_datas['statistics']['memberPlayersTotal'],
                'Tournaments Total': raw_datas['statistics']['tournamentsTotal']
            }

        # Pre-collect federation data
        federation_data = {}
        if 'parentFederation' in contact_infos:
            federation_data = {
                'Parent Federation Logo': contact_infos['parentFederation']['parentFederationLogo'],
                'Parent Federation Name': contact_infos['parentFederation']['parentFederationName'],
                'Parent Federation URL':f"""https://rankedin.com/{ contact_infos['parentFederation']['parentFederationUrl']}""",
                'Parent Fedration Id': contact_infos['parentFederation']['parentFederationId'],
                'Has Parent Federation logo': contact_infos['parentFederation']['hasParentFederationLogo'],
                'Has Parent Federation': contact_infos['parentFederation']['hasParentFederation'],
            }

        # Pre-collect logos data with consistent columns (at the end)
        logo_data = {}
        # Initialize all logo columns with empty values first
        for i in range(1, max_logos + 1):
            logo_data[f'logos_url_{i}'] = ''

        # Fill in actual logo data
        if 'logos' in raw_datas and raw_datas['logos']['logos']:
            for i, logos in enumerate(raw_datas['logos']['logos']):
                if i < max_logos:
                    logo_data[f'logos_url_{i + 1}'] = logos.get('url', '')

        # Create the complete dictionary in the desired order
        orgi_data = {
            'team_id_organisation': season_id,
            'Organisation_Id': org_id,
            'Name': contact_infos['organisationName'],
            'Phone': phone,
            'Email': email,
            'Address': contact_infos['address'],
            'City': contact_infos['city'],
            'Country Abbreviation': contact_infos['countryShort'],
            'Country': contact_infos['country'],
            'Website': website,
            'Poster URL': contact_infos['posterUrl'],
            'URl': f"https://rankedin.com/en/organisation/{org_id}/{raw_datas['nameForUrl']}",
            **admin_data,      # Admin columns in order (consistent count)
            **statistics_data, # Statistics columns
            **federation_data, # Federation columns
            **logo_data,       # Logo columns (now at the end, consistent count)
        }

        return [orgi_data]

    except Exception as e:
        logger.error(f"Error getting organization for team {season_id}, org {org_id}: {str(e)}")
        return []


async def get_standings_rounds_data(league_id, pool_id):
    """
    Get only standings and rounds data - no players/matches/organizations
    """
    headers = {
    'User-Agent': await random_useragent(),
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://google.com',
    'Origin': 'https://rankedin.com',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Connection': 'keep-alive',
    'Cookie': '__stripe_mid=6d617d98-f9ca-4fb0-b4f7-dfc799d68f5a760a87; ARRAffinity=bc076499a11c91231753e64e9765ff1ed1ccf1250ac8779f29466c4ddab3cf22; ARRAffinitySameSite=bc076499a11c91231753e64e9765ff1ed1ccf1250ac8779f29466c4ddab3cf22; __stripe_sid=82403c06-b751-47d8-8033-b26d3ee054a914a0bf',
    }

    division_name = await division_name_call(league_id)
    logger.info(f"Collecting division name {division_name} | Pool id {pool_id}. Please wait")

    # Separate lists for home and away team IDs
    home_team_ids = []
    away_team_ids = []
    match_ids = []
    round_listing_dicts = []

    api_url = f'https://api.rankedin.com/v1/teamleague/GetStandingsSectionAsync?teamleagueId={league_id}&poolid={pool_id}&language=en'

    try:
        response = await make_requests(api_url, headers=headers)

        logger.info(f"Collecting Standings data.")
        standings_listing_dicts = []
        standing_tables = response.json()['Standings']['ScoresViewModels']

        for jdx in range(len(standing_tables)):
            standing_results = standing_tables[jdx]
            standing_datas = {
                'Season League ID': league_id,
                'Pool ID': pool_id,
                'Participant ID': standing_results['ParticipantId'],
                'Season_ID_Standings': standing_results['ParticipantUrl'].split('/')[-1],
                'Standing': standing_results['Standing'],
                'Name': standing_results['ParticipantName'],
                'Match Points': standing_results['MatchPoints'],
                'Played': standing_results['Played'],
                'Win': standing_results['Wins'],
                'Loss': standing_results['Losses'],
                'Games Won': standing_results['GamesWon'],
                'Games Loss': standing_results['GamesLost'],
                'Games Difference': standing_results['GamesDifference'],
                'Team Games Won': standing_results['TeamGamesWon'],
                'Team Games Lost': standing_results['TeamGamesLost'],
                'Team Games Difference': standing_results['TeamGamesDifference'],
                'Scored Points': standing_results['ScoredPoints'],
                'Conceded Points': standing_results['ConcededPoints'],
                'Points Difference': standing_results['PointsDifference'],
            }
            standings_listing_dicts.append(standing_datas)

        logger.info(f"Collecting Round matches data")
        round_matches_tables = response.json()['MatchesSectionModel']['Rounds']

        for idx in range(len(round_matches_tables)):
            target_datas = round_matches_tables[idx]
            matches = target_datas['Matches']

            for gdx in range(len(matches)):
                # Separate home and away team IDs
                home_team_ids.append(matches[gdx]['Team1']['Id'])
                away_team_ids.append(matches[gdx]['Team2']['Id'])
                match_ids.append(matches[gdx]['MatchId'])

                round_matches_datas = {
                    'Season League ID': league_id,
                    'Pool ID': pool_id,
                    'Match ID': matches[gdx]['MatchId'],
                    'Date': target_datas['RoundDate'],
                    'Round': target_datas['RoundNumber'],
                    'Time': matches[gdx]['Details']['Time'],
                    'Home': matches[gdx]['Team1']['Name'],
                    'Team_ID_Home': matches[gdx]['Team1']['Id'],
                    'Home Score': matches[gdx]['Team1']['Result'],
                    'Home Winner': matches[gdx]['Team1']['IsWinner'],
                    'Away': matches[gdx]['Team2']['Name'],
                    'Team_ID_Away': matches[gdx]['Team2']['Id'],
                    'Away Score': matches[gdx]['Team2']['Result'],
                    'Away Winner': matches[gdx]['Team2']['IsWinner'],
                    'Allow Teams To Change Match Date and Location': matches[gdx]['AllowHomeTeamToChangeMatchDateAndLocation'],
                    'Location': matches[gdx]['Location'],
                    'Team URL': f"https://rankedin.com/en/team/matchresults/{matches[gdx]['MatchId']}"
                }

                round_listing_dicts.append(round_matches_datas)

        return {
            'standings': standings_listing_dicts,
            'rounds': round_listing_dicts,
            'Season_ID_Home': home_team_ids,  # Now properly separated
            'Season_ID_Away': away_team_ids,  # Now properly separated
            'round_ids': match_ids,
            'division_name': division_name
        }

    except Exception as e:
        logger.error(f"Error occurred. Are you sure it's the right division and pool ID?\nError: {str(e)}")
        return None


async def collect_data_concurrently(Season_ID_Home, Season_ID_Away, match_ids):
    """
    Collect players, matches, and organizations data concurrently
    """
    logger.info("Starting concurrent data collection...")

    # Combine home and away team IDs for unique team collection
    all_season_ids = Season_ID_Home + Season_ID_Away
    unique_season_ids = list(set(all_season_ids))

    # Create team-match tuples with home/away distinction for matches
    team_match_tuples = list(zip(Season_ID_Home, Season_ID_Away, match_ids))

    # Collect players data concurrently (now includes ranking positions)
    logger.info(f"Collecting players data for {len(unique_season_ids)} teams concurrently...")
    players_tasks = [get_players(str(season_id)) for season_id in unique_season_ids]
    players_results = await asyncio.gather(*players_tasks, return_exceptions=True)

    # Flatten players results and collect organization IDs
    all_players = []
    org_season_pairs = []

    for i, result in enumerate(players_results):
        if isinstance(result, Exception):
            logger.error(f"Error in players task {i}: {result}")
            continue

        if result:
            all_players.extend(result)
            # Collect unique organization IDs from players
            season_id = unique_season_ids[i]
            for player in result:
                org_id = player.get('Team Club Id')
                if org_id:
                    org_season_pairs.append((season_id, org_id))

    # Remove duplicate organization requests
    unique_org_pairs = list(set(org_season_pairs))

    # Collect matches data concurrently with proper home/away team IDs
    logger.info(f"Collecting matches data for {len(team_match_tuples)} matches concurrently...")
    matches_tasks = [get_matches(str(home_id), str(away_id), str(match_id))
                     for home_id, away_id, match_id in team_match_tuples]
    matches_results = await asyncio.gather(*matches_tasks, return_exceptions=True)

    # Flatten matches results
    all_matches = []
    for i, result in enumerate(matches_results):
        if isinstance(result, Exception):
            logger.error(f"Error in matches task {i}: {result}")
            continue

        if result:
            all_matches.extend(result)

    # Remove duplicate organization IDs (keep only unique org_ids)
    unique_org_ids = list(set([org_id for _, org_id in unique_org_pairs]))

    # Collect organizations data concurrently (using first season_id for each unique org_id)
    logger.info(f"Collecting organizations data for {len(unique_org_ids)} unique organizations concurrently...")

    # Create mapping of org_id to season_id for API calls
    org_to_season = {}
    for season_id, org_id in unique_org_pairs:
        if org_id not in org_to_season:
            org_to_season[org_id] = season_id

    org_tasks = [get_organisation_id(str(org_to_season[org_id]), str(org_id)) for org_id in unique_org_ids]
    org_results = await asyncio.gather(*org_tasks, return_exceptions=True)

    # Flatten organization results
    all_organizations = []
    for i, result in enumerate(org_results):
        if isinstance(result, Exception):
            logger.error(f"Error in organization task {i}: {result}")
            continue

        if result:
            all_organizations.extend(result)

    logger.info("Concurrent data collection completed!")

    return {
        'players': all_players,
        'matches': all_matches,
        'organizations': all_organizations
    }


async def collect_all_league_data(league_id, pool_id):
    """
    Master function to collect all data for a league/pool combination with concurrent execution
    """
    # First get the standings and rounds data
    main_data = await get_standings_rounds_data(league_id, pool_id)

    if main_data is None:
        return None

    # Now collect players, matches, and organizations concurrently using separate home/away team IDs
    concurrent_data = await collect_data_concurrently(
        main_data['Season_ID_Home'],
        main_data['Season_ID_Away'],
        main_data['round_ids']
    )

    # Combine all data
    complete_data = {
        'standings': main_data['standings'],
        'rounds': main_data['rounds'],
        'players': concurrent_data['players'],
        'matches': concurrent_data['matches'],
        'organizations': concurrent_data['organizations'],
        'division_name': main_data['division_name']
    }

    logger.info("All data collection completed successfully!")

    return complete_data


async def collect_multiple_league_data(team_pool_combinations, delay, batches):
    """
    Replace your existing collect_multiple_league_data function with this one
    """
    # Split combinations into batches of 5
    batches = [team_pool_combinations[i:i + batches] for i in range(0, len(team_pool_combinations), batches)]

    all_standings = []
    all_rounds = []
    all_players = []
    all_matches = []
    all_organizations = []
    successful_combinations = []
    failed_combinations = []

    logger.info(f"Processing {len(team_pool_combinations)} combinations")

    for batch_idx, batch_combinations in enumerate(batches, 1):
        logger.info(f"Starting batch {batch_idx}/{len(batches)} with {len(batch_combinations)} combinations")

        # Create tasks for all combinations in this batch
        tasks = []
        for season_id, pool_id in batch_combinations:
            task = asyncio.create_task(collect_all_league_data(season_id, pool_id))
            tasks.append((task, season_id, pool_id))

        # Wait for all tasks in the batch to complete
        for task, season_id, pool_id in tasks:
            try:
                scraped_data = await task
                if scraped_data is None:
                    logger.error(f"Failed to collect data for League {season_id}, Pool {pool_id}")
                    failed_combinations.append((season_id, pool_id))
                    continue

                # Add data to combined results
                for record in scraped_data.get('standings', []):
                    record['season_id'] = season_id
                    record['pool_id'] = pool_id
                    all_standings.append(record)

                for record in scraped_data.get('rounds', []):
                    record['season_id'] = season_id
                    record['pool_id'] = pool_id
                    all_rounds.append(record)

                for record in scraped_data.get('players', []):
                    # record['season_id'] = season_id
                    record['pool_id'] = pool_id
                    all_players.append(record)

                for record in scraped_data.get('matches', []):
                    record['season_id'] = season_id
                    record['pool_id'] = pool_id
                    all_matches.append(record)

                for record in scraped_data.get('organizations', []):
                    record['season_id'] = season_id
                    record['pool_id'] = pool_id
                    all_organizations.append(record)

                successful_combinations.append((season_id, pool_id))
                logger.info(f"Successfully processed League {season_id}, Pool {pool_id}")

            except Exception as e:
                logger.error(f"Error processing League {season_id}, Pool {pool_id}: {str(e)}")
                failed_combinations.append((season_id, pool_id))

        # Random delay between batches (except for the last batch)
        if batch_idx < len(batches):
            random_delay = await random_interval(delay)
            logger.info(f"Batch {batch_idx} complete. Waiting {int(random_delay)} seconds before next batch...")
            await asyncio.sleep(random_delay)

    logger.info(f"All batches completed. Success: {len(successful_combinations)}, Failed: {len(failed_combinations)}")

    return {
        'standings': all_standings,
        'rounds': all_rounds,
        'players': all_players,
        'matches': all_matches,
        'organizations': all_organizations,
        'successful_combinations': successful_combinations,
        'failed_combinations': failed_combinations,
        'total_processed': len(team_pool_combinations)
    }


async def collect_multiple_league_data_archive(team_pool_combinations):
    all_standings = []
    all_rounds = []
    all_players = []
    all_matches = []
    all_organizations = []

    successful_combinations = []
    failed_combinations = []

    logger.info(f"Starting batch collection for {len(team_pool_combinations)} league/pool combinations")

    for season_id, pool_id in team_pool_combinations:
        try:
            scraped_data = await collect_all_league_data(season_id, pool_id)
            if scraped_data is None:
                logger.error(f"Failed to collect data for League {season_id}, Pool {pool_id}")
                failed_combinations.append(season_id, pool_id)
                continue

            # Add league_id and pool_id to each record
            # Standings
            for record in scraped_data.get('standings', []):
                record['season_id'] = season_id
                record['pool_id'] = pool_id
                all_standings.append(record)

            # Rounds:
            for record in scraped_data.get('rounds', []):
                record['season_id'] = season_id
                record['pool_id'] = pool_id
                all_rounds.append(record)

            # Players:
            for record in scraped_data.get('players', []):
                record['season_id'] = season_id
                record['pool_id'] = pool_id
                all_players.append(record)

            # Matches:
            for record in scraped_data.get('matches', []):
                record['season_id'] = season_id
                record['pool_id'] = pool_id
                all_matches.append(record)

            # Organizations:
            for record in scraped_data.get('organizations', []):
                record['season_id'] = season_id
                record['pool_id'] = pool_id
                all_organizations.append(record)

            successful_combinations.append((season_id, pool_id))
            logger.info(f"Successfully collected data for league {season_id}, Pool {pool_id}")

            random_delay = await random_interval(5)
            await asyncio.sleep(random_delay)
        except Exception as e:
            logger.error(f"Error processing League {season_id}, Pool {pool_id}: {str(e)}")
            failed_combinations.append(season_id, pool_id)
            continue

    logger.info(f"Batch collection completed. Success: {len(successful_combinations)}, Failed: {len(failed_combinations)}")

    if failed_combinations:
        logger.warning(f"Failed combinations: {failed_combinations}")

    return {
        'standings': all_standings,
        'rounds': all_rounds,
        'players': all_players,
        'matches': all_matches,
        'organizations': all_organizations,
        'successful_combinations': successful_combinations,
        'total_processed': len(team_pool_combinations)
    }


async def save_batch_to_excel(batch_data: Dict):
    season_id = batch_data.get('standings')[0].get('season_id')
    timestamp = datetime.now(ZoneInfo("Europe/Copenhagen")).strftime("%Y-%m-%d_%H:%M:%S")
    division_name = await division_name_call(season_id)
    output_name = f"{division_name}_{timestamp}.xlsx"
    try:

        with pd.ExcelWriter(f"{output_name}", engine='openpyxl') as writer:

            def format_worksheet(worksheet, dataframe):
                """Helper function to freeze first row and auto-adjust row heights"""
                # Freeze the first row (header row)
                worksheet.freeze_panes = 'A2'

                # Auto-adjust row heights
                for row in worksheet.iter_rows():
                    worksheet.row_dimensions[row[0].row].height = None  # Auto height

                # Optional: Auto-adjust column widths too
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
                    worksheet.column_dimensions[column_letter].width = adjusted_width

            # Save consolidated standings data
            if batch_data.get('standings'):
                standings_df = pd.DataFrame(batch_data['standings'])

                # Reorder columns to put identifiers first
                cols = standings_df.columns.tolist()
                if 'season_id' in cols and 'pool_id' in cols:
                    cols.remove('season_id')
                    cols.remove('pool_id')
                    cols = ['season_id', 'pool_id'] + cols
                    standings_df = standings_df[cols]

                standings_df.to_excel(writer, sheet_name='Standings', index=False)
                format_worksheet(writer.sheets['Standings'], standings_df)
                logger.info(f"Saved {len(standings_df)} final standings records.")

            # Save consolidated rounds data
            if batch_data.get('rounds'):
                rounds_df = pd.DataFrame(batch_data['rounds'])
                cols = rounds_df.columns.tolist()
                if 'season_id' in cols and 'pool_id' in cols:
                    cols.remove('season_id')
                    cols.remove('pool_id')
                    cols = ['season_id', 'pool_id'] + cols
                    rounds_df = rounds_df[cols]

                rounds_df.to_excel(writer, sheet_name='Rounds', index=False)
                format_worksheet(writer.sheets['Rounds'], rounds_df)
                logger.info(f"Saved {len(rounds_df)} final rounds records.")

            # Save consolidated players data
            if batch_data.get('players'):
                players_df = pd.DataFrame(batch_data['players'])
                cols = players_df.columns.tolist()
                if 'pool_id' in cols:
                    # cols.remove('season_id')
                    cols.remove('pool_id')
                    cols = ['pool_id'] + cols
                    players_df = players_df[cols]

                players_df.to_excel(writer, sheet_name='Players', index=False)
                format_worksheet(writer.sheets['Players'], players_df)
                logger.info(f'Saved {len(players_df)} final players records')

            # Save consolidated matches data
            if batch_data.get('matches'):
                matches_df = pd.DataFrame(batch_data['matches'])
                cols = matches_df.columns.tolist()
                if 'season_id' in cols and 'pool_id' in cols:
                    cols.remove('season_id')
                    cols.remove('pool_id')
                    cols = ['season_id', 'pool_id'] + cols
                    matches_df = matches_df[cols]

                # Remove empty set score columns
                set_columns = [col for col in matches_df.columns if 'Set Score' in col]
                for col in set_columns:
                    if matches_df[col].isna().all() or (matches_df[col] == '').all():
                        matches_df = matches_df.drop(columns=[col])

                matches_df.to_excel(writer, sheet_name='Matches', index=False)
                format_worksheet(writer.sheets['Matches'], matches_df)
                logger.info(f"Saved {len(matches_df)} final matches records.")

            # Save consolidated organizations data
            if batch_data.get('organizations'):
                organizations_df = pd.DataFrame(batch_data['organizations'])
                cols = organizations_df.columns.tolist()
                if 'season_id' in cols and 'pool_id' in cols:
                    cols.remove('season_id')
                    cols.remove('pool_id')
                    cols = ['season_id', 'pool_id'] + cols
                    organizations_df = organizations_df[cols]

                organizations_df.to_excel(writer, sheet_name='Organizations', index=False)
                format_worksheet(writer.sheets['Organizations'], organizations_df)
                logger.info(f"Saved {len(organizations_df)} final organizations records.")

            # Create comprehensive summary sheet
            summary_data = {
                'Metric': [
                    'Total League-Pool Combinations Processed',
                    'Successful Combinations',
                    'Failed Combinations',
                    'Success Rate (%)',
                    'Total Final Standings Records',
                    'Total Final Rounds Records',
                    'Total Final Players Records',
                    'Total Final Matches Records',
                    'Total Final Organizations Records',
                    'Report Generated'
                ],
                'Count/Value': [
                    batch_data.get('total_processed', 0),
                    len(batch_data.get('successful_combinations', [])),
                    len(batch_data.get('failed_combinations', [])),
                    round((len(batch_data.get('successful_combinations', [])) / max(batch_data.get('total_processed', 1), 1)) * 100, 2),
                    len(batch_data.get('standings', [])),
                    len(batch_data.get('rounds', [])),
                    len(batch_data.get('players', [])),
                    len(batch_data.get('matches', [])),
                    len(batch_data.get('organizations', [])),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
            }

            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Executive_Summary', index=False)
            format_worksheet(writer.sheets['Executive_Summary'], summary_df)

            # Add processing results details
            if batch_data.get('successful_combinations') or batch_data.get('failed_combinations'):
                processing_results = []

                # Add successful combinations
                if batch_data.get('successful_combinations'):
                    for combo in batch_data['successful_combinations']:
                        processing_results.append({
                            'League_ID': combo[0] if isinstance(combo, (list, tuple)) else combo.get('league_id', combo),
                            'Pool_ID': combo[1] if isinstance(combo, (list, tuple)) else combo.get('pool_id', ''),
                            'Status': 'Success',
                            'Records_Found': 'Yes'
                        })

                # Add failed combinations
                if batch_data.get('failed_combinations'):
                    for combo in batch_data['failed_combinations']:
                        processing_results.append({
                            'League_ID': combo[0] if isinstance(combo, (list, tuple)) else combo.get('league_id', combo),
                            'Pool_ID': combo[1] if isinstance(combo, (list, tuple)) else combo.get('pool_id', ''),
                            'Status': 'Failed',
                            'Records_Found': 'No'
                        })

        logger.info(f"Final batch report saved to {output_name}")
        logger.info(f"Report contains consolidated data from all processed league-pool combinations")
        return output_name

    except Exception as e:
        logger.error(f"Error saving final batch data to Excel: {str(e)}")
        return None


async def save_all_to_excel(standings_data, rounds_data, players_data, matches_data, organizations_data, filename):
    """
    Save all collected data to separate sheets in an Excel file
    """
    try:
        excel_filename = f'{filename}_complete_data.xlsx'  # Store the filename

        with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
            # Save standings data
            if standings_data:
                pd.DataFrame(standings_data).to_excel(writer, sheet_name='Standings', index=False)
            # Save rounds data
            if rounds_data:
                pd.DataFrame(rounds_data).to_excel(writer, sheet_name='Rounds', index=False)
            # Save players data
            if players_data:
                pd.DataFrame(players_data).to_excel(writer, sheet_name='Players', index=False)
            if matches_data:
            # Save matches data
                # Convert list of dictionaries to DataFrame
                matches_df = pd.DataFrame(matches_data)
                # Remove completely empty set score columns to avoid the numpy array error
                set_columns = [col for col in matches_df.columns if 'Set Score' in col]
                for col in set_columns:
                    if matches_df[col].isna().all() or (matches_df[col] == '').all():
                        matches_df = matches_df.drop(columns=[col])
                matches_df.to_excel(writer, sheet_name='Matches', index=False)
            # Save organizations data
            if organizations_data:
                pd.DataFrame(organizations_data).to_excel(writer, sheet_name='Organizations', index=False)

        logger.info(f"All data saved to {excel_filename}")
        return excel_filename  # ← ADD THIS LINE - This was missing!

    except Exception as e:
        logger.error(f"Error saving to Excel: {str(e)}")
        return None  # ← ADD THIS LINE - Return None on failure


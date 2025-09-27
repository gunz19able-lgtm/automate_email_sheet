from tools import make_requests, random_useragent, random_interval, convert_unix_timestamp
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


"""async def get_players_url_image(rankedin_id):
    headers = {
    'User-Agent': await random_useragent(),
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    # 'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Referer': 'https://rankedin.com/',
    'Origin': 'https://rankedin.com',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Connection': 'keep-alive',
    'Cookie': '__stripe_mid=d46b5b70-2201-4083-a2b9-cab70e0b697564e55a; ARRAffinity=4334255d9508d90d91675813fd4970201a6736e602b033e6fc00b5a23410f015; ARRAffinitySameSite=4334255d9508d90d91675813fd4970201a6736e602b033e6fc00b5a23410f015; __stripe_sid=21857c77-dc76-473b-8e42-691fba8f012cac3dff',
    }

    api_url = f"https://api.rankedin.com/v1/metadata/GetFeatureMetadataAsync?feature=PlayerProfile&id=0&rankedinId={rankedin_id}&language=en"

    try:
        response = await make_requests(api_url, headers = headers)
        raw_datas = response.json()
        image_url = raw_datas['featureImage']
        return image_url
    except Exception as e:
        logger.error(f"Error getting players for team {rankedin_id}: {str(e)}")
        return []
"""


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
    target_ranking_name = "Dansk Padel Forbunds rangliste (Men-Main/MD)"

    try:
        response = await make_requests(api_url, headers=headers)

        # Add validation for the response
        if response is None:
            logger.warning(f"No response received for player {player_id}")
            return None, None, None

        try:
            ranking_position = response.json()
        except Exception as json_error:
            logger.warning(f"Failed to parse JSON for player {player_id}: {json_error}")
            return None, None, None

        # Validate the ranking_position structure
        if ranking_position is None:
            logger.warning(f"Ranking position data is None for player {player_id}")
            return None, None, None

        if not isinstance(ranking_position, (list, dict)):
            logger.warning(f"Unexpected ranking position data type for player {player_id}: {type(ranking_position)}")
            return None, None, None

        # Handle case where ranking_position is empty
        if isinstance(ranking_position, list) and len(ranking_position) == 0:
            logger.warning(f"Empty ranking position data for player {player_id}")
            return None, None, None

        latest_timestamp = 0
        latest_standing = None
        latest_ranking_name = None

        # Iterate through each ranking list
        try:
            # Handle both list and dict responses
            if isinstance(ranking_position, dict):
                # If it's a dict, convert to list format or handle appropriately
                ranking_position = [ranking_position]

            for idx in range(len(ranking_position)):
                ranking_list = ranking_position[idx]

                if isinstance(ranking_list, list):
                    # Process all entries in this ranking list
                    for entry in ranking_list:
                        if isinstance(entry, dict) and 'UnixTimestamp' in entry and 'Standing' in entry:
                            unix_timestamp = entry['UnixTimestamp']
                            standing = entry['Standing']

                            # Check for ranking name in the entry
                            entry_ranking_name = entry.get('RankingName', f"Ranking_{idx}")

                            # Only process if it's the target ranking name or contains the target name
                            if (entry_ranking_name == target_ranking_name or
                                target_ranking_name in str(entry_ranking_name)):

                                # Keep track of the highest timestamp (latest/furthest date, could be in future)
                                if unix_timestamp > latest_timestamp:
                                    latest_timestamp = unix_timestamp
                                    latest_standing = standing
                                    latest_ranking_name = entry_ranking_name
                elif isinstance(ranking_list, dict):
                    # Handle case where ranking_list is a dict instead of a list
                    if 'UnixTimestamp' in ranking_list and 'Standing' in ranking_list:
                        unix_timestamp = ranking_list['UnixTimestamp']
                        standing = ranking_list['Standing']
                        entry_ranking_name = ranking_list.get('RankingName', f"Ranking_{idx}")

                        if (entry_ranking_name == target_ranking_name or
                            target_ranking_name in str(entry_ranking_name)):

                            if unix_timestamp > latest_timestamp:
                                latest_timestamp = unix_timestamp
                                latest_standing = standing
                                latest_ranking_name = entry_ranking_name
                else:
                    logger.warning(f"Unexpected ranking list type for player {player_id} at index {idx}: {type(ranking_list)}")

        except Exception as processing_error:
            logger.error(f"Error processing ranking data for player {player_id}: {processing_error}")
            return None, None, None

        # Convert and display the latest (highest/furthest) timestamp
        if latest_timestamp > 0:
            latest_date = await convert_unix_timestamp(latest_timestamp)
            logger.info(f"Player {player_id} - Ranking Name: {latest_ranking_name}")
            logger.info(f"Player {player_id} - Latest timestamp: {latest_date}")
            logger.info(f"Player {player_id} - Standing for latest timestamp: {latest_standing}")

            # Return standing, timestamp, and ranking name as a tuple
            return latest_standing, latest_date, latest_ranking_name
        else:
            logger.info(f"Player {player_id} - No valid timestamps found for ranking: {target_ranking_name}")
            return None, None, None

    except Exception as e:
        logger.error(f"Error getting ranking for player {player_id}: {str(e)}")
        return None, None, None


async def get_players(season_id):
    """
    Enhanced get_players function that includes player image URLs
    """
    headers = {
        'User-Agent': await random_useragent(),
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

        # Extract player IDs and RankedInIds for concurrent operations
        player_ids = [player_data['Id'] for player_data in players_lists]
        rankedin_ids = [player_data['RankedinId'] for player_data in players_lists]

        # Get ranking positions, timestamps, ranking names, and image URLs concurrently
        ranking_map = {}
        timestamp_map = {}
        ranking_name_map = {}
        image_url_map = {}

        if player_ids:
            logger.info(f"Collecting data for {len(player_ids)} players in season {season_id}...")

            # Create tasks for ranking positions
            ranking_tasks = [get_ranking_position_of_players(player_id) for player_id in player_ids]

            # Run both sets of tasks concurrently
            logger.info("Fetching ranking positions and player images concurrently...")
            ranking_results = await asyncio.gather(
                asyncio.gather(*ranking_tasks, return_exceptions=True),
                return_exceptions=True
            )

            # Process ranking results
            for i, result in enumerate(ranking_results):
                if isinstance(result, Exception):
                    logger.error(f"Error getting ranking for player {player_ids[i]}: {result}")
                    ranking_map[player_ids[i]] = ""
                    timestamp_map[player_ids[i]] = ""
                    ranking_name_map[player_ids[i]] = ""
                else:
                    # Unpack the tuple returned from get_ranking_position_of_players
                    standing, timestamp, ranking_name = result
                    ranking_map[player_ids[i]] = standing if standing is not None else ""
                    timestamp_map[player_ids[i]] = timestamp if timestamp is not None else ""
                    ranking_name_map[player_ids[i]] = ranking_name if ranking_name is not None else ""

        # Build player data with ranking positions, timestamps, ranking names, and image URLs
        for idx in range(len(players_lists)):
            player_datas = players_lists[idx]
            player_id = player_datas['Id']
            rankedin_id = player_datas['RankedinId']

            datas = {
                'Team_ID_Players': season_id,
                'Pool ID': raw_datas['PoolId'],
                'Team League ID': raw_datas['TeamLeagueId'],
                'Team League Name': raw_datas['TeamLeagueName'],
                'State Message': raw_datas['StateMessage'],
                'Player ID': player_id,
                'Ranking Position': ranking_map.get(player_id, ""),
                'Ranking Timestamp': timestamp_map.get(player_id, ""),
                'Ranking Name': ranking_name_map.get(player_id, ""),
                'RankedInId': rankedin_id,
                'Name': player_datas['FirstName'],
                'Player Order': player_datas['PlayerOrder'],
                'Player Rating': player_datas['RatingBegin'],
                'Team Participant Type': player_datas['TeamParticipantType'],
                'Has License': player_datas['HasLicense'],
                'Player URL': f"https://rankedin.com{player_datas['PlayerUrl']}",
                'Team Organisation Id': team_club_id,
                'Players Home Club Id': player_datas['HomeClub']['Id'],
                'Home Club Name': player_datas['HomeClub']['Name'],
                'Home Club Country': player_datas['HomeClub']['CountryShort'],
                'Home Club City': player_datas['HomeClub']['City'],
                'Home Club URL': f"https://rankedin.com{player_datas['HomeClub']['Url']}",
                'Ranking API URL': f"https://api.rankedin.com/v1/player/GetHistoricDataAsync?id={player_id}",
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
                # 'Season League ID': league_id,
                # 'Pool ID': pool_id,
                'Participant ID': standing_results['ParticipantId'],
                'team_id_standing': standing_results['ParticipantUrl'].split('/')[-1],
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
                    # 'Season League ID': league_id,
                    # 'Pool ID': pool_id,
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


async def get_player_image_url(rankedin_id):
    """
    Get player image URL using RankedInId
    """
    headers = {
        'User-Agent': await random_useragent(),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://rankedin.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Connection': 'keep-alive',
    }

    # Construct player profile URL using RankedInId
    url = f"https://rankedin.com/api/player/{rankedin_id}/profile"

    try:
        response = await make_requests(url, headers=headers)
        if response.status_code == 200:
            player_data = response.json()
            # Extract image URL from player data
            image_url = player_data.get('ProfileImageUrl', '')
            if image_url and not image_url.startswith('http'):
                # Make sure URL is absolute
                image_url = f"https://rankedin.com{image_url}"
            return image_url
        else:
            logger.warning(f"Failed to get player image for RankedInId {rankedin_id}: HTTP {response.status_code}")
            return ""
    except Exception as e:
        logger.error(f"Error getting player image for RankedInId {rankedin_id}: {str(e)}")
        return ""


async def get_players(team_id, max_concurrent_rankings=10, delay_between_batches=5, max_retries=15):
    random_delay = await random_interval(20)
    """
    Get players data with limited concurrency for ranking position requests

    Args:
        team_id: The team ID to get players for
        max_concurrent_rankings: Max concurrent ranking requests (default: 10)
        delay_between_batches: Delay between ranking batches in seconds (default: 5)
        max_retries: Maximum number of retries for the entire function (default: 15)
    """

    for attempt in range(max_retries):
        try:
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
            url = f"https://api.rankedin.com/v1/TeamLeague/GetTeamLeagueTeamHomepageAsync?teamId={team_id}&language=en"

            response = await make_requests(url, headers=headers)

            # Check if make_requests failed completely
            if response is None:
                logger.warning(f"Attempt {attempt + 1}: Failed to get response for team {team_id}")
                raise Exception("No response received from make_requests")

            # Parse JSON with error handling
            try:
                raw_datas = response.json()
            except Exception as json_error:
                logger.warning(f"Attempt {attempt + 1}: Failed to parse JSON for team {team_id}: {json_error}")
                raise Exception(f"JSON parsing failed: {json_error}")

            # Validate response structure
            if not raw_datas:
                logger.warning(f"Attempt {attempt + 1}: Empty response for team {team_id}")
                raise Exception("Empty response data")

            if not isinstance(raw_datas, dict):
                logger.warning(f"Attempt {attempt + 1}: Response is not a dictionary for team {team_id}")
                raise Exception("Response data is not a dictionary")

            # Check if 'Team' key exists and is not None
            if 'Team' not in raw_datas:
                logger.warning(f"Attempt {attempt + 1}: Missing 'Team' key for team {team_id}")
                raise Exception("Invalid response structure: missing 'Team' key")

            if raw_datas['Team'] is None:
                logger.warning(f"Attempt {attempt + 1}: 'Team' key is None for team {team_id}")
                raise Exception("Invalid response structure: 'Team' key is None")

            team_data = raw_datas['Team']

            # Validate Team structure
            if not isinstance(team_data, dict):
                logger.warning(f"Attempt {attempt + 1}: Team data is not a dictionary for team {team_id}")
                raise Exception("Team data is not a dictionary")

            # Check Players key
            if 'Players' not in team_data:
                logger.warning(f"Attempt {attempt + 1}: Missing 'Players' key in Team for team {team_id}")
                raise Exception("Invalid response structure: missing 'Players' key in Team")

            if team_data['Players'] is None:
                logger.warning(f"Attempt {attempt + 1}: 'Players' key is None in Team for team {team_id}")
                raise Exception("Invalid response structure: 'Players' key in Team is None")

            # Check HomeClub key
            if 'HomeClub' not in team_data:
                logger.warning(f"Attempt {attempt + 1}: Missing 'HomeClub' key in Team for team {team_id}")
                raise Exception("Invalid response structure: missing 'HomeClub' key in Team")

            if team_data['HomeClub'] is None:
                logger.warning(f"Attempt {attempt + 1}: 'HomeClub' key is None in Team for team {team_id}")
                raise Exception("Invalid response structure: 'HomeClub' key in Team is None")

            # Validate HomeClub structure
            home_club = team_data['HomeClub']
            if not isinstance(home_club, dict):
                logger.warning(f"Attempt {attempt + 1}: HomeClub data is not a dictionary for team {team_id}")
                raise Exception("HomeClub data is not a dictionary")

            if 'Id' not in home_club:
                logger.warning(f"Attempt {attempt + 1}: Missing 'Id' key in HomeClub for team {team_id}")
                raise Exception("Invalid response structure: missing 'Id' key in HomeClub")

            # If we reach here, the response structure is valid
            team_club_id = home_club['Id']
            players_lists = team_data['Players']

            # Validate players list
            if not isinstance(players_lists, list):
                logger.warning(f"Attempt {attempt + 1}: Players data is not a list for team {team_id}")
                raise Exception("Players data is not a list")

            players_listings_dicts = []
            random_delay = await random_interval(delay_between_batches)

            # Extract player IDs for ranking position lookup with validation
            player_ids = []
            for player_data in players_lists:
                if isinstance(player_data, dict) and 'Id' in player_data and player_data['Id'] is not None:
                    player_ids.append(player_data['Id'])
                else:
                    logger.warning(f"Invalid player data structure found for team {team_id}")

            # Get ranking positions with limited concurrency
            ranking_map = {}
            timestamp_map = {}
            ranking_name_map = {}

            if player_ids:
                logger.info(f"Collecting ranking positions for {len(player_ids)} players in team {team_id} with max {max_concurrent_rankings} concurrent...")

                # Process ranking requests in batches
                for i in range(0, len(player_ids), max_concurrent_rankings):
                    batch_ids = player_ids[i:i + max_concurrent_rankings]
                    logger.info(f"Processing ranking batch {i//max_concurrent_rankings + 1} with {len(batch_ids)} players...")

                    # Create tasks for this batch
                    ranking_tasks = [get_ranking_position_of_players(player_id) for player_id in batch_ids]
                    ranking_results = await asyncio.gather(*ranking_tasks, return_exceptions=True)

                    # Process results for this batch
                    for j, result in enumerate(ranking_results):
                        player_id = batch_ids[j]
                        if isinstance(result, Exception):
                            logger.error(f"Error getting ranking for player {player_id}: {result}")
                            ranking_map[player_id] = ""
                            timestamp_map[player_id] = ""
                            ranking_name_map[player_id] = ""
                        else:
                            # Unpack the tuple returned from get_ranking_position_of_players
                            standing, timestamp, ranking_name = result
                            ranking_map[player_id] = standing if standing is not None else ""
                            timestamp_map[player_id] = timestamp if timestamp is not None else ""
                            ranking_name_map[player_id] = ranking_name if ranking_name is not None else ""

                    # Add delay between batches (except for last batch)
                    if i + max_concurrent_rankings < len(player_ids):
                        await asyncio.sleep(random_delay)
                        logger.info(f"Ranking batch complete, waiting {int(random_delay)}s before next batch...")

            # Build player data with ranking positions, timestamps, and ranking names
            for idx in range(len(players_lists)):
                try:
                    player_datas = players_lists[idx]

                    # Validate player data structure
                    if not isinstance(player_datas, dict):
                        logger.warning(f"Player data at index {idx} is not a dictionary for team {team_id}")
                        continue

                    # Check required fields
                    required_fields = ['Id', 'RankedinId', 'FirstName', 'PlayerOrder', 'RatingBegin',
                                     'TeamParticipantType', 'HasLicense', 'PlayerUrl', 'HomeClub']

                    missing_fields = [field for field in required_fields if field not in player_datas]
                    if missing_fields:
                        logger.warning(f"Player data missing fields {missing_fields} for team {team_id}, player index {idx}")
                        continue

                    player_id = player_datas['Id']

                    # Validate HomeClub structure for this player
                    player_home_club = player_datas['HomeClub']
                    if not isinstance(player_home_club, dict):
                        logger.warning(f"Player HomeClub data is not a dictionary for team {team_id}, player {player_id}")
                        continue

                    # Check required HomeClub fields
                    home_club_fields = ['Id', 'Name', 'CountryShort', 'City', 'Url']
                    missing_home_club_fields = [field for field in home_club_fields if field not in player_home_club]
                    if missing_home_club_fields:
                        logger.warning(f"Player HomeClub missing fields {missing_home_club_fields} for team {team_id}, player {player_id}")
                        continue

                    datas = {
                        'Team_ID_Players': team_id,
                        'Pool ID': raw_datas.get('PoolId', ''),
                        'Team League ID': raw_datas.get('TeamLeagueId', ''),
                        'Team League Name': raw_datas.get('TeamLeagueName', ''),
                        'State Message': raw_datas.get('StateMessage', ''),
                        'Player ID': player_id,
                        'Ranking Position': ranking_map.get(player_id, ""),
                        'Ranking Timestamp': timestamp_map.get(player_id, ""),
                        'Ranking Name': ranking_name_map.get(player_id, ""),
                        'RankedInId': player_datas.get('RankedinId', ''),
                        'Name': player_datas.get('FirstName', ''),
                        'Player Order': player_datas.get('PlayerOrder', ''),
                        'Player Rating': player_datas.get('RatingBegin', ''),
                        'Team Participant Type': player_datas.get('TeamParticipantType', ''),
                        'Has License': player_datas.get('HasLicense', ''),
                        'Player URL': f"https://rankedin.com{player_datas.get('PlayerUrl', '')}",
                        'Team Organisation Id': team_club_id,
                        'Players Home Club Id': player_home_club.get('Id', ''),
                        'Home Club Name': player_home_club.get('Name', ''),
                        'Home Club Country': player_home_club.get('CountryShort', ''),
                        'Home Club City': player_home_club.get('City', ''),
                        'Home Club URL': f"https://rankedin.com{player_home_club.get('Url', '')}",
                        'Ranking API URL': f"https://api.rankedin.com/v1/player/GetHistoricDataAsync?id={player_id}",
                    }

                    players_listings_dicts.append(datas)

                except Exception as player_error:
                    logger.error(f"Error processing player at index {idx} for team {team_id}: {player_error}")
                    continue

            logger.info(f"Successfully collected {len(players_listings_dicts)} players for team {team_id}")
            return players_listings_dicts

        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for team {team_id}: {str(e)}")

            # If this is not the last attempt, wait and retry
            if attempt < max_retries - 1:
                retry_delay = 15  # You might want to implement exponential backoff here
                logger.info(f"Retrying team {team_id} in {retry_delay:.1f} seconds... (attempt {attempt + 2}/{max_retries})")
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"All {max_retries} attempts failed for team {team_id}. Final error: {str(e)}")

    # If all retries failed
    return []


async def collect_data_concurrently(season_id_home, season_id_away, match_ids, max_concurrent=10, delay_between_batches=5):
    """
    Collect players, matches, and organizations data with limited concurrency

    Args:
        max_concurrent: Maximum number of concurrent tasks (default: 3)
        delay_between_batches: Delay in seconds between batches (default: 2)
    """
    logger.info("Starting concurrent data collection with limited concurrency...")

    # Combine home and away team IDs for unique team collection
    all_season_ids = season_id_home + season_id_away
    unique_season_ids = list(set(all_season_ids))
    random_delay = await random_interval(delay_between_batches)

    # Create team-match tuples with home/away distinction for matches
    team_match_tuples = list(zip(season_id_home, season_id_away, match_ids))

    # OPTION 1: Collect players data with limited concurrency
    logger.info(f"Collecting players data for {len(unique_season_ids)} teams with max {max_concurrent} concurrent...")
    all_players = []
    org_season_pairs = []

    # Process players in batches
    for i in range(0, len(unique_season_ids), max_concurrent):
        batch = unique_season_ids[i:i + max_concurrent]
        players_tasks = [get_players(str(season_id)) for season_id in batch]
        players_results = await asyncio.gather(*players_tasks, return_exceptions=True)

        # Process results for this batch
        for j, result in enumerate(players_results):
            if isinstance(result, Exception):
                logger.error(f"Error in players task {i+j}: {result}")
                continue

            if result:
                all_players.extend(result)
                season_id = batch[j]
                for player in result:
                    org_id = player.get('Team Organisation Id')
                    if org_id:
                        org_season_pairs.append((season_id, org_id))

        # Add delay between batches (except for last batch)
        if i + max_concurrent < len(unique_season_ids):
            await asyncio.sleep(random_delay)
            logger.info(f"Processed players batch {i//max_concurrent + 1}, waiting {int(random_delay)}s...")

    # OPTION 2: Collect matches data with limited concurrency
    logger.info(f"Collecting matches data for {len(team_match_tuples)} matches with max {max_concurrent} concurrent...")
    all_matches = []

    for i in range(0, len(team_match_tuples), max_concurrent):
        batch = team_match_tuples[i:i + max_concurrent]
        matches_tasks = [get_matches(str(home_id), str(away_id), str(match_id))
                        for home_id, away_id, match_id in batch]
        matches_results = await asyncio.gather(*matches_tasks, return_exceptions=True)

        # Process results for this batch
        for j, result in enumerate(matches_results):
            if isinstance(result, Exception):
                logger.error(f"Error in matches task {i+j}: {result}")
                continue

            if result:
                all_matches.extend(result)

        # Add delay between batches
        if i + max_concurrent < len(team_match_tuples):
            await asyncio.sleep(random_delay)
            logger.info(f"Processed matches batch {i//max_concurrent + 1}, waiting {int(random_delay)}s...")

    # OPTION 3: Collect organizations data with limited concurrency
    logger.info(f"Collecting organizations data for {len(org_season_pairs)} combinations with max {max_concurrent} concurrent...")
    all_organizations = []

    for i in range(0, len(org_season_pairs), max_concurrent):
        batch = org_season_pairs[i:i + max_concurrent]
        org_tasks = [get_organisation_id(str(season_id), str(org_id)) for season_id, org_id in batch]
        org_results = await asyncio.gather(*org_tasks, return_exceptions=True)

        # Process results for this batch
        for j, result in enumerate(org_results):
            if isinstance(result, Exception):
                logger.error(f"Error in organization task {i+j}: {result}")
                continue

            if result:
                all_organizations.extend(result)

        # Add delay between batches
        if i + max_concurrent < len(org_season_pairs):
            await asyncio.sleep(random_delay)
            logger.info(f"Processed organizations batch {i//max_concurrent + 1}, waiting {int(random_delay)}s...")

    # Drop duplicates from organization data
    if all_organizations:
        logger.info(f"Removing duplicates from {len(all_organizations)} organization records...")
        seen_combinations = set()
        deduplicated_orgs = []

        for org in all_organizations:
            key = (org.get('team_id_organisation'), org.get('Organisation_Id'))
            if key not in seen_combinations:
                seen_combinations.add(key)
                deduplicated_orgs.append(org)

        logger.info(f"Kept {len(deduplicated_orgs)} unique organization records after deduplication")
        all_organizations = deduplicated_orgs

    logger.info("Concurrent data collection completed!")

    return {
        'players': all_players,
        'matches': all_matches,
        'organizations': all_organizations
    }


'''async def collect_data_concurrently(season_id_home, season_id_away, match_ids,):
    """
    Collect players, matches, and organizations data concurrently
    """
    logger.info("Starting concurrent data collection...")

    # Combine home and away team IDs for unique team collection
    all_season_ids = season_id_home + season_id_away
    unique_season_ids = list(set(all_season_ids))

    # Create team-match tuples with home/away distinction for matches
    team_match_tuples = list(zip(season_id_home, season_id_away, match_ids))

    # Collect players data concurrently (now includes ranking positions)
    logger.info(f"Collecting players data for {len(unique_season_ids)} teams concurrently...")
    players_tasks = [get_players(str(season_id)) for season_id in unique_season_ids]
    players_results = await asyncio.gather(*players_tasks, return_exceptions=True)

    # Flatten players results and collect organization IDs
    all_players = []
    org_season_pairs = []  # Keep ALL pairs, not just unique ones

    for i, result in enumerate(players_results):
        if isinstance(result, Exception):
            logger.error(f"Error in players task {i}: {result}")
            continue

        if result:
            all_players.extend(result)
            # Collect ALL organization-season pairs (don't deduplicate yet)
            season_id = unique_season_ids[i]
            for player in result:
                org_id = player.get('Team Organisation Id')
                if org_id:
                    org_season_pairs.append((season_id, org_id))

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

    # FIXED: Don't deduplicate org_season_pairs - we want one record per team-org combination
    logger.info(f"Collecting organizations data for {len(org_season_pairs)} team-organization combinations concurrently...")

    # Create tasks for ALL team-organization combinations
    org_tasks = [get_organisation_id(str(season_id), str(org_id)) for season_id, org_id in org_season_pairs]
    org_results = await asyncio.gather(*org_tasks, return_exceptions=True)

    # Flatten organization results
    all_organizations = []
    for i, result in enumerate(org_results):
        if isinstance(result, Exception):
            logger.error(f"Error in organization task {i}: {result}")
            continue

        if result:
            all_organizations.extend(result)

    # Drop duplicates from organization data based on team_id_organisation and Organisation_Id
    if all_organizations:
        logger.info(f"Removing duplicates from {len(all_organizations)} organization records...")
        seen_combinations = set()
        deduplicated_orgs = []

        for org in all_organizations:
            # Create a unique key based on team_id and org_id
            key = (org.get('team_id_organisation'), org.get('Organisation_Id'))
            if key not in seen_combinations:
                seen_combinations.add(key)
                deduplicated_orgs.append(org)

        logger.info(f"Kept {len(deduplicated_orgs)} unique organization records after deduplication")
        all_organizations = deduplicated_orgs

    logger.info("Concurrent data collection completed!")

    return {
        'players': all_players,
        'matches': all_matches,
        'organizations': all_organizations
    }
'''


async def collect_all_league_data(league_id, pool_id, max_concurrent = 10, delay_between_batches = 5):
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
        main_data['round_ids'],
        max_concurrent = max_concurrent,
        delay_between_batches = delay_between_batches
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
    Collect all data and separate players data for individual processing
    """
    # Split combinations into batches of 5
    batches = [team_pool_combinations[i:i + batches] for i in range(0, len(team_pool_combinations), batches)]

    all_standings = []
    all_rounds = []
    all_players = []  # Keep collecting players data
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
                    record['season_id'] = season_id  # Add season_id for players too
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
        'players': all_players,  # Include players in return data
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
    timestamp = datetime.now(ZoneInfo("Europe/Copenhagen")).strftime("%Y-%m-%d_%H_%M_%S")
    division_name = await division_name_call(season_id)
    output_name = f"{division_name}_{timestamp}.xlsx"

    def expand_matches_to_7_rows(matches_df):
        """
        Ensure each Round_ID has exactly 7 rows total.
        For each Round_ID, ALL 7 rows will have the same Team_Home_ID_Matches and Team_Away_ID_Matches values.
        If a Round_ID has fewer than 7 rows, add empty rows to reach exactly 7.
        If a Round_ID has more than 7 rows, keep only the first 7 rows.
        """
        if matches_df.empty:
            return matches_df

        if 'Round_ID' not in matches_df.columns:
            logger.warning("Round_ID column not found, returning original DataFrame")
            return matches_df

        logger.info("Starting round processing to exactly 7 rows per Round_ID with consistent Team IDs")
        logger.info(f"Available columns: {matches_df.columns.tolist()}")

        # Find the correct column names for Team IDs (handle different naming variations)
        team_home_col = None
        team_away_col = None

        possible_home_names = ['Team_Home_ID_Matches', 'Team_ID_Home_Matches', 'Team_Home_ID', 'Home_Team_ID']
        possible_away_names = ['Team_Away_ID_Matches', 'Team_ID_Away_Matches', 'Team_Away_ID', 'Away_Team_ID']

        for col in matches_df.columns:
            if col in possible_home_names:
                team_home_col = col
            if col in possible_away_names:
                team_away_col = col

        logger.info(f"Found Team Home column: {team_home_col}")
        logger.info(f"Found Team Away column: {team_away_col}")

        # Group by Round_ID to identify unique rounds
        round_counts = matches_df['Round_ID'].value_counts()
        logger.info(f"Found {len(round_counts)} unique Round_IDs")

        expanded_rows = []
        rounds_expanded = 0
        rounds_trimmed = 0
        rows_added = 0
        rows_removed = 0

        for round_id in matches_df['Round_ID'].unique():
            round_matches = matches_df[matches_df['Round_ID'] == round_id]
            current_count = len(round_matches)

            # Get the template row (first row) to extract consistent values
            template_row = round_matches.iloc[0].copy()

            # Extract the consistent values that should be the same for all rows in this Round_ID
            consistent_values = {
                'season_id': template_row.get('season_id', ''),
                'pool_id': template_row.get('pool_id', ''),
                'Round_ID': round_id,
            }

            # Add team ID values if columns exist
            if team_home_col:
                consistent_values[team_home_col] = template_row.get(team_home_col, '')
            if team_away_col:
                consistent_values[team_away_col] = template_row.get(team_away_col, '')

            logger.debug(f"Round_ID {round_id}: Consistent values = {consistent_values}")

            if current_count < 7:
                # First, add existing rows but ensure they have consistent Team IDs
                for _, row in round_matches.iterrows():
                    updated_row = row.copy()
                    # Ensure consistent values for all rows in this Round_ID
                    for key, value in consistent_values.items():
                        if key in updated_row.index:
                            updated_row[key] = value
                    expanded_rows.append(updated_row)

                # Add empty rows to reach exactly 7 total
                rows_to_add = 7 - current_count
                for i in range(rows_to_add):
                    new_row = template_row.copy()

                    # Set consistent values first
                    for key, value in consistent_values.items():
                        if key in new_row.index:
                            new_row[key] = value

                    # Clear all other columns except the consistent ones
                    preserve_columns = list(consistent_values.keys())
                    for col in new_row.index:
                        if col not in preserve_columns:
                            new_row[col] = ''

                    expanded_rows.append(new_row)

                rounds_expanded += 1
                rows_added += rows_to_add
                logger.debug(f"Round_ID {round_id}: Had {current_count} rows, added {rows_to_add} empty rows to reach 7 total")

            elif current_count == 7:
                # Perfect - add all 7 rows but ensure consistent Team IDs
                for _, row in round_matches.iterrows():
                    updated_row = row.copy()
                    # Ensure consistent values for all rows in this Round_ID
                    for key, value in consistent_values.items():
                        if key in updated_row.index:
                            updated_row[key] = value
                    expanded_rows.append(updated_row)
                logger.debug(f"Round_ID {round_id}: Already has exactly 7 rows")

            else:
                # More than 7 rows - keep only the first 7 but ensure consistent Team IDs
                for i, (_, row) in enumerate(round_matches.iterrows()):
                    if i < 7:
                        updated_row = row.copy()
                        # Ensure consistent values for all rows in this Round_ID
                        for key, value in consistent_values.items():
                            if key in updated_row.index:
                                updated_row[key] = value
                        expanded_rows.append(updated_row)
                    else:
                        break

                rounds_trimmed += 1
                rows_removed += (current_count - 7)
                logger.debug(f"Round_ID {round_id}: Had {current_count} rows, trimmed to 7 rows (removed {current_count - 7} rows)")

        logger.info(f"Processed {len(round_counts)} rounds: expanded {rounds_expanded} rounds (added {rows_added} rows), trimmed {rounds_trimmed} rounds (removed {rows_removed} rows)")

        if expanded_rows:
            expanded_df = pd.DataFrame(expanded_rows)

            # Final verification - log a sample of the results
            if not expanded_df.empty:
                sample_round = expanded_df['Round_ID'].iloc[0]
                sample_data = expanded_df[expanded_df['Round_ID'] == sample_round]
                logger.info(f"Sample verification for Round_ID {sample_round}:")
                for col in [team_home_col, team_away_col]:
                    if col and col in sample_data.columns:
                        unique_values = sample_data[col].unique()
                        logger.info(f"  {col}: {unique_values} (should have only 1 unique value)")

            return expanded_df.reset_index(drop=True)
        else:
            return matches_df

    def remove_empty_duplicate_rows(df):
            """
            Remove empty/placeholder rows that were created during expansion.
            Removes rows that only have basic identifier columns filled but no actual match data.
            Only preserves rows that have meaningful match/player data beyond the basic identifiers.
            """
            if df.empty:
                return df

            initial_count = len(df)
            logger.info(f"Starting empty row removal with {initial_count} rows")

            # Find the correct column names for Team IDs (handle different naming variations)
            team_home_col = None
            team_away_col = None

            possible_home_names = ['Team_Home_ID_Matches', 'Team_ID_Home_Matches', 'Team_Home_ID', 'Home_Team_ID']
            possible_away_names = ['Team_Away_ID_Matches', 'Team_ID_Away_Matches', 'Team_Away_ID', 'Away_Team_ID']

            for col in df.columns:
                if col in possible_home_names:
                    team_home_col = col
                if col in possible_away_names:
                    team_away_col = col

            # Define basic identifier columns that don't count as "meaningful data"
            basic_identifier_columns = ['season_id', 'pool_id', 'Round_ID']
            if team_home_col:
                basic_identifier_columns.append(team_home_col)
            if team_away_col:
                basic_identifier_columns.append(team_away_col)

            # Define data columns that indicate this is a real match row with actual data
            meaningful_data_columns = []
            for col in df.columns:
                if col not in basic_identifier_columns:
                    meaningful_data_columns.append(col)

            logger.info(f"Basic identifier columns: {basic_identifier_columns}")
            logger.info(f"Meaningful data columns to check: {len(meaningful_data_columns)} columns")

            # Identify rows to keep: only rows that have meaningful data beyond basic identifiers
            rows_to_keep = []
            empty_rows_removed = 0

            for idx, row in df.iterrows():
                # Check if this row has any meaningful data beyond basic identifiers
                has_meaningful_data = False

                # Check meaningful data columns for actual content
                for col in meaningful_data_columns:
                    value = row[col]
                    if pd.notna(value) and str(value).strip() != '':
                        has_meaningful_data = True
                        break

                if has_meaningful_data:
                    rows_to_keep.append(idx)
                else:
                    empty_rows_removed += 1
                    logger.debug(f"Removing row with only basic identifiers - Round_ID: {row.get('Round_ID', 'Unknown')}")

            # Keep only rows with meaningful data beyond basic identifiers
            df_cleaned = df.loc[rows_to_keep].copy()

            final_count = len(df_cleaned)
            logger.info(f"Removed {empty_rows_removed} rows with only basic identifiers, {final_count} rows remaining")

            return df_cleaned.reset_index(drop=True)

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

            # Save consolidated matches data - WITH IMPROVED EXPANSION TO EXACTLY 7 ROWS
            if batch_data.get('matches'):
                matches_df = pd.DataFrame(batch_data['matches'])
                cols = matches_df.columns.tolist()
                if 'season_id' in cols and 'pool_id' in cols:
                    cols.remove('season_id')
                    cols.remove('pool_id')
                    cols = ['season_id', 'pool_id'] + cols
                    matches_df = matches_df[cols]

                # Check if we need to load existing Excel data for comparison
                # (This would be needed if you're updating an existing file)
                try:
                    # If updating existing file, you could load it here
                    # existing_df = pd.read_excel(output_name, sheet_name='Matches')
                    # combined_df = pd.concat([existing_df, matches_df], ignore_index=True)
                    # First remove empty duplicates, then expand
                    # matches_df = remove_empty_duplicate_rows(combined_df)
                    pass
                except:
                    # New file, use current data
                    pass

                # First remove any empty duplicate rows
                matches_df = remove_empty_duplicate_rows(matches_df)

                # Then expand matches to exactly 7 rows per Round_ID
                matches_df = expand_matches_to_7_rows(matches_df)

                # Remove empty set score columns
                set_columns = [col for col in matches_df.columns if 'Set Score' in col]
                for col in set_columns:
                    if matches_df[col].isna().all() or (matches_df[col] == '').all():
                        matches_df = matches_df.drop(columns=[col])

                matches_df.to_excel(writer, sheet_name='Matches', index=False)
                format_worksheet(writer.sheets['Matches'], matches_df)
                logger.info(f"Saved {len(matches_df)} final matches records (exactly 7 rows per Round_ID).")

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
                    # 'Total Final Players Records',
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
                    # len(batch_data.get('players', [])),
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

                # Save processing results if there are any
                if processing_results:
                    processing_df = pd.DataFrame(processing_results)
                    processing_df.to_excel(writer, sheet_name='Processing_Results', index=False)
                    format_worksheet(writer.sheets['Processing_Results'], processing_df)

        logger.info(f"Final batch report saved to {output_name}")
        logger.info(f"Report contains consolidated data from all processed league-pool combinations")
        logger.info(f"Each Round_ID has been processed to have exactly 7 rows for set tracking")
        return output_name

    except Exception as e:
        logger.error(f"Error saving final batch data to Excel: {str(e)}")
        return None


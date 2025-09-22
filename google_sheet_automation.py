from google.oauth2.service_account import Credentials
from scraper import division_name_call
from logger import setup_logger
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from datetime import datetime
import gspread.exceptions
from typing import Dict
import pandas as pd
import traceback
import asyncio
import gspread
import os


logger = asyncio.run(setup_logger('google_sheet_automation'))


load_dotenv()
LOAD_SPREADSHEET_ID = os.getenv('LOAD_SPREADSHEET_ID')
WRITE_SPREADSHEET_ID = os.getenv('WRITE_SPREADSHEET_ID')
PLAYER_SPREADSHEET_ID = os.getenv('PLAYER_SPREADSHEET_ID')
ADDITIONAL_PLAYER_SPREADSHEET_ID = os.getenv('ADDITIONAL_PLAYER_SPREADSHEET_ID')


'''async def load_players_stats_csv():
    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
                ]

        creds = Credentials.from_service_account_file('credentials.json', scopes = scope)
        client = gspread.authorize(creds)

        spreadsheet = client.open_by_key(PLAYER_SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet('Sheet1')

        data = worksheet.get_all_records()

        players_urls = [row['Player URL'] for row in data]

        return players_urls

    except Exception as e:
        logger.info(f"Error loading configuration from Google Sheets: {str(e)}")'''


async def compare_player_urls(scraped_players_data, reference_urls):
    try:
        scraped_urls = []
        for player in scraped_players_data:
            player_url = player.get('Player URL', '')
            if player_url:
                scraped_urls.append(player_url)

        scraped_urls_set = set(scraped_urls)
        reference_urls_set = set(reference_urls)

        matched_urls = scraped_urls_set.intersection(reference_urls_set)
        unmatched_in_scraped = scraped_urls_set - reference_urls_set
        unmatched_in_reference = reference_urls_set - scraped_urls_set

        unmatched_players_data = []
        for player in scraped_players_data:
            if player.get('Player URL', '') in unmatched_in_scraped:
                unmatched_players_data.append(player)

        comparison_result = {
            'total_scraped_urls': len(scraped_urls_set),
            'total_reference_urls': len(reference_urls_set),
            'matched_count': len(matched_urls),
            'unmatched_in_scraped_count': len(unmatched_in_scraped),
            'unmatched_in_reference_count': len(unmatched_in_reference),
            'matched_urls': list(matched_urls),
            'unmatched_in_scraped': list(unmatched_in_scraped),
            'unmatched_in_reference': list(unmatched_in_reference),
            'unmatched_players_data': unmatched_players_data
        }

        logger.info(f"Player URL Comparison Results:")
        logger.info(f"- Total scraped URLs: {len(scraped_urls_set)}")
        logger.info(f"- Total reference URLs: {len(reference_urls_set)}")
        logger.info(f"- Matched URLs: {len(matched_urls)}")
        logger.info(f"- Unmatched in scraped: {len(unmatched_in_scraped)}")
        logger.info(f"- Unmatched in reference: {len(unmatched_in_reference)}")

        return comparison_result

    except Exception as e:
        logger.error(f"Error comparing player URLs: {str(e)}")
        return {}


async def save_players_to_google_sheets(players_result: Dict, client_email: str = None):
    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
        ]

        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
        client = gspread.authorize(creds)

        # Open existing spreadsheet
        spreadsheet = client.open_by_key(ADDITIONAL_PLAYER_SPREADSHEET_ID)

        timestamp = datetime.now(ZoneInfo("Europe/Copenhagen")).strftime("%Y-%m-%d_%H:%M:%S")
        spreadsheet_name = f"Latest_Players_Stats_{timestamp}"
        spreadsheet.update_title(spreadsheet_name)

        def format_dataframe_for_sheets(df):
            if df.empty:
                return []

            headers = df.columns.tolist()
            data_rows = []

            for _, row in df.iterrows():
                row_data = []
                for value in row:
                    if pd.isna(value):
                        row_data.append('')
                    else:
                        row_data.append(str(value))
                data_rows.append(row_data)

            return [headers] + data_rows

        def column_number_to_letter(col_num):
            """Convert column number to Excel-style column letters (1=A, 26=Z, 27=AA, etc.)"""
            result = ""
            while col_num > 0:
                col_num -= 1  # Make it 0-indexed
                result = chr(65 + (col_num % 26)) + result
                col_num //= 26
            return result

        def format_worksheet(worksheet, data):
            """Format worksheet with headers, freeze, and auto-adjust columns and rows"""
            if not data:
                return

            # Clear and update data
            worksheet.clear()
            worksheet.update('A1', data)

            # Format headers (gray background and bold)
            header_range = f'A1:{column_number_to_letter(len(data[0]))}1'
            worksheet.format(header_range, {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })

            # Freeze header row
            worksheet.freeze(rows=1)

            # Auto-resize columns
            worksheet.columns_auto_resize(0, len(data[0]) - 1)

            # Auto-resize rows (adjust height based on content)
            worksheet.rows_auto_resize(0, len(data) - 1)

        def create_or_get_worksheet(spreadsheet, sheet_name):
            """Create a new worksheet or get existing one"""
            try:
                return spreadsheet.worksheet(sheet_name)
            except gspread.WorksheetNotFound:
                return spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=26)

        # Process players data directly from comparison_result
        if players_result.get('players'):
            players_df = pd.DataFrame(players_result['players'])

            # 2nd sheet: team_league (only specific columns with renamed headers)
            # Check which columns actually exist and use the correct case
            possible_columns = {
                'player_id': ['Player ID', 'player_id'],
                'rankedIn_id': ['RankedInId', 'rankedIn_id', 'rankedin_id'],
                'season_id': ['season_id', 'Season_ID'],
                'pool_id': ['pool_id', 'Pool_ID'],
                'team_id': ['Team_ID_Players', 'team_id', 'Team_ID'],
                'player_order': ['Player Order', 'player_order'],
                'player_type': ['Team Participant Type', 'player_type'],
                'has_license': ['Has License', 'has_license'],
                'team_organisation_id': ['Team Organisation Id', 'team_organisation_id'],
                'player_url': ['Player URL', 'player_url']
            }

            team_league_df_data = {}
            actual_columns_used = []

            for target_col, possible_names in possible_columns.items():
                for possible_name in possible_names:
                    if possible_name in players_df.columns:
                        team_league_df_data[target_col] = players_df[possible_name]
                        actual_columns_used.append(possible_name)
                        break

            if team_league_df_data:
                team_league_df = pd.DataFrame(team_league_df_data)

                # Add number_team_ids column
                # Count how many times each rankedIn_id appears associated with team_id
                if 'rankedIn_id' in team_league_df.columns and 'team_id' in team_league_df.columns:
                    # Create a count mapping for rankedIn_id occurrences
                    rankedIn_id_counts = team_league_df['rankedIn_id'].value_counts().to_dict()

                    # Add the count column
                    team_league_df['number_team_ids'] = team_league_df['rankedIn_id'].map(rankedIn_id_counts)

                    # Handle any NaN values that might occur from mapping
                    team_league_df['number_team_ids'] = team_league_df['number_team_ids'].fillna(0).astype(int)
                else:
                    # If required columns don't exist, add a column with 0s
                    team_league_df['number_team_ids'] = 0

                team_league_sheet = create_or_get_worksheet(spreadsheet, 'team_league')
                team_league_data = format_dataframe_for_sheets(team_league_df)
                format_worksheet(team_league_sheet, team_league_data)
                await asyncio.sleep(1)
                logger.info(f"Team League sheet created with columns: {list(team_league_df.columns)}")

            # 3rd sheet: ranking_name (only players with ranking name values)
            # Check for ranking columns with different possible names
            ranking_columns_map = {
                'player_id': ['Player ID', 'player_id'],
                'team_id': ['Team_ID_Players', 'team_id', 'Team_ID'],
                'rankedIn_id': ['RankedInId', 'rankedIn_id', 'rankedin_id'],
                'ranking_position': ['Ranking Position', 'ranking_position'],
                'ranking_name': ['Ranking Name', 'ranking_name'],
                'ranking_timestamp': ['Ranking Timestamp', 'ranking_timestamp'],
                'player_url': ['Player URL', 'player_url']
            }

            # Find actual column names for ranking data
            ranking_actual_columns = {}
            for target_col, possible_names in ranking_columns_map.items():
                for possible_name in possible_names:
                    if possible_name in players_df.columns:
                        ranking_actual_columns[target_col] = possible_name
                        break

            # Check if we have ranking data columns
            ranking_name_col = ranking_actual_columns.get('ranking_name')
            ranking_position_col = ranking_actual_columns.get('ranking_position')
            ranking_timestamp_col = ranking_actual_columns.get('ranking_timestamp')

            if ranking_name_col or ranking_position_col or ranking_timestamp_col:
                # Create filter mask for rows with ranking data
                ranking_mask = pd.Series([False] * len(players_df))

                if ranking_name_col:
                    ranking_mask |= (players_df[ranking_name_col].notna()) & (players_df[ranking_name_col] != '')
                if ranking_position_col:
                    ranking_mask |= (players_df[ranking_position_col].notna()) & (players_df[ranking_position_col] != '')
                if ranking_timestamp_col:
                    ranking_mask |= (players_df[ranking_timestamp_col].notna()) & (players_df[ranking_timestamp_col] != '')

                ranking_df_source = players_df[ranking_mask].copy()

                if not ranking_df_source.empty:
                    # Create ranking dataframe with available columns
                    ranking_df_data = {}
                    for target_col, actual_col in ranking_actual_columns.items():
                        if actual_col in ranking_df_source.columns:
                            ranking_df_data[target_col] = ranking_df_source[actual_col]

                    if ranking_df_data:
                        ranking_df = pd.DataFrame(ranking_df_data)
                        ranking_df = ranking_df.drop_duplicates(subset=['rankedIn_id'])

                        ranking_sheet = create_or_get_worksheet(spreadsheet, 'ranking_position_men_db')
                        ranking_data = format_dataframe_for_sheets(ranking_df)
                        format_worksheet(ranking_sheet, ranking_data)
                        await asyncio.sleep(1)

                        ranking_count = len(ranking_df)
                        logger.info(f"Ranking sheet created with {ranking_count} players")
                    else:
                        ranking_count = 0
                        # Create empty sheet
                        ranking_sheet = create_or_get_worksheet(spreadsheet, 'ranking_position_men_db')
                        ranking_sheet.clear()
                        ranking_sheet.update('A1', [['No ranking columns found']])
                else:
                    ranking_count = 0
                    # Create empty sheet
                    ranking_sheet = create_or_get_worksheet(spreadsheet, 'ranking_position_men_db')
                    ranking_sheet.clear()
                    ranking_sheet.update('A1', [['No players with ranking data found']])
            else:
                ranking_count = 0
                # Create empty sheet
                ranking_sheet = create_or_get_worksheet(spreadsheet, 'ranking_position_men_db')
                ranking_sheet.clear()
                ranking_sheet.update('A1', [['No ranking columns available']])

        # Share spreadsheet if email provided
        if client_email:
            try:
                spreadsheet.share(
                    client_email,
                    perm_type="user",
                    role="writer",
                    notify=True
                )
                logger.info(f"Shared Google Sheet with {client_email}")
            except Exception as e:
                logger.info(f"Could not share with {client_email}: {str(e)}")

        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{ADDITIONAL_PLAYER_SPREADSHEET_ID}"
        logger.info(f"Latest Players Google Sheets created with:")
        logger.info(f"  - Team League sheet: {len(players_result.get('players', []))} players")
        logger.info(f"  URL: {spreadsheet_url}")
        return spreadsheet_url

    except Exception as e:
        logger.error(f"Error saving players data to Google Sheets: {str(e)}")
        logger.error(f"Error details: {type(e).__name__}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


async def load_config_from_sheets():
    """
    Load configuration data from Google Sheets
    Returns a dictionary with Season_ID_Home, Season_ID_Away, and Round_ID lists
    """
    try:
        # Set up Google Sheets connection
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
        ]

        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
        client = gspread.authorize(creds)

        # Open the configuration spreadsheet
        spreadsheet = client.open_by_key(WRITE_SPREADSHEET_ID)

        # Assuming your config data is in a sheet named 'Config' or 'Sheet1'
        # Adjust the sheet name based on your actual setup
        try:
            worksheet = spreadsheet.worksheet('Rounds')  # Try 'Config' first
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.worksheet('Sheet1')  # Fallback to 'Sheet1'

        # Get all data from the sheet
        data = worksheet.get_all_records()

        if not data:
            raise ValueError("No data found in configuration sheet")

        # Convert to DataFrame for easier processing
        df = pd.DataFrame(data)

        def safe_to_list(series_or_list):
            """Safely convert pandas Series or list to list"""
            if hasattr(series_or_list, 'tolist'):
                return series_or_list.tolist()
            elif isinstance(series_or_list, list):
                return series_or_list
            else:
                return []

        config_data = {
            'Team_ID_Home': safe_to_list(df.get('Team_ID_Home', df.get('Home_Team_ID', []))),
            'Team_ID_Away': safe_to_list(df.get('Team_ID_Away', df.get('Away_Team_ID', []))),
            'Match ID': safe_to_list(df.get('Match ID', df.get('Match ID', [])))
        }

        logger.info(f"Loaded config data: {len(config_data['Team_ID_Home'])} home teams, "
              f"{len(config_data['Team_ID_Away'])} away teams, "
              f"{len(config_data['Match ID'])} matches")

        return config_data

    except Exception as e:
        logger.info(f"Error loading configuration from Google Sheets: {str(e)}")
        return None


async def load_league_pool_combinations_from_google_sheets(start_index=None, end_index=None):
    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]

        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
        client = gspread.authorize(creds)

        # Open by spreadsheet ID
        spreadsheet = client.open_by_key(LOAD_SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet('team_league_id&pool_id')

        values = worksheet.get('A:B')

        data_rows = values[1:] if len(values) > 0 and isinstance(values[0], list) else values
        df = pd.DataFrame(data_rows, columns=['col1', 'col2'])

        league_pool_combinations = []
        for _, row in df.iterrows():
            if len(row) >= 2 and pd.notna(row.iloc[0]) and pd.notna(row.iloc[1]):
                try:
                    league_id = int(row.iloc[0])
                    pool_id = int(row.iloc[1])
                    league_pool_combinations.append((league_id, pool_id))
                except (ValueError, TypeError):
                    continue

        # Apply start/end index range if specified
        if start_index is not None or end_index is not None:
            try:
                total_combinations = len(league_pool_combinations)

                # Set defaults
                start = start_index if start_index is not None else 0
                end = end_index if end_index is not None else total_combinations

                # Validate bounds
                start = max(0, min(start, total_combinations))
                end = max(start, min(end, total_combinations))

                if start >= total_combinations:
                    logger.warning(f"Start index {start} is out of range. Using empty list.")
                    league_pool_combinations = []
                else:
                    league_pool_combinations = league_pool_combinations[start:end]
                    logger.info(f"Selected combinations from index {start} to {end-1} (range {start}:{end})")

            except Exception as range_error:
                logger.warning(f"Error applying start/end index range: {str(range_error)}. Using all combinations.")

        logger.info(f"Loaded {len(league_pool_combinations)} league-pool combinations from Google Sheets")

        return league_pool_combinations

    except Exception as e:
        logger.info(f'Error loading league-pool combinations from Google Sheets: {str(e)}')
        return []
    

async def save_batch_to_google_sheets(batch_data: Dict, client_email: str = None):
    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
        ]

        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
        client = gspread.authorize(creds)

        # Open existing spreadsheet
        spreadsheet = client.open_by_key(WRITE_SPREADSHEET_ID)

        team_id = batch_data.get("standings")[0].get("season_id") if batch_data.get("standings") else "Unknown"
        division_name = await division_name_call(team_id) if team_id != "Unknown" else "Unknown_Division"
        timestamp = datetime.now(ZoneInfo("Europe/Copenhagen")).strftime("%Y-%m-%d_%H:%M:%S")
        spreadsheet_name = f"{division_name}_{timestamp}"
        spreadsheet.update_title(spreadsheet_name)

        def format_dataframe_for_sheets(df):
            if df.empty:
                return []

            headers = df.columns.tolist()
            data_rows = []

            for _, row in df.iterrows():
                row_data = []
                for value in row:
                    if pd.isna(value):
                        row_data.append('')
                    else:
                        row_data.append(str(value))
                data_rows.append(row_data)

            return [headers] + data_rows

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

        def remove_empty_rows(df):
            """
            Remove empty/placeholder rows that were created during expansion.
            Only removes rows that have no meaningful data (only key columns filled).
            Preserves all rows with actual player data or match information.
            Also preserves rows that have Team_ID information even if other data is empty.
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

            # Define key columns that should always have values (identifier columns)
            key_columns = ['season_id', 'pool_id', 'Round_ID']

            # Define team ID columns that indicate this is a valid match row
            team_id_columns = []
            if team_home_col:
                team_id_columns.append(team_home_col)
            if team_away_col:
                team_id_columns.append(team_away_col)

            # Define data columns that indicate this is a real match row (not empty placeholder)
            data_columns = []
            for col in df.columns:
                if col not in key_columns and col not in team_id_columns and any(keyword in col.lower() for keyword in
                    ['player', 'team', 'score', 'match', 'set', 'game', 'win', 'loss', 'point', 'name']):
                    data_columns.append(col)

            # If no data columns found, use all non-key columns (excluding team ID columns)
            if not data_columns:
                data_columns = [col for col in df.columns if col not in key_columns and col not in team_id_columns]

            logger.info(f"Key columns: {key_columns}")
            logger.info(f"Team ID columns: {team_id_columns}")
            logger.info(f"Data columns to check: {len(data_columns)} columns")

            # Identify rows to keep: rows that have meaningful data in data columns OR have team ID information
            rows_to_keep = []
            empty_rows_removed = 0

            for idx, row in df.iterrows():
                # Check if this row has any meaningful data in data columns
                has_data = False

                # First check data columns
                for col in data_columns:
                    value = row[col]
                    if pd.notna(value) and str(value).strip() != '':
                        has_data = True
                        break

                # If no data found, check if it has team ID information (which we want to preserve)
                if not has_data:
                    for col in team_id_columns:
                        if col in row.index:
                            value = row[col]
                            if pd.notna(value) and str(value).strip() != '':
                                has_data = True
                                break

                if has_data:
                    rows_to_keep.append(idx)
                else:
                    empty_rows_removed += 1
                    logger.debug(f"Removing empty row with Round_ID: {row.get('Round_ID', 'Unknown')}")

            # Keep only rows with actual data or team ID information
            df_cleaned = df.loc[rows_to_keep].copy()

            final_count = len(df_cleaned)
            logger.info(f"Removed {empty_rows_removed} empty placeholder rows, {final_count} rows remaining")

            return df_cleaned.reset_index(drop=True)

        def format_worksheet_preserve_manual(worksheet, data):
            """
            Only updates the automated data section, preserves manual additions
            """
            if not data:
                return

            def column_number_to_letter(col_num):
                """Convert column number to Excel-style column letters (1=A, 26=Z, 27=AA, etc.)"""
                result = ""
                while col_num > 0:
                    col_num -= 1  # Make it 0-indexed
                    result = chr(65 + (col_num % 26)) + result
                    col_num //= 26
                return result

            # Clear existing data and update with new data
            worksheet.clear()
            if data:
                worksheet.update('A1', data)

            # Format headers
            header_range = f'A1:{column_number_to_letter(len(data[0]))}1'
            worksheet.format(header_range, {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            worksheet.freeze(rows=1)

        def get_or_create_sheet(title, rows=100, cols=20):
            try:
                return spreadsheet.worksheet(title)
            except gspread.exceptions.WorksheetNotFound:
                return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)

        # Get list of sheets that should NOT be deleted (manual sheets)
        AUTOMATED_SHEETS = ['Standings', 'Rounds', 'Matches', 'Organizations', 'Executive Summary', 'Sheet1']

        # Keep track of sheets we've created/updated
        sheets_created = []

        # Save Standings
        if batch_data.get('standings'):
            standings_df = pd.DataFrame(batch_data['standings'])
            cols = standings_df.columns.tolist()
            if 'season_id' in cols and 'pool_id' in cols:
                cols.remove('season_id')
                cols.remove('pool_id')
                cols = ['season_id', 'pool_id'] + cols
                standings_df = standings_df[cols]

            standings_sheet = get_or_create_sheet('Standings')
            sheets_created.append('Standings')
            standings_data = format_dataframe_for_sheets(standings_df)
            format_worksheet_preserve_manual(standings_sheet, standings_data)
            await asyncio.sleep(1)

        # Save Rounds
        if batch_data.get('rounds'):
            rounds_df = pd.DataFrame(batch_data['rounds'])
            cols = rounds_df.columns.tolist()
            if 'season_id' in cols and 'pool_id' in cols:
                cols.remove('season_id')
                cols.remove('pool_id')
                cols = ['season_id', 'pool_id'] + cols
                rounds_df = rounds_df[cols]

            rounds_sheet = get_or_create_sheet('Rounds')
            sheets_created.append('Rounds')
            rounds_data = format_dataframe_for_sheets(rounds_df)
            format_worksheet_preserve_manual(rounds_sheet, rounds_data)
            await asyncio.sleep(1)

        # Save Matches - WITH EXPANSION TO 7 ROWS
        if batch_data.get('matches'):
            matches_df = pd.DataFrame(batch_data['matches'])
            cols = matches_df.columns.tolist()
            if 'season_id' in cols and 'pool_id' in cols:
                cols.remove('season_id')
                cols.remove('pool_id')
                cols = ['season_id', 'pool_id'] + cols
                matches_df = matches_df[cols]

            # DON'T merge with existing data - start fresh each time to ensure consistency
            # This prevents issues with empty Team ID values from previous runs

            # First remove any empty duplicate rows
            matches_df = remove_empty_rows(matches_df)

            # Then expand matches to exactly 7 rows per Round_ID
            matches_df = expand_matches_to_7_rows(matches_df)

            # Remove empty set score columns
            set_columns = [col for col in matches_df.columns if 'Set Score' in col]
            for col in set_columns:
                if matches_df[col].isna().all() or (matches_df[col] == '').all():
                    matches_df = matches_df.drop(columns=[col])

            matches_sheet = get_or_create_sheet('Matches')
            sheets_created.append('Matches')
            matches_data = format_dataframe_for_sheets(matches_df)
            format_worksheet_preserve_manual(matches_sheet, matches_data)
            await asyncio.sleep(1)

        # Save Organizations
        if batch_data.get('organizations'):
            organizations_df = pd.DataFrame(batch_data['organizations'])
            cols = organizations_df.columns.tolist()
            if 'season_id' in cols and 'pool_id' in cols:
                cols.remove('season_id')
                cols.remove('pool_id')
                cols = ['season_id', 'pool_id'] + cols
                organizations_df = organizations_df[cols]

            orgs_sheet = get_or_create_sheet('Organizations')
            sheets_created.append('Organizations')
            orgs_data = format_dataframe_for_sheets(organizations_df)
            format_worksheet_preserve_manual(orgs_sheet, orgs_data)
            await asyncio.sleep(1)

        # Executive Summary
        summary_data = [
            ['Metric', 'Count/Value'],
            ['Total League-Pool Combinations Processed', str(batch_data.get('total_processed', 0))],
            ['Successful Combinations', str(len(batch_data.get('successful_combinations', [])))],
            ['Failed Combinations', str(len(batch_data.get('failed_combinations', [])))],
            ['Success Rate (%)', str(round((len(batch_data.get('successful_combinations', [])) / max(batch_data.get('total_processed', 1), 1)) * 100, 2))],
            ['Total Final Standings Records', str(len(batch_data.get('standings', [])))],
            ['Total Final Rounds Records', str(len(batch_data.get('rounds', [])))],
            # ['Total Final Players Records', str(len(batch_data.get('players', [])))],
            ['Total Final Matches Records', str(len(batch_data.get('matches', [])))],
            ['Total Final Organizations Records', str(len(batch_data.get('organizations', [])))],
            ['Report Generated', datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        ]

        summary_sheet = get_or_create_sheet('Executive Summary')
        sheets_created.append('Executive Summary')
        format_worksheet_preserve_manual(summary_sheet, summary_data)
        await asyncio.sleep(1)

        # Delete Sheet1 only if we have created other sheets AND it's empty/default
        if sheets_created:
            try:
                default_sheet = spreadsheet.worksheet("Sheet1")
                sheet_data = default_sheet.get_all_values()
                if not sheet_data or (len(sheet_data) == 1 and not any(sheet_data[0])):
                    spreadsheet.del_worksheet(default_sheet)
                    logger.info("Deleted empty default Sheet1")
                else:
                    logger.info("Preserved Sheet1 as it contains data")
            except gspread.exceptions.WorksheetNotFound:
                pass

        if client_email:
            try:
                spreadsheet.share(
                    client_email,
                    perm_type="user",
                    role="writer",
                    notify=True
                )
                logger.info(f"Shared Google Sheet with {client_email}")
            except Exception as e:
                logger.info(f"Could not share with {client_email}: {str(e)}")

        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{WRITE_SPREADSHEET_ID}"
        return spreadsheet_url

    except Exception as e:
        logger.info(f"Error saving batch data to Google Sheets: {str(e)}")
        return None


async def save_hourly_matches_only_to_google_sheets(matches_data: list, client_email: str = None):
    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
        ]

        creds = Credentials.from_service_account_file('credentials.json', scopes = scope)
        client = gspread.authorize(creds)

        spreadsheet = client.open_by_key(WRITE_SPREADSHEET_ID)

        def format_dataframe_for_sheets(df):
            if df.empty:
                return []

            headers = df.columns.tolist()
            data_rows = []

            for _, row in df.iterrows():
                row_data = []
                for value in row:
                    if pd.isna(value):
                        row_data.append('')
                    else:
                        row_data.append(str(value))
                data_rows.append(row_data)
            return [headers] + data_rows

        def column_number_to_letter(col_num):
            result = ""
            while col_num > 0:
                col_num = 1
                result = chr(65 + (col_num % 26)) + result
                col_num //= 26
            return result

        def update_matches_sheet_only(worksheet, data):
            if not data:
                return

            # Get existing data to check for manual additions
            try:
                existing_data = worksheet.get_all_values()
                existing_rows = len(existing_data) if existing_data else 0
                new_data_rows = len(data)

                # Clear only the area where new data will be written
                if existing_rows > 0:
                    end_col = column_number_to_letter(len(data[0]) if data and data[0] else 'A')
                    clear_range = f'A1:{end_col}{new_data_rows}'
                    worksheet.batch_clear([clear_range])

                # Write new data
                worksheet.update('A1', data)

                # If there were manual rows beyond the new data, they remain untouched
                logger.info(f"Preserved {max(0, existing_rows - new_data_rows)} manual rows in Matches sheet")
            except Exception as e:
                logger.info(f"Could not preserve manual data in Matches sheet: {str(e)}")
                worksheet.clear()
                worksheet.update('A1', data)

            # Format headers
            header_range = f"A1:{column_number_to_letter(len(data[0]))}1"
            worksheet.format(header_range, {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            worksheet.freeze(rows = 1)

        if matches_data:
            matches_df = pd.DataFrame(matches_data)

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
                    matches_df = matches_df.drop(columns = [col])

            # Get or create Matches sheet
            try:
                matches_sheet = spreadsheet.worksheet('Matches')
            except gspread.exceptions.WorksheetNotFound:
                matches_sheet = spreadsheet.add_worksheet(title = 'Matches', rows = 1000, cols = 30)

            # Format and update the sheet
            matches_data_formatted = format_dataframe_for_sheets(matches_df)
            update_matches_sheet_only(matches_sheet, matches_data_formatted)

            # Add timestamp to indicate last update
            try:
                timestamp = datetime.now(ZoneInfo('Europe/Copenhagen')).strftime('%Y-%m-%d %H:%M:%S')
                matches_sheet.update('AA1', f'Lat updated: {timestamp}')
            except Exception as e:
                logger.info(f"Could not addd timestamp: {str(e)}")

            logger.info(f"Successfully updated Matcehs sheet with {len(matches_data)} records at {timestamp}")

        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{WRITE_SPREADSHEET_ID}"
        return spreadsheet_url

    except Exception as e:
        logger.info(f"Error saving matches data to Google Sheets: {str(e)}")
        return None


from google.oauth2.service_account import Credentials
from scraper import division_name_call
from logger import setup_logger
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from datetime import datetime
import gspread.exceptions
from typing import Dict
import pandas as pd
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
        import traceback
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

            # Get existing data to check for manual additions
            try:
                existing_data = worksheet.get_all_values()
                existing_rows = len(existing_data) if existing_data else 0
                new_data_rows = len(data)

                # Clear only the area where new data will be written
                if existing_rows > 0:
                    # Clear from A1 to the end of new data columns
                    end_col = column_number_to_letter(len(data[0])) if data and data[0] else 'A'
                    clear_range = f'A1:{end_col}{new_data_rows}'
                    worksheet.batch_clear([clear_range])

                # Write new data
                worksheet.update('A1', data)

                # If there were manual rows beyond the new data, they remain untouched
                logger.info(f"Preserved {max(0, existing_rows - new_data_rows)} manual rows in {worksheet.title}")

            except Exception as e:
                # Fallback to original behavior if there's an issue
                logger.info(f"Could not preserve manual data in {worksheet.title}: {e}")
                worksheet.clear()
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

        # Save Players
        # if batch_data.get('players'):
        #     players_df = pd.DataFrame(batch_data['players'])
        #     cols = players_df.columns.tolist()
        #     if 'pool_id' in cols:
        #         # cols.remove('season_id')
        #         cols.remove('pool_id')
        #         cols = ['pool_id'] + cols
        #     players_sheet = get_or_create_sheet('Players')
        #     sheets_created.append('Players')
        #     players_data = format_dataframe_for_sheets(players_df)
        #     format_worksheet_preserve_manual(players_sheet, players_data)
        #     await asyncio.sleep(1)
       # Save Matches
        if batch_data.get('matches'):
            matches_df = pd.DataFrame(batch_data['matches'])
            cols = matches_df.columns.tolist()
            if 'season_id' in cols and 'pool_id' in cols:
                cols.remove('season_id')
                cols.remove('pool_id')
                cols = ['season_id', 'pool_id'] + cols
                matches_df = matches_df[cols]

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


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

        print(f"Loaded config data: {len(config_data['Team_ID_Home'])} home teams, "
              f"{len(config_data['Team_ID_Away'])} away teams, "
              f"{len(config_data['Match ID'])} matches")

        return config_data

    except Exception as e:
        print(f"Error loading configuration from Google Sheets: {str(e)}")
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
        AUTOMATED_SHEETS = ['Standings', 'Rounds', 'Players', 'Matches', 'Organizations', 'Executive Summary', 'Sheet1']

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
        if batch_data.get('players'):
            players_df = pd.DataFrame(batch_data['players'])
            cols = players_df.columns.tolist()
            if 'pool_id' in cols:
                # cols.remove('season_id')
                cols.remove('pool_id')
                cols = ['pool_id'] + cols
                players_df = players_df[cols]

            players_sheet = get_or_create_sheet('Players')
            sheets_created.append('Players')
            players_data = format_dataframe_for_sheets(players_df)
            format_worksheet_preserve_manual(players_sheet, players_data)
            await asyncio.sleep(1)

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
            ['Total Final Players Records', str(len(batch_data.get('players', [])))],
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


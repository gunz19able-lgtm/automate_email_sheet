from google_sheet_automation import load_config_from_sheets, save_hourly_matches_only_to_google_sheets
from scraper import get_matches
from tools import random_interval
from scraper import get_matches
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import asyncio
import logging


logging.basicConfig(
    level = logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def collect_matches_data_only(team_id_home, team_id_away, match_ids, batch_size, batch_delay):
    
    logger.info("Starting hourly matches data collection with batched processing...")

    # Create team-match tuples with home/away distinction for matches
    team_match_tuples = list(zip(team_id_home, team_id_away, match_ids))
    total_matches = len(team_match_tuples)
    
    logger.info(f"Processing {total_matches} matches in batches of {batch_size} with {batch_delay}s delay between batches")

    all_matches = []
    successful_batches = 0
    failed_requests = 0

    # Process matches in batches
    for batch_num, i in enumerate(range(0, total_matches, batch_size), 1):
        batch_tuples = team_match_tuples[i:i + batch_size]
        batch_start_time = datetime.now()
        
        logger.info(f"Processing batch {batch_num}/{(total_matches + batch_size - 1) // batch_size} "
                   f"(matches {i+1}-{min(i+batch_size, total_matches)})")

        # Create tasks for current batch
        batch_tasks = [get_matches(str(home_id), str(away_id), str(match_id))
                      for home_id, away_id, match_id in batch_tuples]

        try:
            # Execute batch concurrently
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            # Process batch results
            batch_matches = []
            batch_errors = 0

            for j, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Error in batch {batch_num}, match {j+1}: {result}")
                    batch_errors += 1
                    failed_requests += 1
                    continue

                if result:
                    batch_matches.extend(result)

            all_matches.extend(batch_matches)
            successful_batches += 1

            batch_duration = datetime.now() - batch_start_time
            logger.info(f"Batch {batch_num} completed in {batch_duration.total_seconds():.2f}s: "
                       f"{len(batch_matches)} matches collected, {batch_errors} errors")

        except Exception as e:
            logger.error(f"Critical error in batch {batch_num}: {e}")
            failed_requests += len(batch_tasks)

        # Add delay between batches (except for the last batch)
        if i + batch_size < total_matches:
            logger.info(f"Waiting {int(batch_delay)}s before next batch...")
            await asyncio.sleep(batch_delay)

    # Log final statistics
    success_rate = (successful_batches / ((total_matches + batch_size - 1) // batch_size)) * 100 if total_matches > 0 else 0
    logger.info(f"Hourly matches data collection completed!")
    logger.info(f"Summary: {len(all_matches)} total matches collected, "
               f"{successful_batches} successful batches, {failed_requests} failed requests")
    logger.info(f"Batch success rate: {success_rate:.1f}%")

    return all_matches
    

async def main():
    try:
        start_time = datetime.now(ZoneInfo('Europe/Copenhagen'))
        logger.info(f"Starting hourly Matches update at {start_time}")

        config_data = await load_config_from_sheets()

        if not config_data:
            logger.error(f"Failed to load configuration data")
            return

        # Extract the required data with proper type handling
        def safe_extract_ids(data, column_name):
            """Safely extract and convert IDs, handling various data types"""
            ids = []
            for x in data.get(column_name, []):
                try:
                    # Skip if NaN or None
                    if pd.isna(x) or x is None:
                        continue

                    # Convert to string first, then check if it's meaningful
                    str_x = str(x).strip()

                    # Skip empty strings, 'nan', 'None', or boolean strings
                    if not str_x or str_x.lower() in ['nan', 'none', 'true', 'false']:
                        continue

                    # Try to convert to int
                    int_x = int(float(str_x))  # Use float first to handle strings like "123.0"
                    ids.append(int_x)

                except (ValueError, TypeError, AttributeError) as e:
                    logger.warning(f"Skipping invalid {column_name} value: {x} (error: {e})")
                    continue
            return ids

        team_id_home = safe_extract_ids(config_data, 'Team_ID_Home')
        team_id_away = safe_extract_ids(config_data, 'Team_ID_Away')
        match_ids = safe_extract_ids(config_data, 'Match ID')

        logger.info(f"Processing {len(team_id_home)} home teams, {len(team_id_away)} away teams, {len(match_ids)} matches")

        if not team_id_home or not team_id_away or not match_ids:
            logger.error("No valid data found in configuration")
            return

        # Collect only matches data
        batches = 25
        random_delay = await random_interval(3)
        matches_data = await collect_matches_data_only(team_id_home, team_id_away, match_ids,  batches, random_delay)

        if not matches_data:
            logger.warning("No matches data collected")
            return

        logger.info(f"Collected {len(matches_data)} match records")

        # Save only matches data to Google Sheets
        spreadsheet_url = await save_hourly_matches_only_to_google_sheets(matches_data)

        if spreadsheet_url:
            end_time = datetime.now(ZoneInfo("Europe/Copenhagen"))
            duration = end_time - start_time

            logger.info(f"Hourly matches update completed successfully in {duration}")
            logger.info(f"Spreadsheet URL: {spreadsheet_url}")


        else:
            logger.error("Failed to save matches data to Google Sheets")

    except Exception as e:
        logger.error(f"Error in hourly matches update: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())


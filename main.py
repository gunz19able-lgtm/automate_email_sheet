from google_sheet_automation import load_league_pool_combinations_from_google_sheets
from automation import scrape_and_email_batch_tennis_data_with_player_comparison
from logger import setup_logger
from dotenv import load_dotenv
import asyncio
import time
import os


logger = asyncio.run(setup_logger('main'))


load_dotenv()
CLIENT_EMAIL = os.getenv('CLIENT_EMAIL')


async def main():
    league_pool_combinations = await load_league_pool_combinations_from_google_sheets()

    recipients = [
        CLIENT_EMAIL,
    ]

    cc_recipients = []
    bcc_recipients = []

    success = await scrape_and_email_batch_tennis_data_with_player_comparison(
        league_pool_combinations = league_pool_combinations,
        recipients = recipients,
        cc_emails = cc_recipients if cc_recipients else None,
        bcc_emails = bcc_recipients if bcc_recipients else None,
        client_email = CLIENT_EMAIL,
        delay = 5,
        batches = 5

    )

    if success:
        logger.info("Complete workflow finished successfully!")
        logger.info(f"Data scraped and email sent")
    else:
        logger.info("Workflow failed, Check logs for details.")


if __name__ == '__main__':
    start_time = time.time()
    try:
        results = asyncio.run(main())
        logger.info(results)
        end_time = time.time()
        execution_time = round(end_time - start_time, 2)
        logger.info(f"\nExecution time:\n---------------\n{execution_time} second/s.\n{round(execution_time / 60, 2)} minute/s.\n{round(execution_time / 3600, 2)} hour/s")

    except Exception as e:
        logger.exception(f'Error encountered {str(e)}')



from openpyxl.utils import get_column_letter
from fake_useragent import UserAgent
from logger import setup_logger
from primp import AsyncClient
import pandas as pd
import itertools
import asyncio
import random
import os


logger = asyncio.run(setup_logger('tools'))


async def flat(d_lists):
    return list(itertools.chain(*d_lists))


async def random_useragent():
    agent = UserAgent()
    return agent.random


async def random_interval(interval):
    return random.uniform(1, interval + 1)


async def make_requests(url, headers):
    async with AsyncClient(impersonate = 'chrome_131', impersonate_os = 'windows') as client:
        response = await client.get(url, headers = headers)

        if response.status_code != 200:
            return f"Red alert: Status code = {response.status_code}!"

        return response


async def auto_adjust_column_width(worksheet, dataframe):
    for col_num, column in enumerate(dataframe.columns, 1):
        column_letter = get_column_letter(col_num)

        max_length = len(str(column))

        for value in dataframe[column].astype(str):
            if len(value) > max_length:
                max_length = len(value)

        adjusted_width = min(max_length + 2, 50)
        worksheet.column_dimensions[column_letter].width = adjusted_width


async def save_to_excel(standings_dataframe, rounds_dataframe, players_dataframe, file_name):
    directory_name = 'rankedin datasets'
    os.makedirs(f"{directory_name}", exist_ok=True)

    file_path = f"{directory_name}/{file_name}.xlsx"

    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        standings_df = pd.DataFrame(data=standings_dataframe)
        rounds_df = pd.DataFrame(data=rounds_dataframe)
        players_df = pd.DataFrame(data=players_dataframe)
        filter_players_df = players_df.drop_duplicates()

        standings_df.to_excel(writer, sheet_name='standings', index=False)
        rounds_df.to_excel(writer, sheet_name='rounds', index=False)
        filter_players_df.to_excel(writer, sheet_name='players', index=False)

        workbook = writer.book

        standings_sheet = workbook['standings']
        await auto_adjust_column_width(standings_sheet, standings_df)
        standings_sheet.freeze_panes = 'A2'

        rounds_sheet = workbook['rounds']
        await auto_adjust_column_width(rounds_sheet, rounds_df)
        rounds_sheet.freeze_panes = 'A2'

        players_sheet = workbook['players']
        await auto_adjust_column_width(players_sheet, filter_players_df)
        players_sheet.freeze_panes = 'A2'

    logger.info(f"Data saved successfully to {file_path}")

    # IMPORTANT: Return the file path
    return file_path



from datetime import datetime
import asyncio

from scraper import Scraper
from gecko_scraper import GeckoScraper

from match_parser import MatchParser
from database_worker import DataBaseWorker

class ParseLiveStandaloneV:
    dbw = DataBaseWorker(
        'data/parse_live_standalone_data.sqlite3',
        'matches',
        'data/parse_live_standalone_ended.sqlite3',
        'ended'
    )

async def parse_live(live_url, scrap_method=Scraper.get_raw_data,
dbw=ParseLiveStandaloneV.dbw, dt=10):
    # End Of Match
    def eom(mp):
        # return ''
        return mp.time_shift > 120

    iteration = -1

    raw_data = await scrap_method(live_url)
    mp = MatchParser(raw_data)
    while not eom(mp):
        iteration += 1
        print('[i] iteration %d [%s]' % (iteration, live_url))
        # await ??
        asyncio.get_event_loop().create_task(dbw.write_data(mp))

        await asyncio.sleep(dt)

        raw_data = await scrap_method(live_url)
        mp = MatchParser(raw_data)

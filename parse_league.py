import asyncio

from league_parser import LeagueParser
from cron_worker import CronWorker
from scraper import Scraper

from parse_live import parse_live
from _config import config


def generate_prematch_task(match_name):
    task = '''cd %s && python3 %s --findlive %s''' % (
        config['api']['path'],
        config['api']['prog'],
        match_name.replace(' ', '_'))
    return task

async def parse_league(league_url, cron=None):
    print('[ ] parsing league %s'  % league_url)
    print('[ ] obtainig league data %s' % league_url)
    raw_data = await Scraper.get_raw_data(league_url)
    print('[ ] league data obtaied; len = %s; %s' % (len(raw_data), league_url))

    lp = LeagueParser(league_url)
    lt = await lp.parse(raw_data)

    loop = asyncio.get_event_loop()

    w = False
    if cron is None:
        cron = CronWorker()
        w = True

    for index, row in lt.iterrows():
        url = row.events
        datetime_ = row.datetime

        if 'live' in url:
            print("[i] parse_league<%s> processing live match %s" % (league_url, url))
            loop.create_task(parse_live(url))

        elif 'prematch' in url:
            print('[i] processing prematch %s' % url)

            match_name = lp.get_match_name(url)
            cron.pushTask(generate_prematch_task(match_name))
            cron.pushComment(url)
            cron.pushDatetime(datetime_)
            cron.commit()

    if w:
        cron.write()

    print('[*] league<%s> parsed' % league_url)


async def main():
    loop = asyncio.get_event_loop()
    league_url = input('Enter league_url $ ')
    await parse_league(league_url)
    loop.stop()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    loop.create_task(main())
    loop.run_forever()

    pending = asyncio.Task.all_tasks(loop=loop)
    group = asyncio.gather(*pending, return_exceptions=True)
    loop.run_until_complete(group)
    loop.close()

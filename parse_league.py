import asyncio
from datetime import timedelta

from league_parser import LeagueParser
from cron_worker import CronWorker
from scraper import Scraper

from parse_live import parse_live
from _config import config


def generate_findlive_task(match_name):
    task = '''cd %s && python3 %s --findlive %s''' % (
        config['api']['path'],
        config['api']['prog'],
        match_name.replace(' ', '_'))
    return task

def generate_deltask_task(match_url):
    task = '''cd %s && python3 %s --deltask %s''' % (
        config['api']['path'],
        config['api']['prog'],
        match_url)
    return task


async def parse_league(league_url_, cron=None):
    print('[ ] parsing league %s'  % league_url_)

    # preprocess league_url_
    league_url = league_url_
    if 'oddsfan.ru' in league_url_:
        league_url = league_url_.replace('oddsfan.ru', 'oddsfan.com')
        print('[ ] Change league url to ` .com ` : %s' % league_url)

    print('[ ] obtainig league data %s' % league_url)
    raw_data = await Scraper.get_raw_data(league_url)
    print('[*] league data obtaied; len = %s; %s' % (len(raw_data), league_url))

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
            cron.pushTask(generate_findlive_task(match_name))
            cron.pushComment(url)
            cron.pushDatetime(datetime_ + timedelta(minutes=3))
            cron.commit()

            cron.pushTask(generate_deltask_task(url))
            cron.pushComment(url)
            cron.pushDatetime(datetime_ + timedelta(minutes=5))
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

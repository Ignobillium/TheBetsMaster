import asyncio

from lxml import html

from scraper import Scraper
from cron_worker import CronWorker
from parse_league import parse_league

async def parse_all(cron_=None):
    async def get_countries():
        raw_data = await Scraper.get_raw_data('https://www.oddsfan.com/sport/soccer')

        page = html.fromstring(raw_data)
        a = page.xpath('//li[@class="open-country-popup-js"]/a')
        hrefs = list('https://oddsfan.com/%s' % i.get("href") for i in a)

        return hrefs

    async def process_country(href):
        raw_data2 = await Scraper.get_raw_data(href)

        page2 = html.fromstring(raw_data2)
        a2 = page2.xpath('//ul/li[@class="list-item"]/a')
        hrefs2 = list('https://oddsfan.com/%s' % i.get('href') for i in a2)

        return hrefs2

    async def _parse_league(league_url, cron=None):
        try:
            await parse_league(league_url, cron=cron)
            print('[*] league processed [%s]' % league_url)
        except:
            print('[!] league not processed! [%s]' %  league_url)

    hrefs = await get_countries()

    tasks = []
    for href in hrefs:
        task = asyncio.ensure_future(process_country(href))
        tasks.append(task)

    data = await asyncio.gather(*tasks)
    leagues_urls = []
    for d in data:
        leagues_urls += d

    id_ = 0
    del tasks
    tasks = []

    if cron_ is None:
        cron = CronWorker()
    else:
        cron = cron_

    for league_url in leagues_urls:
        print('%3d / %3d processed' % (id_, len(leagues_urls)) )
        task = asyncio.ensure_future(_parse_league(league_url, cron=cron))
        tasks.append(task)
        id_ += 1

    await asyncio.gather(*tasks)

    cron.write()

    print('[ ] Parsing_all complete!')

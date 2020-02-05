from datetime import datetime
import asyncio
import logging
import traceback

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
    err_counter = 0

""" async def parse_live(live_url_, scrap_method=Scraper.get_raw_data,
dbw=ParseLiveStandaloneV.dbw, dt=10):
    # End Of Match
    def eom(mp):
        # return ''
        return mp.time_shift > 120

    async def parsing_iteration(live_url_, scrap_method, dbw, dt):
        t0 = datetime.now()

        raw_data = await scrap_method(live_url_)
        mp = MatchParser(raw_data)

        asyncio.get_event_loop().create_task(dbw.write_data(mp))

        t_sleep = (datetime.now() - t0).seconds
        if dt > t_sleep:
            await asyncio.sleep(dt - t_sleep)

        return mp

    t_start = datetime.now()

    if 'oddsfan.com' in live_url_:
        print('[i] There is ` .com ` in live_url (parse_live); replacing...')
        live_url = live_url_.replace('oddsfan.com', 'oddsfan.ru')
    else:
        live_url = live_url_

    iteration = 0
    while True:
        print('[ ] parsing live iteration %3d # %s' % (iteration, live_url))
        try:
            mp = await parsing_iteration(live_url, scrap_method, dbw, dt)

            if (datetime.now() - t_start).seconds > 8100:
                break
            elif eom(mp):
                break
        except:
            print('[!] An exception occurs in %s' % live_url)
        finally:
            iteration += 1
 """


async def parse_live(live_url, scrap_method=Scraper.get_raw_data,
dbw=ParseLiveStandaloneV.dbw, dt=10):
    def process_url(url):
        """Проверяет корректность url (начличие live) и преобразует его к .ru
        по необходимости.

        Parameters
        ----------
        url : ` str `
            Url, который предлагается парсить.

        Returns
        ----------
        `.ru`-url либо None, если url некорректный.
        """
        if not 'live' in url:
            logging.error('There is no `live` in given url <%s>' % url)
            return None

        if 'oddsfan.com' in url:
            return url.replace('oddsfan.com', 'oddsfan.ru')
        else:
            return url

    async def save_html_for_analysis(html_data):
        print('[ ] Handling <shape of passed values> exception')
        with open('data/exceptions/%s' % datetime.now()) as f:
            f.write(mp.data)
        with open('data/exceptions/exception.txt') as f:
            f.write(traceback.format_exc())
        print('[*] Html data saved to data/exceptions')

    async def get_primary_cache(url, scrap_method, max_iter=10):
        """Извлекает постоянные матча: названия команд и дату начала.

        Parameters
        ----------
        url : ` str `
            Url, который предлагается парсить.

        scrap_method : ` coroutine `
            Метод, получающий данные по url.

        max_iter : ` int `
            Максимальное число попыток получения данных.

        Returns
        ----------
            mp, cache : ` MatchParser `, ` dict `
                mp : объект MatchParser, сконструированный по последним (корректным)
                данным
                cache : словарь с ключами 'team_1', 'team_2', 'match_datetime'
        """
        succsess = False
        iteration = 0

        while not succsess:
            raw_data = await scrap_method(url)

            try:
                mp = MatchParser(raw_data)
                t1 = mp.team1
                t2 = mp.team2
                dt = mp.match_datetime

                if dt is not None:
                    succsess = True
                else:
                    logging.error('Can`t detect match_datetime <%s>' % url)

                if iteration == max_iter:
                    logging.error('Iteraion limit reached # %s' % url)
                    return None, None
            # no `teams` block in the given data
            except IndexError:
                logging.error('Can`t detect teams name')
                logging.exception(traceback.format_exc())
            except ValueError as e:
                if 'Shape of passed values' in str(e):
                    await save_html_for_analysis(raw_data)
                    logging.exception(traceback.format_exc())
            finally:
                await asyncio.sleep(iteration)
                iteration += 1

        return mp, {'team1': t1, 'team2': t2, 'match_datetime': dt}

    def end_of_match(mp):
        """Проверяет, наступил ли конец матча.

        Parameters
        ----------
        mp : ` MatchParser `
            Объект MatchParser, матч которого нужно проверит на завершение.

        Returns
        ----------
            True если да, False если нет.
        """
        return 'матч завершен' in mp.data.lower() or mp.time_shift > 115 # 90 + 15 + 10


    logging.debug('Processing url <%s>' % live_url)
    url = process_url(live_url)
    if url is None:
        logging.error('Given incorrect url <%s>' % live_url)
        ParseLiveStandaloneV.err_counter += 1
        return

    status = Scraper.get_status(url)
    if status is not None:
        logging.debug('Given request `parse_live` for url <%s> with status %s' % (url, status))
        if status == 'parse_live':
            logging.warning('Duplicated request parse_live <%s>' % url)
            # may be stop parsing
    else:
        logging.debug('Set `parsing_live` status to %s' % url)
        Scraper.set_status(url, 'parsing_live')

    logging.debug('Generating primary cache for <%s>' % url)
    mp, cache = await get_primary_cache(url, scrap_method)

    if mp is None:
        logging.error('No primary cache for <%s> break' % url)
        ParseLiveStandaloneV.err_counter += 1
        return

    #? нужен ли здесь try-except?
    #? или забить и обработать иначе?
    try:
        assert isinstance(cache['team1'], str) and len(cache['team1']) > 0
        assert isinstance(cache['team2'], str) and len(cache['team2']) > 0
        assert isinstance(cache['match_datetime'], datetime)
    except AssertionError:
        logging.error('Cant generate primary cache for <%s>' % url)
        logging.exception(traceback.format_exc())
        asyncio.get_event_loop().create_task(
            parse_live(live_url, scrap_method, dbw, dt))
        ParseLiveStandaloneV.err_counter += 1
        return

    logging.debug('Start parsing <%s>' % url)

    iteration = -1
    t_sleep = dt
    while not end_of_match(mp):
        t0 = datetime.now()

        mp.team1 = cache['team1']
        mp.team2 = cache['team2']
        mp.match_datetime = cache['match_datetime']

        err_ = mp.check_data()

        if len(err_) == 0:
            logging.debug('Common iteration %3d # %s' % (iteration, url))

            mt = mp.match_table
            asyncio.get_event_loop().create_task(dbw.write_data(mt))

            tk = datetime.now()
            if dt > (tk - t0).seconds:
                t_sleep = dt - (tk - t0).seconds
        else:
            ParseLiveStandaloneV.err_counter += len(err_)

            if MatchParser.Err.work_in_progress in err_:
                logging.error('Work in progress. Break')
                Scraper.set_status(url, 'work in progress')
                return
            if MatchParser.Err.no_match_datetime in err_:
                logging.error('Dont`now how and why, but there is no match_datetime for %s' % url)
                asyncio.get_event_loop().create_task(
                    parse_live(live_url, scrap_method, dbw, dt))
                return
            if MatchParser.Err.no_match_name in err_:
                logging.error('Dont`now how and why, but there is no match_name for %s' % url)
                asyncio.get_event_loop().create_task(
                    parse_live(live_url, scrap_method, dbw, dt))
                return
            if MatchParser.Err.no_current_score in err_:
                logging.error('There is no current score for %s' % url)
                try:
                    if mp.match_datetime > datetime.now():
                        t_sleep = (mp.match_datetime - datetime.now()).seconds

                    mt = mp.match_table
                    asyncio.get_event_loop().create_task(
                        dbw.write_data(mt))
                    t_sleep = dt
                except:
                    pass
            if MatchParser.Err.no_odds_available in err_:
                logging.info('No odds available at this moment for <%s>; sleep for %d' % (url, 3*dt))
                # await asyncio.sleep(3 * dt)
                t_sleep = 3 * dt
            if MatchParser.Err.no_ratio_block in err_:
                logging.info('No ratio blocks available for <%s> at this moment; sleep for %d' % (url, dt))
                # await asyncio.sleep(dt)
                t_sleep = dt

        await asyncio.sleep(t_sleep)

        iteration += 1
        if iteration % 3 == 0:
            print('[ ] parsing_live iteartion %3d # %s' % (iteration, url))

        q = 10
        while q > 0:
            try:
                raw_data = await scrap_method(url)
                mp = MatchParser(raw_data)
                break
            except ValueError as e:
                if 'Shape of passed values' in str(e):
                    await save_html_for_analysis(raw_data)
                    logging.exception(traceback.format_exc())
            except:
                logging.exception(traceback.format_exc())
            finally:
                q -= 1
                await asyncio.sleep(dt)
        else:
            logging.error('Too many trying for reach %s' % url)
            Scraper.set_status(url, 'Xyeta')
            asyncio.get_event_loop().create_task(
                parse_live(live_url, scrap_method, dbw, dt))
        # try:
        #     raw_data = await scrap_method(url)
        #     mp = MatchParser(raw_data)
        # except:
        #     logging.error('')

    dbw.mark_match_as_ended(mp)


async def main():
    loop = asyncio.get_event_loop()
    live_url = input('Enter live_url $ ')
    await parse_live(live_url)
    loop.stop()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    loop.create_task(main())
    loop.run_forever()

    pending = asyncio.Task.all_tasks(loop=loop)
    group = asyncio.gather(*pending, return_exceptions=True)
    loop.run_until_complete(group)
    loop.close()

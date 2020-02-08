import asyncio
import aiohttp.client_exceptions
import logging
from datetime import datetime, timedelta
import traceback

from lxml import html
import pandas as pd
import numpy as np

from scraper import Scraper
from abstract_parser import AbstractParser
from database_worker import DataBaseWorker


# TODO:
# Решить, нужно ли здесь вести logging

class OddsfanParser:
    drop_data_path = 'data/exceptions'
    dbw = DataBaseWorker(
        'data/oddsfan_data.sqlite3',
        'matches',
        'data/oddsfan_ended.sqlite3',
        'ended')

    @staticmethod
    async def dropData(data, msg):
        """Сохраняет страницу, вызвавшую исключение, для последующего анализа.

        Parameters
        ----------
        data : ` str ` or ` bytes `
            html-код страницы, вызвавшей исключение.

        msg : ` str ` or ` bytes `
            Сопровождающее сообщение (что за ошибку вызвала эта страница).
        """
        fname = str(datetime.now())
        with open('%s/%s.html' % (OddsfanParser.drop_data_path, fname), 'w') as f:
            f.write(data)
        with open('%s/%s.msg' % (OddsfanParser.drop_data_path, fname), 'w') as f:
            f.write(msg)

    @staticmethod
    def instance(raw_data, cache=None):
        """Возвращает объект AbstractParser, сконфигурированный под парсинг oddsfan.

        Parameters
        ----------
        raw_data : ` str ` or ` bytes `
            HTML-код страницы.

        cache : ` AbstractParser.Cache `
            Кэшированные данные матча, остающиеся неизменными:
                - названия команд
                - дата и время начала матча

        Returns
        ----------
        OddsfanParser : объект AbstractParser, сконфигурированный под парсинг oddsfan.

        Raises
        ----------
        # TODO AssertationError, если присваиваешь кривым функциям
        AbstractParser.exceptions.CacheError
            При некорректном содержимом кэша.

        TypeError
            1) если type(raw_data) отличен от `str` или `bytes
                # raises in AbstractParser.__init__
                TypeError("Given raw_data is %s, not str or bytes" % type(raw_data))
            2) если cache is not None и type(cache) отличен от AbstractParser.Cache
                # raises непосредственно в instance() во время инициации полей парсера кэшем
                TypeError('`cache` must be AbstractParser.Cache or dict, not %s' % type(cache))
        """
        def findTeams(self):
            """Ищет блок с названиями команд.

            Parameters
            ----------
            self : ` AbstractParser `
                Объект AbstractParser, содержащий в себе html-код страницы.

            Returns
            ----------
            teams : ` list of lxml.html.HtmlElement `; len = 2
                Список из двух элементов, содержащий названия состязающихся команд.

            Raises
            ----------
            AbstractParser.exceptions.TeamsError
                В случае, если длина полученного списка != 2.
            """
            teams = self.page.find_class('team-name')
            if len(teams) != 2:
                msg = 'Invalid `team-name` block len (%d)' % len(teams)
                logging.error('msg')
                raise AbstractParser.exceptions.TeamsError(msg, self, len(teams))
            return teams

        def _findHome(self):
            """Возвращает название первой команды (хозяев)."""
            return findTeams(self)[0].text_content().replace('\n', '')

        def _findAway(self):
            """Возвращает название второй команды (гостей)."""
            return findTeams(self)[1].text_content().replace('\n', '')

        def _findCurrentScore(self):
            """Возвращает текущий счёт в виде кортежа из двух элементов.

            Parameters
            ----------
            self : ` AbstractParser `
                AbstractParser текущего матча.

            Returns
            ----------
            current_score : ` list of float `
                (score_1, score_2).

            Raises
            ----------
            AbstractParser.exceptions.ScoreError
                В случае, если блок с текущим счётом не найден.
            """
            current_score_block = self.page.find_class('current_score')
            if len(current_score_block) == 0:
                #! error message
                msg = 'There is no current_score block'
                logging.error(msg)
                raise AbstractParser.exceptions.ScoreError(msg, self)

            return tuple(
                map(
                    lambda x: int(x),
                    current_score_block[0].text_content().split()[0].split(':')))

        def _findDatetime(self):
            """Возвращает время начала матча объектом datetime.datetime.

            Parameters
            ----------
            self : ` AbstractParser `
                AbstractParser текущего матча.

            Returns
            ----------
            match_datetime : ` datetime.datetime `
                Время начала матча.
            """
            datetime_block = self.page.find_class('date')
            if len(datetime_block) == 0:
                msg = 'Can`t find datetime block'
                logging.error(msg)
                raise AbstractParser.exceptions.DatetimeError(msg, self)

            datetime_string = datetime_block[0].text_content()
            datetime_list = [i.replace(',', '') for i in datetime_string.split('\n') if len(i) > 0]
            match_datetime = pd.to_datetime('%s %s' % (
                datetime_list[1], datetime_list[-1])).to_pydatetime() + timedelta(hours=3)

            return match_datetime

        def _generateMatchTable(self):
            def getBlockName(self, block):
                name_container = block.xpath('a')
                if len(name_container) != 1:
                    # raise RuntimeError("len(name_container) = %d" % len(name_container))
                    raise AbstractParser.exceptions.BlockNameError('Can`t detect block name', self, block)
                return name_container[0].text_content().replace('\n', '').lower()

            # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = #
            blocks = self.page.find_class('ratio-block')
            if len(blocks) == 0:
                # raise RuntimeError("")
                raise AbstractParser.exceptions.NoRatioBlocks("Can`t find ratio blocks", self)

            # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = #
            hda_block = 0
            for j, block in enumerate(blocks):
                try:
                    block_name = getBlockName(self, block)
                    if block_name == '1-x-2':
                        hda_block = block
                        break
                except AbstractParser.exceptions.BlockNameError as e:
                    logging.exception(traceback.format_exc())
                    asyncio.get_event_loop().create_task(
                        OddsfanParser.dropData(
                            e.parser.data, 'AbstractParser.exceptions.BlockNameError, block[%d]' % j))
            if hda_block == 0:
                raise AbstractParser.exceptions.NoHDABlock('Can`t find HDA block', self)

            # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = # = #
            #! Здесь может быть ошибка пустого списка block.find_class('ratio-row')
            # first_row = block.find_class('ratio-row')[0]
            #* Исправляю
            ratio_rows = block.find_class('ratio-row')
            if len(ratio_rows) > 0:
                first_row = ratio_rows[0]
            else:
                raise AbstractParser.exceptions.RatioRowsError(
                    'Can`t find ratio rows here', self, block)

            n_cols_total = len(first_row.getchildren())
            # n_k = n_cols_total - (открыть/закрыть' + 'payout')
            n_k = n_cols_total - 2

            p_names = list(map(
                lambda x: x.text_content().replace('\n', '').lower(),
                first_row.find_class('bold')))
            k_names = list(map(
                lambda x: 'k_%s' % x,
                p_names))

            # TODO:
            # решить проблему reshape
            try:
                odds = np.array(
                    list(map(
                        lambda x: float(x) if len(x) > 0 and x != '-' else np.nan,
                        [i.text_content().replace('\n', '')
                        for i in block.find_class('number')[n_k:]]
                    ))).reshape((-1, n_k))
            except:
                raise AbstractParser.exceptions.ReshapingError(traceback.format_exc(), self)

            bookies = [i.text_content().replace('\n', '') for i in block.find_class('bookmaker-name')]

            # коэффициенты
            k = pd.DataFrame(data=odds, columns=k_names, index=bookies[n_k:])
            #print(k)
            # вероятности
            p = pd.DataFrame(data=np.nanmean(odds ** -1, axis=0).reshape((1,n_k)), columns=p_names)

            try:
                score1 = self.current_score[0]
                score2 = self.current_score[1]
            except AbstractParser.exceptions.ScoreError:
                logging.error('ScoreError')
                score1, score2 = None, None

            _match_table = pd.DataFrame({
                'match_name': self.match_name,
                'time_shift': self.shift,
                'score1': score1,
                'score2': score2}, index=[1])

            for bookie in ['Fonbet', 'LigaStavok']:
                cols = [
                    '%s_%s' % (k_name, bookie)
                    for k_name in k_names
                ]

                _match_table[cols] = pd.DataFrame(
                    columns=cols,
                    data=np.array([np.nan, np.nan, np.nan]).reshape((1, 3)), index=[1])

                if bookie in k.index:
                    data = k.loc[bookie].values.reshape((-1, len(cols)))
                    _match_table[cols] = pd.DataFrame(columns=cols, data=data, index=[1])

            cols = p.columns.values
            data = p.values.reshape((-1, len(cols)))
            _match_table[cols] = pd.DataFrame(columns=cols, data=data, index=[1])

            print(_match_table)

            return _match_table

        parser = AbstractParser(raw_data)
        parser.findHome = _findHome
        parser.findAway = _findAway
        parser.findCurrentScore = _findCurrentScore
        parser.findDatetime = _findDatetime
        parser.generateMatchTable = _generateMatchTable

        if isinstance(cache, AbstractParser.Cache):
            # if isinstance(cache, AbstractParser.Cache):
            #     parser.home = cache.home
            #     parser.away = cache.away
            #     parser.datetime = cache.match_datetime
            # elif isinstance(cache, dict):
            #     if  'home' in cache.keys() and 'away' in cache.keys() and 'match_datetime' in cache.keys():
            #         parser.home = cache['home']
            #         parser.away = cache['away']
            #         parser.datetime = cache['match_datetime']
            #     else:
            #         raise ValueError('Wrong keys in cache dict: %s' % cache.keys())
            parser.home = cache.home
            parser.away = cache.away
            parser.datetime = cache.match_datetime
        elif cache is not None:
            raise TypeError('`cache` must be AbstractParser.Cache or dict, not %s' % type(cache))

        return parser


async def parse_live(live_url, scrap_method=Scraper.get_raw_data, dbw=OddsfanParser.dbw, dt=10):

    def process_url(url):
        if 'live' not in url:
            return
        elif 'oddsfan.com' in url:
            return url.replace('oddsfan.com', 'oddsfan.ru')
        else:
            return url

    async def generate_primary_cache(url, scrap_method=Scraper.get_raw_data, max_iter=10):
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
            cache : ` AbstractParser.Cache
                Кэшированные постоянные матча.
        """
        iteration = max_iter

        while iteration >= 0:
            try:
                raw_data = await scrap_method(url)
                parser = OddsfanParser.instance(raw_data)
                t1 = parser.home
                t2 = parser.away
                dt = parser.match_datetime
                return AbstractParser.Cache(t1, t2, dt)
            except aiohttp.client_exceptions.ClientPayloadError:
                # TODO:
                #! Вписать обработку этого исключения в Scraper.get_raw_data
                #! А то слишком уж часто мелькает и всё не отследить
                print('\n[!] aiohttp.client_exceptions.ClientPayloadError # %s\n' % url)
            # except Exception as e:
            #     print('[!] another exception occurs # %s' % url)
            #     print(str(e))
            finally:
                await asyncio.sleep(iteration)
                iteration -= 1

    url = process_url(live_url)
    if url is None:
        logging.error('Invalid url # %s' % live_url)
        return

    cache = await generate_primary_cache(url)
    if cache is None:
        logging.error('Can`t generate primary cache # %s' % url)
        asyncio.get_event_loop().create_task(
            parse_live(url, scrap_method)
        )

    isEndOfMatch = lambda x: x.shift > 110 or 'матч завершен' in x.data.lower()
    end_of_match = False
    iteration = 0
    t_sleep = dt
    while not end_of_match:
        try:
            raw_data = await scrap_method(url)
            parser = OddsfanParser.instance(raw_data, cache)
            mt = parser.match_table
            asyncio.get_event_loop().create_task(
                dbw.write_data(mt))
            print(mt)

            end_of_match = isEndOfMatch(parser)
            t_sleep = dt
        except aiohttp.client_exceptions.ClientPayloadError:
            print('[!] aiohttp.client_exceptions.ClientPayloadError # %s' % url)
            t_sleep = dt
        # except Exception as e:
            # print('[!] Another exception occurs:\n\ttype = %s' % type(e))
            # t_sleep = dt
        finally:
            print('[ ] [parse_live] iteration %3d complete # %s' % (iteration, url))
            await asyncio.sleep(t_sleep)
            iteration += 1
    dbw.mark_match_as_ended(parser)
    print('[*] [parsing_live] match parsed # %s' % url)

async def main():
    loop = asyncio.get_event_loop()
    live_url = input('Enter live_url $ ')
    await parse_live(live_url)
    loop.stop()


if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)-8s [%(asctime)s] %(message)s',
        level=logging.DEBUG)
    loop = asyncio.get_event_loop()

    loop.create_task(main())
    loop.run_forever()

    pending = asyncio.Task.all_tasks(loop=loop)
    group = asyncio.gather(*pending, return_exceptions=True)
    loop.run_until_complete(group)
    loop.close()

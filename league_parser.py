from datetime import datetime, timedelta

import pandas as pd
from lxml import html

from datetime_parser import parse_datetime

class LeagueParser:
    """Инструмент для парсинга html-страницы лиги в pandas-таблицу.
    """
    def __init__(self, league_url):
        """
        Parameters
        ----------
        league_url : `str`
            Суть url лиги. Используется в методе parse_by_URL и для ведения логов.
        """
        self.league_url = league_url
        self.league_table = None
        self.pre_urls = None
        self.live_urls = None

    def get_match_name(self, match_url):
        if self.league_table is None:
            print('[!] league table is None! Cant process get_match_name(%s)' % match_url)
            return None

        names = self.league_table[self.league_table.events == match_url].names
        if len(names):
            return names.values[0]
        else:
            return None

    async def parse(self, raw_data, encoding='utf-8'):
        """Превращает сырые данные (если они корректны) в удобную для работы
        pandas-таблицу. Заполняет поля self.live_urls и self.pre_urls.

        Parameters
        ----------
        raw_data : `bytes` or `str`
            Суть html-страница лиги.

        Returns
        ----------
        league_table : pandas.DataFrame
            Возвращает данные лиги в виде таблицы со столбцами
                datetime : `pandas datetime`, дата и время матча
                events: `str`, суть url матча
        """
        if isinstance(raw_data, bytes):
            self.data = raw_data.decode(encoding)
        elif isinstance(raw_data, str):
            self.data = raw_data

        self.league_page = html.fromstring(self.data)
        try:
            self.league_table = pd.read_html(self.data)[0]
        except:
            print('[!] There is no league table %s' % self.league_url)
            return

        self.live_urls = [
            'https://oddsfan.ru/%s' % i.get('href')
            for i in self.league_page.xpath("//tr[@class='live']/td[@class='event-holder']/a")]

        self.pre_urls  = [
            'https://oddsfan.ru/%s' % i.get('href')
            for i in self.league_page.xpath("//td[@class='event-holder']/a") if not 'live' in i.get('href')]

        self.league_table.rename(columns={
            'Unnamed: 0': 'datetime',
            'СОБЫТИЯ': 'events'}, inplace=True)

        for j, i in enumerate(self.league_table.datetime):
            self.league_table.datetime[j] = parse_datetime(self.league_table.datetime[j])

        self.league_table.datetime += pd.to_timedelta(3, unit='h')
        self.league_table['names'] = self.league_table['events']
        self.league_table['events'] = self.live_urls + self.pre_urls
        # self.league_table = self.league_table[self.league_table.columns.values[[0,2]]]

        return self.league_table[['datetime', 'events']]

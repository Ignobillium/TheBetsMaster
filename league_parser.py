from datetime import datetime, timedelta

import pandas as pd
from lxml import html

from scraper import Scraper

from datetime import datetime, timedelta

import pandas as pd
from lxml import html

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

    async def parse_by_raw_data(self, raw_data):
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
        self.raw_data = raw_data

        self.league_page = html.fromstring(self.raw_data.decode('utf-8'))
        try:
            self.league_table = pd.read_html(self.raw_data)[0]
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

        today_s = datetime.now().strftime('%Y-%m-%d')
        tomorrow_s = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        for j, i in enumerate(self.league_table.datetime):
            if 'Live' in i:
                self.league_table.datetime[j] = pd.to_datetime(datetime.now())
            elif 'Сегодня' in i:
                time_s = i.replace('\n', '').split()[-1]
                datetime_s = '%s %s' % (today_s, time_s)
                self.league_table.datetime[j] = pd.to_datetime(datetime_s)
            elif 'Завтра' in i:
                time_s = i.replace('\n', '').split()[-1]
                datetime_s = '%s %s' % (tomorrow_s, time_s)
                self.league_table.datetime[j] = pd.to_datetime(datetime_s)
            else:
                self.league_table.datetime[j] = pd.to_datetime(self.league_table.datetime[j])

        self.league_table.datetime += pd.to_timedelta(3, unit='h')
        self.league_table['events'] = self.live_urls + self.pre_urls
        self.league_table = self.league_table[self.league_table.columns.values[[0,2]]]

        return self.league_table

    async def parse_by_URL(self):
        """Загружает сырые данные посредством Scraper.get_raw_data(self.league_url)
        и преобразует их в удобную для работы pandas-таблицу посредством
        self.parse_by_raw_data. Заполняет поля self.live_urls и self.pre_urls.

        Returns
        ----------
        league_table : pandas.DataFrame
            Возвращает данные лиги в виде таблицы со столбцами
                datetime : `pandas datetime`, дата и время матча
                events: `str`, суть url матча
        """
        raw_data = await Scraper.get_raw_data(self.league_url)
        return await self.parse_by_raw_data(raw_data)

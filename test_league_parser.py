import pytest

from league_parser import LeagueParser

class TestLeagueParser:

    def get_data(self):
        with open('data/for_tests/league_page1.html', 'r') as f:
            return f.read()

    @pytest.mark.asyncio
    async def test_parse_by_raw_data(self):
        data = self.get_data()

        lp = LeagueParser('https://www.oddsfan.ru/sport/soccer/country/israel/league/4182')
        lt = await lp.parse(data)

        datetimes = (
            '2020-01-25 16:00:00',
            '2020-01-25 18:30:00',
            '2020-01-25 19:30:00',
            '2020-01-25 20:50:00',
            '2020-01-26 20:00:00',
            '2020-01-26 21:15:00',
            '2020-01-27 21:15:00')

        pre_urls = (
            'https://www.oddsfan.ru/prematch/events/187946308',
            'https://www.oddsfan.ru/prematch/events/187946254',
            'https://www.oddsfan.ru/prematch/events/187943461',
            'https://www.oddsfan.ru/prematch/events/187946224',
            'https://www.oddsfan.ru/prematch/events/187928926',
            'https://www.oddsfan.ru/prematch/events/187928914',
            'https://www.oddsfan.ru/prematch/events/187946296')

        i = 0
        for index, row in lt.iterrows():
            assert str(row.datetime) == datetimes[i]
            assert row.events.split('/')[-1] == pre_urls[i].split('/')[-1]
            i += 1

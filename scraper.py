import asyncio
from aiohttp import ClientSession

class Scraper:
    """Отвечает только за загрузку данных из интернета
    """
    _status = {}
    _sem = asyncio.Semaphore(1500)

    @staticmethod
    async def get_raw_data(url):
        """Загружает данные по указанному url

        Parameters
        ----------
        url : `string`
            Суть url

        Returns
        ----------
        _raw_data : `bytes`
            Суть загруженные данные в сыром виде
        """
        Scraper._status[url] = 'Obtaining data'
        async with ClientSession() as sess:
            async with Scraper._sem:
                async with sess.get(url) as response:
                    _raw_data = await response.read()
                    Scraper._status[url] = 'Data obtained'
                    return _raw_data

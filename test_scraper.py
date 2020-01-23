import asyncio

import pytest

from scraper import Scraper

@pytest.mark.asyncio
async def test_get_raw_data():
    data = await Scraper.get_raw_data('https://yandex.ru')
    decoded_data = data.decode('utf-8')
    assert 'Яндекс' in decoded_data

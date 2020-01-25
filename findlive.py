import asyncio

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from match_parser import MatchParser
from gecko_scraper import GeckoScraper


async def _findlive_lemma(_gs, match_name):
    sf  = 'select2-search__field'
    lih = 'select2-results__option--highlighted'
    li  = 'select2-results__option'
    ul  = 'select2-results__options'

    await _gs.new_tab('https://oddsfan.ru')

    wait = WebDriverWait(_gs.driver, 10)
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, sf)))

    el = _gs.driver.find_element_by_class_name(sf)
    el.click()
    el.send_keys(match_name)

    wait.until(EC.presence_of_element_located((By.CLASS_NAME, lih)))

    el = _gs.driver.find_element_by_class_name(lih)
    el.click()

    wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'team-name')))

    return _gs.driver.current_url


async def findlive(_gs, match_name):
    current_url = _gs.driver.current_url
    try:
        live_url = await _findlive_lemma(_gs, match_name)
    except:
        if _gs.driver.current_url != current_url:
            live_url = _gs.driver.current_url
        else:
            print('НЕ ПОЛУЧИЛОСЬ')
            live_url = None

        return live_url

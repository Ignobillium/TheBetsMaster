import asyncio

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from match_parser import MatchParser
from gecko_scraper import GeckoScraper
from api import TBMApi


async def _findlive_lemma(_gs, match_name):
    sf  = 'select2-search__field'
    lih = 'select2-results__option--highlighted'
    li  = 'select2-results__option'
    ul  = 'select2-results__options'

    await _gs.new_tab('https://oddsfan.com')

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


async def findlive(match_name):
    print('[ ] handle request findlive %s' % match_name)
    print('[ ] init gecko_scraper')

    _gs = GeckoScraper()

    try:
        print('[ ] working with gecko_scraper')
        live_url = await _findlive_lemma(_gs, match_name)
        print('[*] complete with %s' % match_name)
    except:
        if 'live' in _gs.driver.current_url:
            print('[i] sending request parse_live to parsing_server')
            live_url = _gs.driver.current_url
            api = TBMApi()
            api.parse(live_url)
            print('[*] complete sending request parse_live to parsing_server')
        else:
            print('НЕ ПОЛУЧИЛОСЬ НАЙТИ live ДЛЯ %s' % match_name)
            live_url = None

    print('[i] %s => %s' % (live_url, match_name))

    print('[i] quit gecko_scraper')
    _gs.quit()

    print('[*] request findlive(%s) handled' % match_name)

    return live_url

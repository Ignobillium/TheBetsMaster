import asyncio
import logging

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

class GeckoScraper:

    @staticmethod
    def get_headless_options():
        opts = Options()
        # ?opts.add_argument("--disable-extensions")
        # ?opts.add_argument("--disable-gpu")
        # ?opts.add_argument("--no-sandbox) # linux only
        opts.add_argument("--headless")
        return opts

    @staticmethod
    def headless():
        return  GeckoScraper(options = GeckoScraper.get_headless_options())

    def __init__(self, options=None, lock=None):
        gecko_path = '/home/ignobillium/dev/geckodriver'

        if options is None:
            self.driver = webdriver.Firefox(executable_path=gecko_path)
        else:
            self.driver = webdriver.Firefox(executable_path=gecko_path, options=options)

        self.driver.get("http://www.python.org")

        assert "Python" in self.driver.title

        self.current_url = 'http://www.python.org'
        self.current_tab = self.driver.window_handles

        self.tabs = {}
        self.tabs[self.current_url] = self.current_tab

        if lock is None:
            self.lock = asyncio.Lock()
        elif isinstance(lock, asyncio.Lock):
            self.lock = lock
        else:
            logging.error("`lock` param must be asyncio.Lock obj, not %s" % type(lock))
            raise TypeError("`lock` param must be asyncio.Lock obj, not %s" % type(lock))

    def __del__(self):
        if self.driver is not None:
            self.driver.quit()

    def quit(self):
        self.driver.quit()
        self.driver = None

    async def new_tab(self, url):
        if url in self.tabs.keys():
            return

        # switch to last tab
        with await self.lock:
            self.driver.switch_to.window(self.driver.window_handles[-1])
            self.driver.execute_script('window.open("%s");' % url)
            self.driver.switch_to.window(self.driver.window_handles[-1])

            self.tabs[url] = self.current_tab
            self.current_url = url
            self.current_tab = self.driver.current_window_handle

            # заменить на "дождаться критерия загрузки страницы"
            await asyncio.sleep(10)


    def switch_to_tab(self, url):
        # добавить обработку исключений
        # и подумать о механике переключения current_tab: не собьётся ли из-за async
        if self.current_url == url:
            return

        tab = self.tabs[url]
        self.driver.switch_to.window(tab)
        self.current_tab = tab
        self.current_url = url

    # async ??
    async def get_source(self, url):
        if url not in self.tabs.keys():
            await self.new_tab(url)

        with await self.lock:
            self.switch_to_tab(url)
            source = self.driver.page_source

        return source

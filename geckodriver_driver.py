from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

class GeckodriverDriver:

    def __init__(self):
        gecko_path = '/home/ignobillium/dev/geckodriver'
        self.driver = webdriver.Firefox(executable_path=gecko_path)
        self.driver.get("http://www.python.org")
        assert "Python" in self.driver.title

        self.current_url = 'http://www.python.org'
        self.current_tab = self.driver.window_handles

        self.tabs = {}
        self.tabs[self.current_url] = self.current_tab

    def new_tab(self, url):
        # switch to last tab
        self.driver.switch_to.window(self.driver.window_handles[-1])
        self.driver.execute_script('window.open("%s");' % url)
        self.driver.switch_to.window(self.driver.window_handles[-1])

        self.current_url = url
        self.current_tab = self.driver.current_window_handle
        self.tabs[url] = self.current_tab

    def switch_to_tab(self, url):
        # добавит обработку исключений
        tab = self.tabs[url]
        self.driver.switch_to.window(tab)
        self.current_tab = tab
        self.current_url = url

    def get_source(self):
        return self.driver.page_source

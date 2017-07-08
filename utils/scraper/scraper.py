import os
import sys
import csv
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import Select

LOGS_ENABLED = False

class Scraper:
    def __init__(self):
        self.sleep_time = 3
        self.url = 'https://www.ucrdatatool.gov/Search/Crime/Local/RunCrimeOneYearofData.cfm'
        self.webpage_title = 'Uniform Crime Reporting Statistics'
        self.MAX_STATE_ID = 52
        self.out_directory = 'fbi-crime-data/'
        if not os.path.exists(self.out_directory):
            os.makedirs(self.out_directory)
        with open('headers') as handle:
            self.headers = handle.readlines()

    def openBrowserWindow(self):
        self.driver = webdriver.Chrome()

        self.driver.get(self.url)
        assert self.webpage_title in self.driver.title

    def closeBrowserWindow(self):
        self.driver.quit()

    def navigate_state_page(self, state_id):
        state = self.driver.find_element_by_xpath("//select[@id='state']/option[@value='{0}']".format(state_id))
        state.click()
        state_name = state.text
        file_path = self.out_directory + state_name
        if LOGS_ENABLED:
            print("Value for {0} is {1}".format(state_name, state_id))

        next_page_button = self.driver.find_element_by_name("NextPage")
        self.removePopUpAdd()
        next_page_button.click()

        return file_path

    def navigate_agency_page(self):
        # Selecting all agencies
        agencies_select_element = Select(self.driver.find_element_by_id("agencies"))
        for option in agencies_select_element.options:
            agencies_select_element.select_by_visible_text(option.text)

        # Selecting all crime groups
        crime_group_select_element = Select(self.driver.find_element_by_id("groups"))
        for option in crime_group_select_element.options:
            crime_group_select_element.select_by_visible_text(option.text)
            self.removePopUpAdd()

        #Selecting year 2013
        year_select = self.driver.find_element_by_xpath("//select[@id='year']/option[@value='2013']")
        year_select.click()

        self.removePopUpAdd()

        next_page_button = self.driver.find_element_by_name("NextPage")
        next_page_button.click()

    def scrape(self, file_path):
        data_table = self.driver.find_element_by_xpath("//table[@title]").get_attribute('innerHTML')
        html_doc = "<table>" + data_table + "</table>"
        self.writeCSV(file_path, html_doc)

    def removePopUpAdd(self):
        if self.driver.find_elements_by_class_name('__acs'):
            self.driver.execute_script("var elements = document.getElementsByClassName('__acs'); elements[0].parentNode.removeChild(elements[0]);")

    def run(self):
        for state_id in range(1,self.MAX_STATE_ID):
            self.openBrowserWindow()
            file_path = self.navigate_state_page(str(state_id))
            self.navigate_agency_page()
            self.scrape(file_path)
            self.closeBrowserWindow()

    def writeCSV(self, file_path, html_doc):
        soup = BeautifulSoup(html_doc, 'html.parser')

        with open(file_path+'.csv','w+') as handle:
            output = csv.writer(handle)
            output.writerow([header.strip() for header in self.headers])

            for table in soup.find_all('table'):
                for row in table.find_all('tr')[4:]:
                    col = map(lambda cell: " ".join(cell.stripped_strings), row.find_all('td'))
                    output.writerow(col)
                output.writerow([])

if __name__ == "__main__":
    Scraper().run()

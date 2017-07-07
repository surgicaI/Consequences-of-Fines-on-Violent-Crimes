from selenium import webdriver
from selenium.webdriver.support.ui import Select

LOGS_ENABLED = False

class Scraper:
    def __init__(self):
        self.url = 'https://www.ucrdatatool.gov/Search/Crime/Local/RunCrimeOneYearofData.cfm'
        self.webpage_title = 'Uniform Crime Reporting Statistics'
        self.MAX_STATE_ID = 52

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
        file_path = 'raw-data/' + state_name
        if LOGS_ENABLED:
            print("Value for {0} is {1}".format(state_name, state_id))

        next_page_button = self.driver.find_element_by_name("NextPage")
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

        #Selecting year 2013
        year_select = self.driver.find_element_by_xpath("//select[@id='year']/option[@value='2013']")
        year_select.click()

        next_page_button = self.driver.find_element_by_name("NextPage")
        next_page_button.click()

    def scrape(self, file_path):
        data_table = self.driver.find_element_by_xpath("//table[@title]").get_attribute('innerHTML')
        with open(file_path,'w') as handle:
            handle.write(data_table)

    def run(self):
        for state_id in range(1,self.MAX_STATE_ID):
            self.openBrowserWindow()
            file_path = self.navigate_state_page(str(state_id))
            self.navigate_agency_page()
            self.scrape(file_path)
            self.closeBrowserWindow()


if __name__ == "__main__":
    Scraper().run()

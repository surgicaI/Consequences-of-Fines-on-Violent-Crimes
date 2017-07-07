from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select

LOGS_ENABLED = False
DEBUG = False

def init():
    pass

def navigate():
    pass

def scrape():
    pass

def main():
    url = 'https://www.ucrdatatool.gov/Search/Crime/Local/RunCrimeOneYearofData.cfm'
    webpage_title = 'Uniform Crime Reporting Statistics'
    driver = webdriver.Chrome()

    driver.get(url)
    assert webpage_title in driver.title

    state_select_element = driver.find_element_by_id("state")
    all_options = state_select_element.find_elements_by_tag_name("option")
    for option in all_options:
        if LOGS_ENABLED:
            print("Value for {0} is {1}".format(option.text, option.get_attribute("value")))
            
        state_name = option.text
        file_path = 'raw-data/' + state_name

        option.click()
        next_page_button = driver.find_element_by_name("NextPage")
        next_page_button.click()

        # ----- Select agencies page -----
        # Selecting all agencies
        agencies_select_element = Select(driver.find_element_by_id("agencies"))
        for option in agencies_select_element.options:
            agencies_select_element.select_by_visible_text(option.text)

        # Selecting all crime groups
        crime_group_select_element = Select(driver.find_element_by_id("groups"))
        for option in crime_group_select_element.options:
            crime_group_select_element.select_by_visible_text(option.text)

        #Selecting year 2013
        year_select = driver.find_element_by_xpath("//select[@id='year']/option[@value='2013']")
        year_select.click()

        next_page_button = driver.find_element_by_name("NextPage")
        next_page_button.click()

        # Reading data table
        data_table = driver.find_element_by_xpath("//table[@title]").get_attribute('innerHTML')
        with open(file_path,'w') as handle:
            handle.write(data_table)

        input('wait')


if __name__ == "__main__":
    init()
    navigate()
    scrape()
    main()

from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

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
    action_key_down_command = ActionChains(driver).key_down(Keys.COMMAND)
    action_key_up_command = ActionChains(driver).key_up(Keys.COMMAND)

    state_select_element = driver.find_element_by_id("state")
    all_options = state_select_element.find_elements_by_tag_name("option")
    for option in all_options:
        if LOGS_ENABLED:
            print("Value for {0} is {1}".format(option.text, option.get_attribute("value")))
        option.click()
        next_page_button = driver.find_element_by_name("NextPage")
        next_page_button.click()

        # ----- Select agencies page -----
        # Selecting all agencies
        agencies_select_element = driver.find_element_by_id("agencies")
        all_agencies = agencies_select_element.find_elements_by_tag_name("option")
        action_key_down_command.perform()
        for agency in all_agencies:
            if LOGS_ENABLED:
                print("Value for {0} is {1}".format(agency.text, agency.get_attribute("value")))
            agency.click()
        action_key_up_command.perform()

        # Selecting all crime groups
        crime_group_select_element = driver.find_element_by_id("groups")
        all_crime_groups = crime_group_select_element.find_elements_by_tag_name("option")
        action_key_down_command.perform()
        for crime_group in all_crime_groups:
            if LOGS_ENABLED:
                print("Value for {0} is {1}".format(crime_group.text, crime_group.get_attribute("value")))
            crime_group.click()
        action_key_down_command.perform()

        next_page_button = driver.find_element_by_name("NextPage")
        next_page_button.click()
        input('wait')


if __name__ == "__main__":
    init()
    navigate()
    scrape()
    main()

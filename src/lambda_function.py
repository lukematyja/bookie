from webdriver_wrapper import WebDriverWrapper
import schedule, es_data, os_data

week_id, week_data = schedule.get_week_urls()

def lambda_handler(*args, **kwargs):
    driver = WebDriverWrapper()
    driver.get_url(week_data['es_url'])
    driver.get_soup()
    es_data.ProcessGamesToS3(driver.soup, week_id)
    driver.close()

    driver = WebDriverWrapper()
    driver.get_url(week_data['os_url'])
    driver.get_soup()
    os_data.ProcessGamesToS3(driver.soup)
    driver.close()

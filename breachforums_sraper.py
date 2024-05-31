import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from fake_useragent import UserAgent
import datetime
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def scrape_forum(url, num_pages, filename_prefix):
    cookies = {
    }

    all_threads = []

    for page in range(1, num_pages + 1):
        url_page = url.format(page)
        while True:
            ua = UserAgent()
            user_agent = ua.random
            headers = {'User-Agent': user_agent}

            response = requests.get(url_page, headers=headers, cookies=cookies)
            logger.info(f"Scraping page {page} - Status code: {response.status_code}")

            if response.status_code == 200:
                break
            elif response.status_code in [429, 503, 403]:
                logger.info(f"Received status code {response.status_code}. Pausing for 2 seconds.")
                logger.info(f"Retrying with a new UserAgent.")
                time.sleep(2)
            else:
                logger.info(f"Failed to retrieve data from {url_page}. Retrying with a new UserAgent.")
                time.sleep(2)

        soup = BeautifulSoup(response.text, 'html.parser')

        marker = soup.find('td', class_='tcat', string=lambda text: text and 'Normal Threads' in text)
        if marker:
            parent_row = marker.parent
            thread_rows = parent_row.find_next_siblings('tr', class_='inline_row')
            for row in thread_rows:
                subject_tag = row.find('span', class_='subject_new') or row.find('span', class_='subject_old')
                if subject_tag:
                    thread_title = subject_tag.get_text(strip=True)
                    thread_link = "https://breachforums.st/" + subject_tag.find('a')['href']

                author_tag = row.find('span', class_='author smalltext')
                author = author_tag.get_text(strip=True).replace(',', '').replace('by', '')

                if row.find_all('td', align="center", class_='trow2 forumdisplay_regular'):
                    for views_element in row.find_all('td', align="center", class_='trow2 forumdisplay_regular'):
                        view_count = views_element.get_text(strip=True)
                else:
                    for views_element in row.find_all('td', align="center", class_='trow1 forumdisplay_regular'):
                        view_count = views_element.get_text(strip=True)

                all_threads.append({'Threads': thread_title, 'Author': author, 'Link': thread_link, 'Views': view_count})
        else:
            logger.info(f"'Normal Threads' marker not found on page {page}.")

        time.sleep(1)

    df = pd.DataFrame(all_threads)

    current_datetime = datetime.datetime.now().strftime("%Y%m%d-%H%M")
    filename = f"{filename_prefix}_{current_datetime}.csv"
    df.to_csv(filename, index=False)
    logger.info(f"Data saved to {filename}")
    logger.info("Scraping completed.")

# Scraping Databases
base_url_db = 'https://breachforums.st/Forum-Databases?page={}&sortby=started'
num_pages_db = 5
scrape_forum(base_url_db, num_pages_db, 'scraped_breachforums_db')

# Scraping Other Leaks
base_url_leaks = 'https://breachforums.st/Forum-Other-Leaks?page={}&sortby=started'
num_pages_leaks = 5
scrape_forum(base_url_leaks, num_pages_leaks, 'scraped_breachforums_leaks')

# Scraping Official Databases
base_url_off_db = 'https://breachforums.st/Forum-Official?page={}&sortby=started'
num_pages_db = 5
scrape_forum(base_url_off_db, num_pages_db, 'scraped_breachforums_off_db')

# Scraping Marketplace
base_url_off_db = 'https://breachforums.st/Forum-Sellers-Place?page={}&sortby=started'
num_pages_db = 5
scrape_forum(base_url_off_db, num_pages_db, 'scraped_breachforums_market')

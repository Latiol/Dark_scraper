import pandas as pd
import asyncio
import logging
from bs4 import BeautifulSoup
from requests_tor import RequestsTor
import re
import datetime

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def fetch(url):
    rt = RequestsTor(tor_ports=(9050,), tor_cport=9051)
    response = rt.get(url)
    return response


async def scrape_website_data(url):
    try:
        response = await fetch(url)
        if response and response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            website_names = [element.get_text(
                strip=True) for element in soup.find_all(class_='post-title')]
            published = []
            for element in soup.find_all(class_='post-title-block'):
                timer_end_element = element.find(class_='post-timer-end')
                if timer_end_element and 'd-none' not in timer_end_element.get('class', []):
                    published.append('yes')
                else:
                    published.append('no')
            updated_dates = []
            updated_time = []
            for element in soup.find_all(class_='updated-post-date'):
                date_str = element.get_text(strip=True)
                match = re.search(
                    r'Updated: (\d{2} \w+, \d{4},\s+\d{2}:\d{2}) UTC', date_str)
                if match:
                    date_time = match.group(1)
                    date_time = date_time.replace(',', '')
                    updated_dates.append((' '.join(date_time.split(' ')[:-1])))
                    updated_time.append(date_time.split(' ')[-1].strip())
                else:
                    updated_dates.append("N/A")
            views_counts = []
            for element in soup.find_all(class_='views'):
                span_elements = element.find_all('span')
                views_counts.append(span_elements[1].get_text(strip=True))
            if not website_names or not updated_dates or not views_counts:
                logger.warning(f"No data found on {url}")
                return None
            else:
                data = {'Website Name': website_names,
                        'Published': published,
                        'Updated Date': updated_dates,
                        'Updated Time UTC': updated_time,
                        'Views Count': views_counts}
                return pd.DataFrame(data)
        else:
            logger.error(
                f"Error scraping website: {url}. Status code {response.status_code if response else 'Unknown'}")
            return None
    except Exception as e:
        logger.error(f"Error scraping website: {url}. {e}")
        return None


async def scrape_multiple_websites(sites):
    scraped_data = []
    for name, url in sites:
        data = await scrape_website_data(url)
        if data is not None:
            scraped_data.append(data)
            break
    return scraped_data


def save_to_csv(df, filename_prefix):
    current_datetime = datetime.datetime.now().strftime("%Y%m%d-%H%M")
    filename = f"{filename_prefix}_{current_datetime}.csv"
    df.to_csv(filename, index=False)
    logger.info(f"Data saved to {filename}")


async def main():
    sites = [
        ("Lockbit 3.0",
         "http://lockbit3g3ohd3katajf6zaehxz4h4cnhmz5t735zpltywhwpc6oy3id.onion/"),
        ("Lockbit 3.0 Mirror1",
         "http://lockbit3753ekiocyo5epmpy6klmejchjtzddoekjlnt6mu3qh4de2id.onion/"),
        ("Lockbit 3.0 Mirror2",
         "http://lockbit7ouvrsdgtojeoj5hvu6bljqtghitekwpdy3b6y62ixtsu5jqd.onion/")
    ]
    scraped_data = await scrape_multiple_websites(sites)
    if scraped_data:
        df = pd.concat(scraped_data, ignore_index=True)
        save_to_csv(df, 'scraped_lockbit')
        logger.info("Scraping completed.")
    else:
        logger.warning("No data scraped.")

if __name__ == '__main__':
    asyncio.run(main())

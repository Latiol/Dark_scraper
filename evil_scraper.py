import asyncio
import logging
import re
from bs4 import BeautifulSoup
from requests_tor import RequestsTor

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def fetch(url):
    rt = RequestsTor(tor_ports=(9050,), tor_cport=9051)
    response = rt.get(url)
    return response


async def scraper(name, url, keywords):
    logger.info(f"\n----- Scraping {name} -----")
    try:
        response = await fetch(url)
        if response and response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            text_content = soup.get_text().lower()
            logger.debug(f"Scraped content from {name}: {text_content}")

            keyword_counts = {}
            for keyword in keywords:
                if keyword.lower().endswith('.fr'):
                    pattern = r'\b([\w-]+\.fr)\b'
                    matches = re.findall(pattern, text_content)
                    for match in matches:
                        logger.info(
                            f"Keyword '{match}' found on {name}: {url}")
                else:
                    count = text_content.count(keyword.lower())
                    keyword_counts[keyword] = count

            for keyword, count in keyword_counts.items():
                if count > 0:
                    logger.info(
                        f"Keyword '{keyword}' found {count} times on {name}: {url}")
        else:
            logger.error(
                f"Error scraping {name}: Status code {response.status_code if response else 'Unknown'}")
    except Exception as e:
        logger.error(f"Error scraping {name}: {e}")


async def main():
    sites = [
        ("Lockbit 3.0 blog",
         "http://lockbit3g3ohd3katajf6zaehxz4h4cnhmz5t735zpltywhwpc6oy3id.onion/"),
        ("Lockbit 3.0 fileserver",
         "http://lockbit7z2jwcskxpbokpemdxmltipntwlkmidcll2qirbu7ykg46eyd.onion/"),
        ("RansomEXX", "http://rnsm777cdsjrsdlbs4v5qoeppu3px6sb2igmh53jzrx7ipcrbjz5b2ad.onion/"),
        ("Everest", "http://ransomocmou6mnbquqz44ewosbkjk3o5qjsl3orawojexfook2j7esad.onion/"),
        ("Black Basta", "http://stniiomyjliimcgkvdszvgen3eaaoz55hreqqx6o77yvmpwt7gklffqd.onion/"),
        ("Cl0p", "http://santat7kpllt6iyvqbr7q4amdv6dzrh6paatvyrzl7ry3zm72zigf4ad.onion/"),
        ("Cl0p torrent", "http://toznnag5o3ambca56s2yacteu7q7x2avrfherzmz4nmujrjuib4iusad.onion/"),
        ("Lorenz", "http://lorenzmlwpzgxq736jzseuterytjueszsvznuibanxomlpkyxk6ksoyd.onion/"),
        # ("", ""),
    ]
    keywords = ['password', 'leak', 'leaked', 'breach',
                'breached', 'exploit', 'french', 'france', '.fr']

    tasks = [scraper(name, url, keywords) for name, url in sites]
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())

import os
import asyncio
import traceback
from async_timeout import timeout
import pyppeteer
import requests

from pyppeteer import launch
from bs4 import BeautifulSoup

"""

        > utliliser pypeteer
        > mettre le request header

        https://stackoverflow.com/questions/63440994/requests-html-render-returns-access-denied


        > faire classe Scrapper abstraite, et implémenter les meme fonctions
            dans chaque sous classe

    """


EXECUTABLE_PATH = "/usr/bin/chromium-browser"

request_header = {
    "GET": '/rest/document/9683325/references HTTP/1.1',
    "Host": 'ieeexplore.ieee.org',
    "Connection": 'keep-alive',
    "Pragma": 'no-cache',
    "Cache-Control": 'no-cache',
    "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="98"',
    "Accept": 'application/json, text/plain, */*',
    "cache-http-response": True,
    "sec-ch-ua-mobile": '?0',
    "User-Agent": 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    "sec-ch-ua-platform": "Linux",
    "Sec-Fetch-Site": 'same-origin',
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": "https://ieeexplore.ieee.org/document/9683325/references",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6,es-419;q=0.5"
}


class IEEE_scrapper():
    def __init__(self, path, request_header, timeout=5):
        self.path = path
        self.request_header = request_header
        self.doi_org_url = "https://doi.org/api/handles/"
        self.timeout = timeout

    async def get_page(self, doi, browser):
        page = await browser.newPage()

        doi_resolved_url = self.doi_org_url + doi
        try:
            response = requests.get(
                doi_resolved_url, timeout=self.timeout
            ).json()

        except requests.exceptions.Timeout:
            print("doi timeout")
            return

        if response["responseCode"] == 1:
            url = response["values"][0]["data"]["value"]
            url = os.path.join(url, "references")
        else:
            return 0

        try:
            await page.goto(url, {'waitUntil': 'load'}, header=self.request_header)
        except pyppeteer.errors.TimeoutError:
            return
        except:
            print("err")
            traceback.print_exc()
        else:
            return await page.content()
        finally:
            await page.close()

    async def _scrapp_page(self, doi):
        """
            Manque l'envoi des données vers le dataset

        """
        browser = await launch(
            headless=True, args=['--no-sandbox'],
            executablePath=self.path
        )
        html_page = await self.get_page(doi, browser)
        await browser.close()

        if html_page is None:
            return None

        soup = BeautifulSoup(html_page, "lxml")

        ref_list = soup.find_all("div", class_="reference-container")
        if len(ref_list) > 0:
            ref_text_list = []
            for ref in ref_list:
                ref_span = ref.find("span", class_=lambda x: x != "number")
                ref_text = ref_span.text
                ref_text_list.append(ref_text)

            return ref_text_list
        else:
            print('scrap failed')
            return None

    async def _close(self):
        return

    def close(self):
        asyncio.run(self._close())

    def scrapp_page(self, url):
        asyncio.run(self._scrapp_page(url))


if __name__ == "__main__":
    s = IEEE_scrapper(
        EXECUTABLE_PATH, request_header
    )
    #url = "https://ieeexplore.ieee.org/document/7780459"
    doi = "10.23919/eusipco.2017.8081399"
    s.scrapp_page(doi)

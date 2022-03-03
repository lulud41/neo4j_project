from http.client import responses
import os
import asyncio
import traceback
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
    def __init__(self, path, request):
        self.path = path
        self.request = request
        self.doi_org_url = "https://doi.org/api/handles/"

    """def __await__(self):
        return self.async_init().__await__()

    async def async_init(self):
        self.browser = await launch(
            headless=True, args=['--no-sandbox'],
            executablePath=self.path
        )
        return self"""

    async def get_page(self, doi):
        self.page = await self.browser.newPage()

        doi_resolved_url = self.doi_org_url + doi
        response = requests.get(doi_resolved_url).json()
        if response["responseCode"] == 1:
            url = response["values"][0]["data"]["value"]
            url = os.path.join(url, "references")

            print(doi, " -> ", url)
        else:
            return 0

        try:
            await self.page.goto(url, {'waitUntil': 'load'}, header=self.request)
        except:
            print("err")
            traceback.print_exc()
        else:
            print("ok")
            return await self.page.content()
        finally:
            print("close page")
            await self.page.close()

    async def _scrapp_page(self, doi):
        """
            Manque l'envoi des données vers le dataset

        """
        self.browser = await launch(
            headless=True, args=['--no-sandbox'],
            executablePath=self.path
        )
        html_page = await self.get_page(doi)
        await self.browser.close()
        soup = BeautifulSoup(html_page, "lxml")

        ref_list = soup.find_all("div", class_="reference-container")

        if len(ref_list) > 0:
            print("got scrapping results")
            for ref in ref_list:
                ref_span = ref.find("span", class_=lambda x: x != "number")
                ref_text = ref_span.text
                print(ref_text)

            return ref_list

    async def _close(self):
        await self.browser.close()

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

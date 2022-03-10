import os
import asyncio
import traceback
import requests

import pyppeteer
from bs4 import BeautifulSoup


EXECUTABLE_PATH = "/usr/bin/chromium-browser"

# request header afin de charcher le site IEEE (qui est basé sur JS donc pas de get normal possible)
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
    """classe implémentant un scrapper du site de l'éditeur IEEE.
       Le scrapping consiste à récupérer la liste des références d'un papier, à partir de son DOI.

       Le site IEEE utilisant JS, la page doit être chargée à partir d'un navigateur headless (usage de pyppeteer)
    """

    def __init__(self, path, request_header, timeout=5):
        """
        Args:
            path (str): chemin du navigateur
            request_header (str): header pour les requetes GET
            timeout (int, optional): timeout avant abandon de requete. Defaults to 5.
        """
        self.path = path
        self.request_header = request_header
        self.doi_org_url = "https://doi.org/api/handles/"
        self.timeout = timeout

    async def get_page(self, doi, browser):
        """Traduit le DOI d'un papier en lien vers le site de l'éditeur, via le site doi.org
            La page est ensuite chargé dans le navigateur

        Args:
            doi (_type_): _description_
            browser (_type_): _description_

        Returns:
            str: contenu de la page
        """
        page = await browser.newPage()

        doi_resolved_url = self.doi_org_url + doi
        try:
            response = requests.get(
                doi_resolved_url, timeout=self.timeout
            ).json()

        except requests.exceptions.Timeout:
            print("doi timeout")
            return
        except:
            print("doi unkown error")
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
        """scrapping des références du papier

        Args:
            doi (str): DOI du papier

        Returns:
            list[str]: liste des titre des références, affichées sur la page du papier, sur le site IEEE
        """
        browser = await pyppeteer.launch(
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

    def scrapp_page(self, url):
        asyncio.run(self._scrapp_page(url))


if __name__ == "__main__":
    s = IEEE_scrapper(
        EXECUTABLE_PATH, request_header
    )
    doi = "10.23919/eusipco.2017.8081399"
    s.scrapp_page(doi)

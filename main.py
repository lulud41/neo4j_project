import asyncio
import time
import re
import json
from difflib import SequenceMatcher
import aiohttp
import copy

import paperswithcode

import ieee_scrapper


# Probleme numéro de ref
# voir si probleme blocage avec le parser
# limitter le nombre de ref
# rapport à conserver : 1 : 15 entre unm paper et ref


# https://tenthousandmeters.com/blog/python-behind-the-scenes-12-how-asyncawait-works-in-python/

"""
    if paper.arxiv_id is not None:
            query = ARXIV_API+paper.arxiv_id
            xml = requests.get(query).text
            data = feedparser.parser(xml)["entries"][0]
            paper["domaine_arxiv"] = data["arxiv_primary_category"]["term"]
            paper["langue"] = data["summary_detail"]["language"]

    > A faire
        : file de requettes  | priority queue ?
            : une requete
                {"type":titre | doi, "content": , "priorité":}

        : prendre en compte la priorité
            : dabord noeuds de PWC,
                puis en fonction du nombre de sauts

        : faire classe doi converter pour les references
            : dabord doi > url, puis get l'url comme ieee

    > bien intégrer le scrapper et le reste en async
"""

"""
    > coroutines sur un chunk entier


    : lock sur l'accès au dataset

    > coroutines
        : tout le chunk
            : puis rempli la queue avec les requettes
                {"doi", "query"}

    : itère block par  block (bloquant , sinon avec event ? )
        : itère papier
            : si reference list
                : itère et add doi
                : ou itère et cherche title -> doi
            : si publisher
                : scrapp
                    : requette + parse

    > start acquis
        : liste de coroutine
            : une coroutine = un papier doi de pwc



"""


class PaperGraphCreator():
    def __init__(self, arxiv_api, cross_ref_api, mail_to, known_publisher_list,
                 max_item=50, get_time_out=5, scrapp=True,):

        self.pwc_client = paperswithcode.PapersWithCodeClient()
        self.arxiv_api = arxiv_api
        self.cross_ref_api = cross_ref_api
        self.mail_to = mail_to
        self.max_item = max_item
        self.get_time_out = get_time_out
        self.known_publisher_list = known_publisher_list
        self.new_publishers_list = set()

        self.scrapp = scrapp
        if scrapp is True:
            self.scrappers = {
                "IEEE": ieee_scrapper.IEEE_scrapper(
                    ieee_scrapper.EXECUTABLE_PATH,
                    ieee_scrapper.request_header
                ),
                "Springer": None,
                "research_gate": None
            }

        self.paper_template_dict = {
            "url_doi": None,
            "titre": None,
            "authors": [],
            "date": str(None),
            "langue": None,
            "domaine_arxiv": None,
            "task": None,
            "num_citations": None,
            "conference": {"nom": None, "lieu": None, "date": None},
            "publisher": None,
            "key_words": None,
            "reference": []
        }

        self.dataset = {}
        self.num_papers = 0
        self.num_reference = 0

    def clean_title_string(self, title):
        t = title.lower()
        t = re.sub(r'[ \t]+', ' ', t)
        t = re.sub(r'[\\^(){}<>$=\n]', ' ', t)

        return t

    async def retrieve_paper_from_title(self, title, aiohttp_session):

        query = f"{CROSS_REF_API}/works?query.bibliographic={title}" +\
            f"&select=DOI,title&rows=1" +\
            f"&mailto={MAIL_TO}"

        try:
            result_ref = await aiohttp_session.request(method="GET", url=query)
        except asyncio.exceptions.TimeoutError:
            print("time")
            return None

        if result_ref.ok is False:
            return None

        result_ref = await result_ref.json()

        if result_ref["status"] != "ok" or \
                len(result_ref["message"]["items"]) == 0:
            return None

        result_ref_content = result_ref["message"]["items"][0]

        if "title" not in result_ref_content.keys() or \
                "DOI" not in result_ref_content.keys():

            return None

        return result_ref_content

    async def _parse_reference_list(self, ref_list, doi,
                                    aiohttp_session, dataset_lock, async_queue):

        doi_ref_list = []
        for ref in ref_list[:MAX_REFENCES]:
            if "DOI" in ref.keys():
                ref_doi = ref["DOI"]

                """ #TODO ajout à la liste de requettes"""
                doi_ref_list.append(ref_doi)
                self.num_reference += 1

            elif "unstructured" in ref.keys():
                unstruct_title = ref["unstructured"]

                result_ref_content = await self.retrieve_paper_from_title(
                    unstruct_title, aiohttp_session
                )

                if result_ref_content is None:
                    continue

                if SequenceMatcher(None, result_ref_content["title"], unstruct_title).ratio() > 0.75:
                    doi_ref_list.append(result_ref_content["DOI"])

                    self.num_reference += 1
            '''
                champ
                    ...["reference"][i]["article-title"]
                                        volume-title
            '''
        await dataset_lock.acquire()
        self.dataset[doi]["refence"] = doi_ref_list
        dataset_lock.release()
        print("- got : ", doi, " with ",
              len(doi_ref_list), " references")

    async def _scrapp_from_publisher_name(self, publisher_name, paper_doi,
                                          aiohttp_session, dataset_lock):

        if publisher_name in self.known_publisher_list:
            print("    (scrapper) peut scrapper publisher", publisher_name)

            if publisher_name.strip() == "IEEE" or\
                    publisher_name.strip() == "Institute of Electrical and Electronics Engineers (IEEE)":

                ref_list = await self.scrappers["IEEE"]._scrapp_page(paper_doi)
                if ref_list is None:
                    return None

                doi_ref_list = []

                for i, reference_name in enumerate(ref_list):
                    """
                        si recherche trop vague :
                            : le titre est soit:  entre quotes, sinon en italique

                            > TODO: implem aussi les titres en italique si pas de quotes
                    """
                    reference_name = re.findall(r'"(.+)"', reference_name)
                    if len(reference_name) == 0:
                        continue

                    result = await self.retrieve_paper_from_title(
                        reference_name[0], aiohttp_session
                    )

                    if result is None:
                        continue

                    matched = self._check_title_match(
                        result["title"][0], reference_name[0], 0.75)

                    if matched is True:
                        doi_ref_list.append(result["DOI"])
                        self.num_reference += 1

                print("(scrapper) - got : ", paper_doi, " with ",
                      len(doi_ref_list), " references")

                await dataset_lock.acquire()
                self.dataset[paper_doi]["reference"] = doi_ref_list
                dataset_lock.release()

        else:
            self.new_publishers_list.add(publisher_name)
            print("--> new publisher_name ", publisher_name)

    async def _parse_author_list(self, author_list, paper_doi, dataset_lock):
        author_list = []
        for author in author_list:
            author_list.append(
                {
                    "name": author["given"]+" "+author["family"],
                    "affiliation": author["affiliation"]
                }
            )

        await dataset_lock.acquire()
        self.dataset[paper_doi]["authors"] = author_list
        dataset_lock.release()

    def _check_title_match(self, title_1, title_2, thresh):
        t1 = self.clean_title_string(title_1)
        t2 = self.clean_title_string(title_2)
        score = SequenceMatcher(None, t1, t2).ratio()

        if score >= thresh:
            return True
        else:
            return False

    async def collect_paper(self, paper_info, aiohttp_session, dataset_lock, async_queue):
        paper_dict = copy.deepcopy(self.paper_template_dict)

        query = f"{CROSS_REF_API}/works?" +\
            f"query.bibliographic={paper_info.title.replace('&', ' and ')}" +\
            f"&select=DOI,URL,title,subject,publisher,reference,author&rows=1" +\
            f"&mailto={MAIL_TO}"

        try:
            result = await aiohttp_session.request(
                method="GET", url=query
            )
        except asyncio.exceptions.TimeoutError:
            return None

        if result.ok is False:
            return

        result = await result.json()

        if result["status"] != "ok":
            return

        result = result["message"]["items"][0]
        matched = self._check_title_match(
            result["title"][0], paper_info.title, 0.9
        )

        if matched:
            paper_doi = result["DOI"]
            paper_dict["titre"] = result["title"][0]

            if "URL" in result.keys():
                paper_dict["url_doi"] = result["URL"]

            paper_dict["date"] = str(
                paper_info.published) if paper_info.published is not None else None

            paper_dict["conference"]["nom"] = paper_info.conference

            self.num_papers += 1

            await dataset_lock.acquire()
            if paper_doi not in self.dataset.keys():
                self.dataset[paper_doi] = paper_dict
                dataset_lock.release()

            if "reference" in result.keys():
                reference_list = result["reference"]

                await self._parse_reference_list(
                    reference_list, paper_doi, aiohttp_session,
                    dataset_lock, async_queue
                )

            elif "publisher" in result.keys():
                publisher = result["publisher"]

                if self.scrapp:
                    await self._scrapp_from_publisher_name(
                        publisher, paper_doi, aiohttp_session, dataset_lock
                    )

            elif "URL" in result.keys():
                print("peut etre scrapper ", result["URL"])

            if "author" in result.keys():
                await self._parse_author_list(
                    result["author"], paper_doi, dataset_lock
                )

    async def _start_acquisition(self, papers_chunk):

        timeout = aiohttp.ClientTimeout(total=self.get_time_out)
        async with aiohttp.ClientSession(timeout=timeout) as session:

            dataset_lock = asyncio.Lock()
            async_queue = asyncio.Queue()
            corountines_list = []

            for paper in papers_chunk.results:
                corountines_list.append(
                    self.collect_paper(
                        paper, session, dataset_lock, async_queue
                    )

                )
            await asyncio.gather(*corountines_list)
            await async_queue.join()

    def start(self):
        pwc_num_papers = self.pwc_client.paper_list(
            page=1, items_per_page=1).count

        for page_num in range(1001, pwc_num_papers // self.max_item):
            papers_chunk = self.pwc_client.paper_list(
                page=page_num, items_per_page=MAX_ITEM
            )
            print("\n___________ page num : ", page_num)

            asyncio.run(
                self._start_acquisition(papers_chunk)
            )

            print(
                f"\n___________ chunk completed : papers: {len(self.dataset)} ref:{self.num_reference}"
            )
            with open('dataset.json', "w") as f:
                f.write(json.dumps(self.dataset))


if __name__ == '__main__':

    ARXIV_API = "http://export.arxiv.org/api/query?id_list="
    CROSS_REF_API = "https://api.crossref.org"
    MAIL_TO = "lulud41@gmail.com"

    MAX_ITEM = 50
    GET_TIMEOUT = 20

    MAX_REFENCES = 70

    KNOWN_PUBLISHER_LIST = [
        "springer", "IEEE", "research_gate",
        "Institute of Electrical and Electronics Engineers (IEEE)"
    ]

    t1 = time.perf_counter()

    app = PaperGraphCreator(
        ARXIV_API, CROSS_REF_API, MAIL_TO, KNOWN_PUBLISHER_LIST,
        max_item=MAX_ITEM, get_time_out=GET_TIMEOUT, scrapp=True
    )
    app.start()
    print(time.perf_counter() - t1)

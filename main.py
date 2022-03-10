import asyncio
import time
import re
import json
from difflib import SequenceMatcher
import copy

import aiohttp
import traceback
import paperswithcode
import feedparser
import tea_client

import ieee_scrapper

"""
    @authors
        DEROUET Lucien
        PURIRER Anthone
    Programme principal pour la collecte des données

    lancer


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
        self.new_publishers_list = {}

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
            "title": None,
            "authors": [],
            "date": str(None),
            "arxiv_category": None,
            "conference": {
                "name": None, "place": None, "date": None
            },
            "language": None,
            "publisher": None,
            "key_words": [],
            "reference": []
        }

        self.dataset = {}
        self.num_papers = 0
        self.num_references = 0

    def clean_title_string(self, title):
        t = title.lower()
        t = re.sub(r'[ \t]+', ' ', t)
        t = re.sub(r'[\\^(){}<>$=\n]', ' ', t)

        return t

    async def async_get_request(self, query, aiohttp_session, is_json=True):
        try:
            result = await aiohttp_session.request(method="GET", url=query)
        except asyncio.exceptions.TimeoutError as e:
            print("timeout")
            return None
        except aiohttp.client_exceptions.ServerDisconnectedError as e:
            print("server disconnected")
            return None
        except aiohttp.client_exceptions.ClientOSError as e:
            print("reset by peer")
            return None
        except:
            print("unkown error :", traceback.print_exc())
            return None

        if result.ok is False:
            print("nok : ", await result.content.read(), "\n request ", query)
            return None

        if is_json is True:
            result = await result.json()

            if result["status"] != "ok":
                return None
            if "items" in result["message"].keys():
                if len(result["message"]["items"]) == 0:
                    return None
                if "title" not in result["message"]["items"][0].keys():
                    return None
                if len(result["message"]["items"][0]["title"]) == 0:
                    return None
                if "DOI" not in result["message"]["items"][0].keys():
                    return None
                result = result["message"]["items"][0]
            else:
                if "title" not in result["message"].keys():
                    return None
                if len(result["message"]["title"]) == 0:
                    return None
                result = result["message"]
        else:
            result = await result.text()

        return result

    async def retrieve_paper_from_title(self, title, aiohttp_session):

        query = f"{CROSS_REF_API}/works?" +\
            f"query.bibliographic={title.replace('&', ' and ')}" +\
            f"&select=DOI,URL,title,subject,publisher,reference,author,event&rows=1" +\
            f"&mailto={MAIL_TO}"

        result_ref = await self.async_get_request(query, aiohttp_session)

        return result_ref

    async def parse_paper_dict(self, paper, aiohttp_session, dataset_lock):
        doi = paper["DOI"]
        if doi in self.dataset.keys():
            return
        self.num_papers += 1

        paper_dict = self._parse_result_info(paper, None)

        await dataset_lock.acquire()
        self.dataset[doi] = paper_dict
        dataset_lock.release()

        await self.get_arxiv_info(
            paper["title"], aiohttp_session,
            dataset_lock, doi, by_title=True
        )

        ref_doi_list = []
        if "reference" in paper.keys():
            references_list = paper["reference"]
            for ref in references_list[:MAX_REFERENCES]:
                if "DOI" in ref.keys():
                    ref_doi_list.append(ref["DOI"])

            await dataset_lock.acquire()
            self.dataset[doi]["reference"] = ref_doi_list
            dataset_lock.release()

        self.num_references += len(ref_doi_list)
        print("- got ref paper : ", doi, " with  ",
              len(ref_doi_list), " references")

    async def _parse_references_list(self, ref_list, aiohttp_session, dataset_lock):

        doi_ref_list = []
        for ref in ref_list[:MAX_REFERENCES]:
            if "DOI" in ref.keys():
                ref_doi = ref["DOI"]
                doi_ref_list.append(ref_doi)
                await self.collect_paper_from_doi(ref_doi, aiohttp_session, dataset_lock)

            elif "unstructured" in ref.keys() or "article-title" in ref.keys():
                key = set(ref.keys()).intersection(
                    ["article-title", "unstructured"]
                ).pop()

                unstruct_title = ref[key]
                result_ref_content = await self.retrieve_paper_from_title(unstruct_title, aiohttp_session)

                if result_ref_content is None:
                    continue

                matched = self._check_title_match(
                    result_ref_content["title"][0], unstruct_title, 0.8)

                if matched:
                    doi_ref_list.append(result_ref_content["DOI"])
                    await self.parse_paper_dict(
                        result_ref_content, aiohttp_session, dataset_lock)

        return doi_ref_list

    async def _scrapp_from_publisher_name(self, publisher_name, paper_doi,
                                          aiohttp_session, dataset_lock):

        if publisher_name not in self.known_publisher_list:
            if publisher_name not in self.new_publishers_list.keys():
                self.new_publishers_list[publisher_name] = 1
            else:
                self.new_publishers_list[publisher_name] += 1

        if publisher_name.strip() == "IEEE" or\
                publisher_name.strip() == "Institute of Electrical and Electronics Engineers (IEEE)":

            ref_list = await self.scrappers["IEEE"]._scrapp_page(paper_doi)
            if ref_list is None:
                return None

            doi_ref_list = []

            for references_name in ref_list:
                references_name = re.findall(r'"(.+)"', references_name)
                if len(references_name) == 0:
                    continue

                result = await self.retrieve_paper_from_title(references_name[0], aiohttp_session)

                if result is None:
                    continue

                matched = self._check_title_match(
                    result["title"][0], references_name[0], 0.75)

                if matched is True:
                    doi_ref_list.append(result["DOI"])
                    await self.parse_paper_dict(
                        result, aiohttp_session, dataset_lock)

            print("(scrapper) - got : ", paper_doi, " with ",
                  len(doi_ref_list), " referencess")

            return doi_ref_list

    def _check_title_match(self, title_1, title_2, thresh):
        t1 = self.clean_title_string(title_1)
        t2 = self.clean_title_string(title_2)
        score = SequenceMatcher(None, t1, t2).ratio()

        if score >= thresh:
            return True
        else:
            return False

    def _parse_result_info(self, result_dict, paper_info):
        paper_dict = copy.deepcopy(self.paper_template_dict)

        paper_dict["title"] = result_dict["title"][0]

        if "URL" in result_dict.keys():
            paper_dict["url_doi"] = result_dict["URL"]

        if "subject" in result_dict.keys():
            paper_dict["key_words"] = [
                *paper_dict["key_words"], *result_dict["subject"]
            ]

        if paper_info is not None and paper_info.published is not None:
            paper_dict["date"] = str(paper_info.published)
        else:
            if "created" in result_dict.keys():
                paper_dict["date"] = result_dict["created"]["date-time"][:10]
            elif "deposited" in result_dict.keys():
                paper_dict["date"] = result_dict["deposited"]["date-time"][:10]

        if "event" in result_dict.keys() and "location" in result_dict.keys():
            paper_dict["conference"]["name"] = result_dict["event"]["name"]
            paper_dict["conference"]["place"] = result_dict["event"]["location"]

            if "start" in result_dict["event"].keys():
                date_list = result_dict["event"]["start"]["date-parts"][0]
                paper_dict["conference"]["date"] = f"{date_list[0]}-{date_list[1]}-{date_list[2]}"

        elif paper_info is not None:
            paper_dict["conference"]["name"] = paper_info.conference

        if "publisher" in result_dict.keys():
            paper_dict["publisher"] = result_dict["publisher"]

        if "author" in result_dict.keys():
            authors = []
            for author in result_dict["author"]:
                if "given" not in author.keys():
                    continue
                authors.append(
                    {
                        "name": author["given"]+" "+author["family"],
                        "organisation": author["affiliation"]
                    }
                )
            paper_dict["authors"] = authors

        return paper_dict

    async def get_arxiv_info(self, paper_info, aiohttp_session, dataset_lock, paper_doi, by_title=False):
        if by_title is False:
            query = f"{ARXIV_API}/query?id_list={paper_info.arxiv_id}"
            xml = await self.async_get_request(query, aiohttp_session, is_json=False)
            if xml is None:
                return
            data = feedparser.parse(xml)["entries"][0]

            await dataset_lock.acquire()
            self.dataset[paper_doi]["arxiv_category"] = data["arxiv_primary_category"]["term"]
            self.dataset[paper_doi]["language"] = data["summary_detail"]["language"]
            dataset_lock.release()
        else:
            query = f"{ARXIV_API}/query?search_query=ti:{paper_info}&max_results=1"
            xml = await self.async_get_request(query, aiohttp_session, is_json=False)
            if xml is None:
                return
            data = feedparser.parse(xml)["entries"]
            if len(data) == 0:
                return
            else:
                data = data[0]
            await dataset_lock.acquire()
            self.dataset[paper_doi]["arxiv_category"] = data["arxiv_primary_category"]["term"]
            self.dataset[paper_doi]["language"] = data["summary_detail"]["language"]
            dataset_lock.release()

    async def collect_paper_from_doi(self, paper_doi, aiohttp_session, dataset_lock):
        if paper_doi in self.dataset.keys():
            return

        query = f"{CROSS_REF_API}/works/{paper_doi}"
        result = await self.async_get_request(query, aiohttp_session)
        if result is None:
            return

        title = result["title"][0]

        query = f"{CROSS_REF_API}/works?" +\
            f"query.bibliographic={title.replace('&', ' and ')}" +\
            f"&select=DOI,URL,title,subject,publisher,reference,author,event&rows=1" +\
            f"&mailto={MAIL_TO}"

        result = await self.async_get_request(query, aiohttp_session)
        if result is None:
            return

        matched = self._check_title_match(result["title"][0], title, 0.9)
        if matched:
            await self.parse_paper_dict(result, aiohttp_session, dataset_lock)

    async def collect_paper(self, paper_info, aiohttp_session, dataset_lock):

        query = f"{CROSS_REF_API}/works?" +\
            f"query.bibliographic={paper_info.title.replace('&', ' and ')}" +\
            f"&select=DOI,URL,title,subject,publisher,reference,author,event&rows=1" +\
            f"&mailto={MAIL_TO}"

        result = await self.async_get_request(query, aiohttp_session)
        if result is None:
            return

        matched = self._check_title_match(
            result["title"][0], paper_info.title, 0.9)

        if matched:
            paper_doi = result["DOI"]

            if paper_doi not in self.dataset.keys():
                self.num_papers += 1
                paper_dict = self._parse_result_info(result, paper_info)

                await dataset_lock.acquire()
                self.dataset[paper_doi] = paper_dict
                dataset_lock.release()

            if paper_info.arxiv_id is not None:
                await self.get_arxiv_info(paper_info, aiohttp_session, dataset_lock, paper_doi)

            ref_list = None
            if "reference" in result.keys():
                references_list = result["reference"]

                ref_list = await self._parse_references_list(references_list, aiohttp_session, dataset_lock)

            elif "publisher" in result.keys() and self.scrapp:
                ref_list = await self._scrapp_from_publisher_name(
                    result["publisher"], paper_doi, aiohttp_session, dataset_lock
                )

            if ref_list is not None and len(ref_list) > 0:
                await dataset_lock.acquire()
                self.dataset[paper_doi]["reference"] = ref_list
                dataset_lock.release()

            l = len(ref_list) if ref_list is not None else 0
            print("- got pwc paper : ", paper_doi, " with ", l, " referencess")

    async def _start_acquisition(self, papers_chunk):

        timeout = aiohttp.ClientTimeout(total=self.get_time_out)

        connector = aiohttp.TCPConnector(limit=MAX_TCP_CONNECTIONS)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as aiohttp_session:

            dataset_lock = asyncio.Lock()
            corountines_list = []

            for paper in papers_chunk.results:
                corountines_list.append(
                    self.collect_paper(paper, aiohttp_session, dataset_lock)
                )
            await asyncio.gather(*corountines_list)

    def start(self):
        pwc_num_papers = self.pwc_client.paper_list(
            page=1, items_per_page=1).count

        for page_num in range(30, pwc_num_papers // self.max_item):
            try:
                papers_chunk = self.pwc_client.paper_list(
                    page=page_num, items_per_page=MAX_ITEM
                )
            except:
                print("failed to load chunk")
                continue
            print("\n___________ page num : ", page_num)
            self.time_start = time.perf_counter()

            asyncio.run(
                self._start_acquisition(papers_chunk)
            )

            print("\n__________ publisher_name \n", end=" ")
            for k in self.new_publishers_list.keys():
                print(k, self.new_publishers_list[k])
            print(
                f"\n___________ chunk completed : papers: {len(self.dataset)} ref:{self.num_references}"
            )

            with open('dataset.json', "bw") as f:
                f.write(
                    json.dumps(self.dataset, ensure_ascii=False).encode(
                        "utf-8")
                )
                print(
                    f"{self.num_papers} papers and {self.num_references} refs in {time.perf_counter() - self.time_start} sec")

        self.exit()

    def exit(self):
        with open('dataset.json', "bw") as f:
            f.write(
                json.dumps(self.dataset, ensure_ascii=False).encode("utf-8")
            )
            print(
                f"{self.num_papers} papers and {self.num_references} refs in {time.perf_counter() - self.time_start} sec")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.exit()


if __name__ == '__main__':

    ARXIV_API = "http://export.arxiv.org/api"
    CROSS_REF_API = "https://api.crossref.org"
    MAIL_TO = "lulud41@gmail.com"

    MAX_ITEM = 100
    MAX_TCP_CONNECTIONS = 40
    GET_TIMEOUT = 5
    MAX_REFERENCES = 50

    """
        
        max 50 :  241 pap, 3059 ref 98 sec, timeout 5    : 2.45 / sec
        max 200 : tcp 50, time 5 , 361 pap, 7700 ref, 157 secs  : 2.29 / sec


        max 20 : tcp 50, time 5,  : 0.6/sec
        max 400 : tcp 50, time 5,

    """
    KNOWN_PUBLISHER_LIST = [
        "IEEE", "Institute of Electrical and Electronics Engineers (IEEE)"
    ]

    app = PaperGraphCreator(
        ARXIV_API, CROSS_REF_API, MAIL_TO, KNOWN_PUBLISHER_LIST,
        max_item=MAX_ITEM, get_time_out=GET_TIMEOUT, scrapp=True)

    app.start()

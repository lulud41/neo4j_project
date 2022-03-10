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

import ieee_scrapper

"""

    Programme pour la collecte de données sur des publications scientifiques.
    Collecte de données sur Papers With Code (API), puis complète les données 
    avec l'API Crossref et l'api Arxiv. Si besoin, les références du papier
    sont scrappées sur le site IEEE.

    lancer le script ()
    
        / ! \  ________________________   / ! \   ________________________ / ! \ 
        
                            Testé uniquement sur linux et Python 3.8.10
                            
                            Usage de asyncio, donc python >= 3.5 obligatoire
    
        / ! \  ________________________   / ! \   ________________________ / ! \

            Pour le scrapping, le navigateur chromium est utilisé en mode headless, 
                donner le chemin du navigateur dans la variable IEEE_SCRAPPER_EXECUTABLE_PATH
                : si déjà installé, peut obtenir le chemin avec : $ whereis chromium-browser

              Pour désactiver le scrapping, mettre la variable 'scrapp' à False dans le constructeur
                de la classe PaperGraphCreator 

            

        $ pip install -r requirements.txt
        $ python3 main.py


"""


class PaperGraphCreator():
    def __init__(self, arxiv_api, cross_ref_api, mail_to, known_publisher_list,
                 max_item=50, get_timeout=5, scrapp=True, max_tcp_conn=50,
                 max_reference=50):
        """

        Args:
            arxiv_api (str): url de l'api arxiv

            cross_ref_api (str): url de l'api crossref

            mail_to (str): addresse mail de lucien derouet (pour bénéficier d'une 
                meilleure qualité de service pour l'api crossref)

            known_publisher_list (list[str]): liste des éditeurs connus, pour lesquels in scrapper est implémenté
                 actuellement seulement IEEE

            max_item (int, optional): Nombre papiers à récupérer par requête Arxiv. Defaults to 50.

            get_timeout (int, optional): timeout avant abandon de la requête. Defaults to 5.

            scrapp (bool, optional): True pour permettre le scrapping. Defaults to True.

            max_tcp_conn (int, optional): Nombre max de connections TCP permises simultanément. Defaults to 50.

            max_reference (int, optional): Nombre max de références prises en compte pour un papier. Defaults to 50.
        """

        self.pwc_client = paperswithcode.PapersWithCodeClient()
        self.arxiv_api = arxiv_api
        self.cross_ref_api = cross_ref_api
        self.mail_to = mail_to
        self.max_item = max_item
        self.get_timeout = get_timeout
        self.max_tcp_conn = max_tcp_conn
        self.max_reference = max_reference
        self.known_publisher_list = known_publisher_list
        self.new_publishers_list = {}

        self.scrapp = scrapp
        if scrapp is True:
            self.scrappers = {
                "IEEE": ieee_scrapper.IEEE_scrapper(
                    IEEE_SCRAPPER_EXECUTABLE_PATH,
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
        """lance une requête GET avec aiohttp

        Args:
            query (str): requête
            aiohttp_session : session aiohttp
            is_json (bool, optional): _description_. Defaults to True.

        Returns:
            : réponses de la requête, None si échec, ou un dictionnaire / xml si succès
        """
        try:
            result = await aiohttp_session.request(method="GET", url=query)
        except asyncio.exceptions.TimeoutError as e:
            # print("timeout")
            return None
        except aiohttp.client_exceptions.ServerDisconnectedError as e:
            #print("server disconnected")
            return None
        except aiohttp.client_exceptions.ClientOSError as e:
            #print("reset by peer")
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
        """Collecte les informations sur un papier à partir de son titre, ou chaîne de caractère

        Args:
            title (str): titre ou str décrivant le papier
            aiohttp_session (_type_): session

        Returns:
            _type_: None ou dictionnaire json 
        """

        query = f"{CROSS_REF_API}/works?" +\
            f"query.bibliographic={title.replace('&', ' and ')}" +\
            f"&select=DOI,URL,title,subject,publisher,reference,author,event,created,deposited&rows=1" +\
            f"&mailto={MAIL_TO}"

        result_ref = await self.async_get_request(query, aiohttp_session)

        return result_ref

    async def parse_paper_dict(self, paper, aiohttp_session, dataset_lock):
        """Parse les informations d'un papier, cherche la classification sur arxiv, et collecte 
            la liste des références

        Args:
            paper (dict): dict décrivant le papier
            aiohttp_session (_type_): session
            dataset_lock (asyncio.Lock): pour l'accès concurent au dataset
        """
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
            for ref in references_list[:self.max_reference]:
                if "DOI" in ref.keys():
                    ref_doi_list.append(ref["DOI"])

            await dataset_lock.acquire()
            self.dataset[doi]["reference"] = ref_doi_list
            dataset_lock.release()

        self.num_references += len(ref_doi_list)
        print("- got ref paper : ", doi, " with  ",
              len(ref_doi_list), " references")

    async def _parse_references_list(self, ref_list, aiohttp_session, dataset_lock):
        """Traite la liste des références d'un papier. La liste peut contenir des 
            DOI ou les titre / chaine de caractère non structurée


        Args:
            ref_list (list[str]): liste des références
            aiohttp_session (_type_):session
             dataset_lock (asyncio.Lock): pour l'accès concurent au dataset

        Returns:
            list[str]: la liste des DOIs de chaque référence
        """

        doi_ref_list = []
        for ref in ref_list[:self.max_reference]:
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
        """Si l'api crossref ne donne pas la liste des références d'un papier,
           la liste est obtenue en scrapant le site de l'éditeur, si un scraper
           est implémenté. Le seul disponible actuellement est IEEE

        Args:
            publisher_name (str): nom de l'éditeur du papier
            paper_doi (str): doi du papier
            aiohttp_session (_type_): session
            dataset_lock (asyncio.Lock): pour l'accès concurent au dataset

        Returns:
            list[str]: liste des DOIs de chaque référence
        """

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
        """l'API crossref est solicitée avec le titre d'un papier. Pour valider
        le résultat, il faut mesurer la similitude entre le titre demandé et le 
        titre du papier trouvé par l'api

        Args:
            title_1 (str): titre du papier demandé
            title_2 (str): titre du papier renvoyé par l'api
            thresh (float): seuil entre 0 et 1, mesurant la similitude des titres

        Returns:
            bool: vrai ou faux, si les titres sont suffisement similaires ou pas
        """
        t1 = self.clean_title_string(title_1)
        t2 = self.clean_title_string(title_2)
        score = SequenceMatcher(None, t1, t2).ratio()

        if score >= thresh:
            return True
        else:
            return False

    def _parse_result_info(self, result_dict, paper_info):
        """Parse les données d'un dictionnaire décrivant un papier
        et crée un dict à partir du template puis le rempli

        Args:
            result_dict (dict): dict obtenu à partir de crossref
            paper_info (~ namespace): objet venant de paper with code

        Returns:
            dict: Dict suivant le template, prêt à être intégré au dataset
                    mais les références sont absentes du dict
        """
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
        """Requête vers l'api arxiv, pour chercher la classification d'un papier

        Args:
            paper_info (dict): dict décriavant le papier
            aiohttp_session (_type_): session
            dataset_lock (asyncio.Lock): pour l'accès concurent au dataset
            paper_doi (str): DOI du papier
            by_title (bool, optional): la recherche se fait soit par titre soi par arxiv_id. Defaults to False.
        """
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
        """Requête vers l'api Crossref à partir du DOI d'un papier

        Args:
            paper_doi (str):
            aiohttp_session (_type_): session
            dataset_lock (asyncio.Lock): pour l'accès concurent au dataset
        """
        if paper_doi in self.dataset.keys():
            return

        query = f"{CROSS_REF_API}/works/{paper_doi}"
        result = await self.async_get_request(query, aiohttp_session)
        if result is None:
            return

        title = result["title"][0]

        query = f"{CROSS_REF_API}/works?" +\
            f"query.bibliographic={title.replace('&', ' and ')}" +\
            f"&select=DOI,URL,title,subject,publisher,reference,author,event,created,deposited&rows=1" +\
            f"&mailto={MAIL_TO}"

        result = await self.async_get_request(query, aiohttp_session)
        if result is None:
            return

        matched = self._check_title_match(result["title"][0], title, 0.9)
        if matched:
            await self.parse_paper_dict(result, aiohttp_session, dataset_lock)

    async def collect_paper(self, paper_info, aiohttp_session, dataset_lock):
        """Collecte l'ensemble des infos sur un papier, ainsi que les infos sur ses références, et ajoute
            le tout au dataset
                Recherche du papier via l'API crossref, recherche de la classification via l'API arxiv.
                Scrappe les références si elle ne sont pas données, et que l'éditeur est connu.
                Collecte les informations relatives à chaque référence.
                Ajoute les infos au dataset

        Args:
            paper_info (dict): dict décrivant un papier, informations partielles et sommaires, venant de papers with code
            aiohttp_session (_type_): session
            dataset_lock (asyncio.Lock): pour l'accès concurent au dataset
        """

        query = f"{CROSS_REF_API}/works?" +\
            f"query.bibliographic={paper_info.title.replace('&', ' and ')}" +\
            f"&select=DOI,URL,title,subject,publisher,reference,author,event,created,deposited&rows=1" +\
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
        """Lance la collecte d'informations à partir d'un bloc de papiers, fourni par l'api papers with code
            La collecte des infos de ce bloc de papier est faite en asynchrone
            Une fois la collecte terminée, le programme passe à un bloc suivant.

        Args:
            papers_chunk: objet comportant le titre + quelques infos sur un bloc de papiers, venant de papers with code
        """

        timeout = aiohttp.ClientTimeout(total=self.get_timeout)

        connector = aiohttp.TCPConnector(limit=self.max_tcp_conn)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as aiohttp_session:

            dataset_lock = asyncio.Lock()
            corountines_list = []

            for paper in papers_chunk.results:
                corountines_list.append(
                    self.collect_paper(paper, aiohttp_session, dataset_lock)
                )
            await asyncio.gather(*corountines_list)

    def start(self):
        """Itère sur l'ensemble des papiers disponibles via l'api papers with code, bloc par bloc.
            Pour chaque bloc, les infos de chaque papiers et leurs références sont collectées, et 
            ajouter au dataset.
        """
        pwc_num_papers = self.pwc_client.paper_list(
            page=1, items_per_page=1).count

        for page_num in range(30, pwc_num_papers // self.max_item):
            try:
                papers_chunk = self.pwc_client.paper_list(
                    page=page_num, items_per_page=self.max_item
                )
            except:
                print("failed to load chunk")
                continue

            print("\n___________ page num : ", page_num)
            self.time_start = time.perf_counter()

            asyncio.run(self._start_acquisition(papers_chunk))
            print("\n__________ publisher_name \n", end=" ")

            for k in self.new_publishers_list.keys():
                print(k, self.new_publishers_list[k])

            print(
                f"\n___________ chunk completed : papers: {len(self.dataset)} ref:{self.num_references}")

            # entre chaque bloc, les donénes sont écrites
            with open('dataset_test.json', "bw") as f:
                f.write(
                    json.dumps(
                        self.dataset, ensure_ascii=False
                    ).encode("utf-8")
                )
                print(
                    f"{self.num_papers} papers and {self.num_references} refs in {time.perf_counter() - self.time_start} sec")

        self.exit()

    def exit(self):
        """Fin de l'acquisition, écriture des données
        """
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

    MAX_ITEM = 5
    MAX_TCP_CONNECTIONS = 40
    GET_TIMEOUT = 5
    MAX_REFERENCES = 50

    IEEE_SCRAPPER_EXECUTABLE_PATH = "/usr/bin/chromium-browser"

    KNOWN_PUBLISHER_LIST = [
        "IEEE", "Institute of Electrical and Electronics Engineers (IEEE)"
    ]

    app = PaperGraphCreator(
        ARXIV_API, CROSS_REF_API, MAIL_TO, KNOWN_PUBLISHER_LIST,
        max_item=MAX_ITEM, get_timeout=GET_TIMEOUT, scrapp=True,
        max_tcp_conn=MAX_TCP_CONNECTIONS, max_reference=MAX_REFERENCES)

    app.start()

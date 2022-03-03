import time
import queue
import requests
import re
from difflib import SequenceMatcher

import paperswithcode

import ieee_scrapper


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

        : bien benchmark

        : faire query sur l'affiliation

"""


class PaperGraphCreator():
    def __init__(self, arxiv_api, cross_ref_api, mail_to,
                 known_publisher_list, max_item=50, get_time_out=5):

        self.pwc_client = paperswithcode.PapersWithCodeClient()
        self.arxiv_api = arxiv_api
        self.cross_ref_api = cross_ref_api
        self.mail_to = mail_to
        self.max_item = max_item
        self.get_time_out = get_time_out

        self.known_publisher_list = known_publisher_list

        self.query_tasks_queue = queue.Queue()

        self.new_publishers_list = set()

        self.scrappers = {
            "IEEE": ieee_scrapper.IEEE_scrapper(
                ieee_scrapper.EXECUTABLE_PATH,
                ieee_scrapper.request_header
            ),
            "Springer": None,
            "research_gate": None
        }

        self.dataset = {}

    def clean_title_string(self, title):
        t = title.lower()
        t = re.sub(r'[ \t]+', ' ', t)
        t = re.sub(r'[\\^(){}<>$=\n]', ' ', t)

        return t

    def do_request(self, query):
        try:
            result = requests.get(
                query, timeout=self.get_time_out).json()

            if result["status"] != "ok":
                return "failed"
            else:
                return result

        except requests.ReadTimeout:
            return "failed"

    def start_acquisition(self):
        pwc_num_papers = self.pwc_client.paper_list(
            page=1, items_per_page=1).count

        for page_num in range(2528, pwc_num_papers // self.max_item):

            papers_chunk = self.pwc_client.paper_list(
                page=page_num, items_per_page=MAX_ITEM
            )

            print("\n___________ page num : ", page_num)
            r = 0
            s = 0
            for i, paper in enumerate(papers_chunk.results):
                if i % 5 == 0:
                    print("papier ", i)
                paper_dict = {
                    "url_doi": None,
                    "titre": paper.title,
                    "authors": [None],
                    "date": str(paper.published),
                    "langue": None,
                    "domaine_arxiv": None,
                    "task": None,
                    "num_citations": None,
                    "conference": {"nom": paper.conference, "lieu": None, "date": None},
                    "publisher": None,
                    "key_words": None,
                    "reference": []
                }

                query = f"{CROSS_REF_API}/works?query.bibliographic={paper.title.replace('&', ' and ')}" +\
                        f"&select=DOI,URL,title,subject,publisher,reference,author&rows=1" +\
                        f"&mailto={MAIL_TO}"

                result = self.do_request(query)

                if result == "failed" or result["status"] != "ok":
                    continue

                result = result["message"]["items"][0]
                t1 = self.clean_title_string(paper.title)
                t2 = self.clean_title_string(result['title'][0])
                score = SequenceMatcher(None, t1, t2).ratio()

                if score >= 0.9:
                    print("\nsearch : ", score, "\n", t1, "\n", t2, "\n")
                    paper_doi = result["DOI"]
                    print("> ", paper_doi, end=" ")
                    s = s + 1

                    if paper_doi not in self.dataset.keys():
                        self.dataset[paper_doi] = paper_dict

                    if "reference" in result.keys():
                        reference_list = result["reference"]
                        print(" reflist ", len(reference_list), end=" ")
                        r = r + 1

                        for ref in reference_list:
                            if "DOI" in ref.keys():
                                ref_doi = ref["DOI"]

                                self.query_tasks_queue.put({
                                    "type": "query_from_doi",
                                    "query": f"{CROSS_REF_API}/works/{ref_doi}"
                                })

                                self.dataset[paper_doi]["reference"].append(
                                    ref_doi)

                            elif "unstructured" in ref.keys():
                                unstruct_title = ref["unstructured"]

                                query = f"{CROSS_REF_API}/works?query.bibliographic={unstruct_title}" +\
                                    f"&select=DOI,title&rows=1" +\
                                    f"&mailto={MAIL_TO}"

                                result_ref = self.do_request(query)
                                if result_ref == "failed":
                                    continue

                                result_ref_content = result_ref["message"]["items"][0]
                                if "title" in result_ref_content.keys() is False:
                                    continue

                                if SequenceMatcher(None, result_ref_content["title"], unstruct_title).ratio() > 0.75:
                                    if "DOI" in result_ref_content.keys():
                                        self.dataset[paper_doi]["reference"].append(
                                            result_ref_content["DOI"]
                                        )

                        print(" got ref ", len(
                            self.dataset[paper_doi]["reference"])
                        )

                    elif "publisher" in result.keys():

                        #url = result["URL"]
                        publisher = result["publisher"]

                        if publisher in self.known_publisher_list:
                            print("    peut scrapper publisher", publisher)

                            if publisher.strip() == "IEEE" or \
                                    publisher.strip() == "Institute of Electrical and Electronics Engineers (IEEE)" or\
                                                         "Institute of Electrical and Electronics Engineers (IEEE)":
                                self.scrappers["IEEE"].scrapp_page(paper_doi)
                        else:
                            self.new_publishers_list.add(publisher)
                            print("publisher ", self.new_publishers_list)

                    elif "URL" in result.keys():
                        print("peut etre scrapper ", result["URL"])

                    if "author" in result.keys():
                        for author in result["author"]:
                            paper_dict["authors"].append(
                                {
                                    "name": author["given"]+" "+author["family"],
                                    "affiliation": author["affiliation"]
                                }
                            )

            print(" ref ", r, " num pap ", s)


if __name__ == '__main__':

    ARXIV_API = "http://export.arxiv.org/api/query?id_list="
    CROSS_REF_API = "https://api.crossref.org"
    MAIL_TO = "lulud41@gmail.com"

    MAX_ITEM = 20
    GET_TIMEOUT = 10

    KNOWN_PUBLISHER_LIST = ["springer", "IEEE", "research_gate",
                            "Institute of Electrical and Electronics Engineers (IEEE)"]

    t1 = time.perf_counter()

    app = PaperGraphCreator(
        ARXIV_API, CROSS_REF_API, MAIL_TO, KNOWN_PUBLISHER_LIST,
        max_item=MAX_ITEM, get_time_out=GET_TIMEOUT
    )
    app.start_acquisition()
    print(time.perf_counter() - t1)

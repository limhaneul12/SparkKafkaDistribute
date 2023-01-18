import os
import pathlib
import collections
import calendar
import logging
from typing import List
from queue import Queue

from urllib.request import urlretrieve
from bs4 import BeautifulSoup
from page_source import GoogleUtilityDriver as gd


if not hasattr(collections, 'Callable'):
    collections.Callable = collections.abc.Callable


try:
    os.mkdir(f"{pathlib.Path(__file__).parent.parent}/sparkAnaliysis/data/")
except FileExistsError:
    logging.info(f'이미 메인 파일이 존재합니다.')


q = Queue()
month = list(calendar.month_name)
tlc_url: str = gd().page()
bs = BeautifulSoup(tlc_url, "html.parser")    

def dirctory_download(element: BeautifulSoup) -> List[str]:
    return [data["href"] for data in element.find_all("a", {"title": "High Volume For-Hire Vehicle Trip Records"})]


def file_download(start: int, end: int) -> None:
    for j in range(start, end-1, -1):
        for i in bs.find_all("div", {"data-answer": f"faq20{j}", "class": "faq-questions collapsed"}):
            try:
                name = i.text.replace("\n", "")
                os.mkdir(f"{pathlib.Path(__file__).parent.parent}/sparkAnaliysis/data/{name}/")
            except (FileExistsError, ValueError):
                continue 

        
def search_injection(start: int, end: int) -> None:
    for i in range(start, end-1, -1):
        for inner in bs.find_all("div", {"class": "faq-answers", "id": f"faq20{i}"}): 
            file_download(start=start, end=end)
            data_struct: List[str] = dirctory_download(inner)
            q.put(data_struct)


def download(n: int) -> None:
    j: int = 0
    while j < n:
        logging.info(f"{j}번째 색션에 접근합니다")
        for da in q.get():
            name: str = da.split("/")[4]
            name_number: int = int(name.split("_")[2].split("-")[0])
            file_location: str = f"{pathlib.Path(__file__).parent.parent}/sparkAnaliysis/data/{name_number}/{name}"
            logging.info(f"{file_location} 저장합니다")
            urlretrieve(da, file_location)
        j+=1     



search_injection(start=22, end=20)
download(n=3)
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
    os.mkdir(f"{pathlib.Path().cwd()}/data/")
except FileExistsError:
    logging.info(f'이미 메인 파일이 존재합니다.')


q = Queue()
month = list(calendar.month_name)
tlc_url: str = gd().page()


def dirctory_download(element: BeautifulSoup) -> List[str]:
    return [data["href"] for data in element.find_all("a", {"title": "High Volume For-Hire Vehicle Trip Records"})]


def file_download(element: BeautifulSoup) -> None:
    for i in element.find_all("p"):
        try:
            if month.index(i.text): 
                os.mkdir(f"{pathlib.Path().cwd()}/data/{month.index(i.text)}/")
        except (FileExistsError, ValueError):
            continue 

        
def search_injection() -> None:
    bs = BeautifulSoup(tlc_url, "html.parser")
    for i in range(22, 19, -1):
        for inner in bs.find_all("div", {"class": "faq-answers", "id": f"faq20{i}"}): 
            file_download(inner)
            data_struct: List[str] = dirctory_download(inner)
            q.put(data_struct)


def download() -> None:
    j: int = 0
    while j < 3:
        logging.info(f"{j}번째 색션에 접근합니다")
        for da in q.get():
            name: str = da.split("/")[4]
            name_number: int = int(name.split("_")[2].split("-")[1].split(".")[0])
            file_location: str = f'{os.getcwd()}/data/{name_number}'
            logging.info(f"{pathlib.Path(__file__).parent.parents}/sparkAnaliysis/{file_location}/{name_number} 파일로 저장합니다")
            urlretrieve(da, file_location)
        j+=1     


search_injection()
download()
import os
import pathlib
import logging
from typing import Final
from queue import Queue

from urllib.request import urlretrieve
from bs4 import BeautifulSoup
from page_source import GoogleUtilityDriver as gd


PATH: Final[str] = f"{pathlib.Path(__file__).parent.parent}/sparkAnaliysis/data"
CPATH: Final[str] = f"{os.getcwd()}/data"


try:
    os.mkdir(f"{PATH}/")
except FileExistsError:
    logging.info(f"이미 메인 파일이 존재합니다.")


q = Queue()
tlc_url: str = gd().page()
bs = BeautifulSoup(tlc_url, "html.parser")


def file_download(element: BeautifulSoup) -> list[str]:
    return [
        data["href"]
        for data in element.find_all(
            "a", {"title": "High Volume For-Hire Vehicle Trip Records"}
        )
    ]


def folder_making(start: int, end: int, path: str) -> None:
    for j in range(start, end - 1, -1):
        for i in bs.find_all(
            "div", {"data-answer": f"faq20{j}", "class": "faq-questions collapsed"}
        ):
            try:
                name: str = i.text.replace("\n", "")
                os.mkdir(f"{path}/{name}/")
            except (FileExistsError, ValueError):
                continue


def search_injection(start: int, end: int, path: str) -> None:
    for i in range(start, end - 1, -1):
        for inner in bs.find_all("div", {"class": "faq-answers", "id": f"faq20{i}"}):
            folder_making(start=start, end=end, path=path)
            data_struct: list[str] = file_download(inner)
            q.put(data_struct)


def download(n: int, path: str) -> None:
    j: int = 0
    while j < n:
        logging.info(f"{j}번째 색션에 접근합니다")
        for da in q.get():
            name: str = da.split("/")[4]
            name_number: int = int(name.split("_")[2].split("-")[0])
            file_location: str = f"{path}/{name_number}/{name}"
            logging.info(f"{file_location} 저장합니다")
            urlretrieve(da, file_location)
        j += 1


if __name__ == "__main__":
    search_injection(start=23, end=19, path=PATH)
    download(n=5, path=PATH)

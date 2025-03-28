from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

from .bigwebscrape import bigweb_scrape as bigwebscrapemain
from .yuyuteiscrape2 import yuyutei_scrape as yuyuteiscrapemain


def main2():
    with ThreadPoolExecutor() as executor:  # optimally defined number of threads
        futures = []
        futures.append(executor.submit(bigwebscrapemain))
        futures.append(executor.submit(yuyuteiscrapemain))

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except AttributeError as e:
                print(type(e), e)


if __name__ == "__main__":
    main2()

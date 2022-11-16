import csv
import datetime
import json
import logging
import sys
import time
from encodings import utf_8_sig  # 액셀파일에서 한글깨질때

import azure.functions as func
import requests
from bs4 import BeautifulSoup

sys.path.append('./')
#from . import kospi
  
#def main(mytimer: func.TimerRequest) -> None:
def main(mytimer: func.TimerRequest, tablePath:func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    tags1 = finance()
    
    title = "N	종목명	현재가	전일비	등락률	액면가	시가총액	상장주식수	외국인비율	거래량	PER	ROE".split("\t")
    data__kospi1 = []
    for i, tags in enumerate(tags1):
        data_kospi = {
            "N": tags[0],
            "종목명": tags[1],
            "현재가": tags[2],
            "전일비": tags[3],
            "등락률": tags[4],
            "액면가": tags[5],
            "시가총액": tags[6],
            "상장주식수": tags[7],
            "외국인비율": tags[8],
            "거래량": tags[9],
            "PER": tags[10],
            "ROE": tags[11],
            "PartitionKey": f"종목명{i}",
            "RowKey": time.time()
            
        }
        data__kospi1.append(data_kospi)
    #print (data__kospi1)
    print("data_kospi1")
    
    tablePath.set(json.dumps(data__kospi1))

def finance():
    
    url = "https://finance.naver.com/sise/sise_market_sum.nhn?&page="

    filename = "kospi_index.csv"
    f = open(filename, "w", encoding="utf_8_sig",newline= "")
    writer = csv.writer(f)
    title = "N	종목명	현재가	전일비	등락률	액면가	시가총액	상장주식수	외국인비율	거래량	PER	ROE".split("\t")
    writer.writerow(title)

    for page in range(1,5):
        res = requests.get(url + str(page))
        res.raise_for_status
        soup = BeautifulSoup(res.text, "lxml")
        
        data_results = []
        try:
            data_rows = soup.find("table", attrs={"class":"type_2"}).find("tbody").find_all("tr")
            for row in data_rows:
                columns = row.find_all("td")
                if len(columns) <= 1 :
                    continue
                data = [column.get_text().strip() for column in columns]
                #print(data)
                data_results.append(data)
                writer.writerow(data)
                print(data_results)
        except IndexError:
            pass
        return data_results
        
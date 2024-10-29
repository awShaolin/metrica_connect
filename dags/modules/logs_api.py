import requests
import configparser 
import time 
import pandas as pd
import os
import shutil
import logging
from urllib.parse import urlencode
from io import StringIO





class LogsApi:
    ########################################################################################################################
    #                                       CONSTANTS 
    
    # забираем токен и id счетчика из config.ini
    config = configparser.ConfigParser()
    config.read('/opt/airflow/dags/creds/config.ini')
    COUNTER_ID = config['METRICA']['COUNTER_ID']
    TOKEN = config['METRICA']['TOKEN']

    API_URL = "https://api-metrika.yandex.ru/management/v1/counter/{counter_id}/logrequest".format(counter_id=COUNTER_ID)
    SOURCE = ['hits','visits']

    API_FIELDS = {'visits':('ym:s:visitID', 'ym:s:counterID', 'ym:s:watchIDs',
                            'ym:s:date', 'ym:s:dateTime', 'ym:s:clientID', 'ym:s:startURL', 
                            'ym:s:deviceCategory', 'ym:s:lastDirectClickOrder', 'ym:s:lastDirectClickOrderName', 'ym:s:lastTrafficSource',
                            'ym:s:lastAdvEngine', 'ym:s:lastReferalSource', 'ym:s:lastSearchEngineRoot', 'ym:s:lastSearchEngine', 
                            'ym:s:lastSocialNetwork', 'ym:s:referer', 'ym:s:lastUTMCampaign', 'ym:s:lastUTMContent',
                            'ym:s:lastUTMMedium', 'ym:s:lastUTMSource', 'ym:s:lastUTMTerm', 'ym:s:regionCountry',
                            'ym:s:regionCity', 'ym:s:goalsID', 'ym:s:goalsSerialNumber', 'ym:s:goalsDateTime',
                        ),
                'hits':('ym:pv:watchID','ym:pv:counterID','ym:pv:dateTime','ym:pv:title',
                        'ym:pv:URL','ym:pv:UTMCampaign','ym:pv:UTMContent','ym:pv:UTMMedium',
                        'ym:pv:UTMSource','ym:pv:UTMTerm','ym:pv:clientID','ym:pv:parsedParamsKey1','ym:pv:parsedParamsKey2','ym:pv:parsedParamsKey3')}

    header_dict = {'Authorization': f'OAuth {TOKEN}',
    'Content-Type': 'application/x-yametrika+json'
    }    

    ########################################################################################################################


    @classmethod
    def create_log_request(cls, date1, date2, source):
        # создание запроса на логирование
        try:
            url_params = urlencode(
                [
                    ('date1', date1),
                    ('date2', date2),
                    ('source', source),
                    ('fields', ','.join(cls.API_FIELDS.get(source)))
                ]
            )
            url = cls.API_URL + 's?' + url_params

            logging.info(f"URL для отправки запроса: {url}")
            logging.info(f"Параметры запроса: {url_params}")
            logging.info(f"Заголовки запроса: {cls.header_dict}")

            response = requests.post(url, headers=cls.header_dict)
            logging.info(response)
            response.raise_for_status()  

            print(f"Запрос для {source} за даты {date1} и {date2} успешно создан.")
            return response.json()["log_request"]["request_id"]

        except requests.exceptions.RequestException as e:
            print(f"Ошибка при создании запроса: {e}")
        except KeyError:
            print("Ошибка в структуре ответа: отсутствует 'log_request' или 'request_id'.")

        
    @classmethod
    def get_log_request_status(cls, request_id):
        # проверка статуса запроса на логирование
        try:
            time.sleep(120)
            url = f"{cls.API_URL}/{request_id}"

            response = requests.get(url, headers=cls.header_dict)
            logging.info(f'Response: {response}')
            response.raise_for_status()
            status = response.json()["log_request"]
        
            return status
        
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при проверке статуса запроса: {e}")

        
    @classmethod
    def download_log_files(cls, request_id, status, source, folder_base):
        #create dir for load metrica files 
        local_dwnld_dir = f'{folder_base}{source}/{request_id}' 
        if os.path.exists(local_dwnld_dir):
            shutil.rmtree(local_dwnld_dir)
        os.makedirs(local_dwnld_dir, exist_ok=True)

        parts = status['parts']
        logging.info(f'Parts: {parts}')

        for part in parts:
            part_number = part['part_number']
            url = f"{cls.API_URL}/{request_id}/part/{part_number}/download"
            response = requests.get(url, headers=cls.header_dict)
            if response.status_code == 200:
                tmp_df = pd.read_csv(StringIO(response.text), sep='\t')
                output_path = os.path.join(local_dwnld_dir, f'log_data_{request_id}_part_{part_number}.csv')
                tmp_df.to_csv(output_path, index=False)
                print(f"Часть {part_number} успешно сохранена в {output_path}")
            else:
                print(f"Ошибка при скачивании части {part_number}: {response.status_code} - {response.text}")

    @classmethod
    def clean_log_files(cls, request_id):
        # очистка логов после загрузки 
        url = f"{cls.API_URL}/{request_id}/clean"

        response = requests.post(url, headers=cls.header_dict)
        if response.status_code == 200:
            print(f"Логи по request_id = {request_id} очищены")
        else:
            print(f"Ошибка при попытке очистить логи по request_id = {request_id}: {response.status_code} - {response.text}")


    

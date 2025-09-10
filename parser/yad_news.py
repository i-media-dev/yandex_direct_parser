import json
import logging
import os
import time

from dotenv import load_dotenv
import pandas as pd
import requests
from inspect import getsourcefile

from parser.constants import CLIENT_LOGINS, YAD_REPORTS_URL, REPORT_FIELDS, REPORT_NAME
from parser.logging_config import setup_logging

load_dotenv()
setup_logging()


token = str(os.getenv('YANDEX_DIRECT_TOKEN'))

dates_list = []


def get_direct_report(token, login, date_from, date_to):

    def u(x):
        if type(x) is type(b''):
            return x.decode('utf8')
        else:
            return x

    response = requests.Response

    token = token

    clientLogin = login

    headers = {
        "Authorization": "Bearer " + token,
        "Client-Login": clientLogin,
        "Accept-Language": "ru",
        "processingMode": "auto"
    }

    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": REPORT_FIELDS[0],
            "ReportName": u(REPORT_NAME),
            "ReportType": "CUSTOM_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "NO"
        }
    }

    body = json.dumps(body, indent=4)

    while True:
        try:
            response = requests.post(YAD_REPORTS_URL, body, headers=headers)
            response.encoding = 'utf-8'
            if response.status_code == 400:
                print('''Параметры запроса указаны неверно
                или достигнут лимит отчетов в очереди''')
                print('RequestId: {}'.format(
                    response.headers.get('RequestId', False)))
                print('JSON-код запроса: {}'.format(u(body)))
                print('JSON-код ответа сервера: \n{}'.format(u(response.json())))
                break
            elif response.status_code == 200:
                format(u(response.text))
                break
            elif response.status_code == 201:
                retryIn = int(response.headers.get('retryIn', 60))
                time.sleep(retryIn)
            elif response.status_code == 202:
                retryIn = int(response.headers.get('retryIn', 60))
                time.sleep(retryIn)
            elif response.status_code == 500:
                print(
                    'Ошибка. Повторить запрос позднее')
                print('RequestId: {}'.format(
                    response.headers.get('RequestId', False)))
                print('JSON-код ответа сервера: \n{}'.format(u(response.json())))
                break
            elif response.status_code == 502:
                print('Время формирования отчета превышено')
                print(
                    'Изменить параметры запроса')
                print('JSON-код запроса: {}'.format(body))
                print('RequestId: {}'.format(
                    response.headers.get('RequestId', False)))
                print('JSON-код ответа сервера: \n{}'.format(u(response.json())))
                break
            else:
                print('Произошла непредвиденная ошибка')
                print('RequestId:  {}'.format(
                    response.headers.get('RequestId', False)))
                print('JSON-код запроса: {}'.format(body))
                print('JSON-код ответа сервера: \n{}'.format(u(response.json())))
                break

        except requests.exceptions.ConnectionError:
            print('Произошла ошибка соединения с сервером API')
            break

        except Exception as e:
            print(f'ошибка: {e}')
            break

    # json_string = json.dumps(body)

    return response.text


def get_platform_type(row):
    if 'srch' in row['CampaignName']:
        return 'поиск'
    else:
        return 'сеть'


def get_campaign_category(row):
    if 'dsa' in row['CampaignName']:
        return 'dsa'
    elif '-nz' in row['CampaignName']:
        return 'nz'
    elif '_nz' in row['CampaignName']:
        return 'nz'
    elif 'shop' in row['CampaignName']:
        return 'shoping'
    elif 'corporate' in row['CampaignName']:
        return 'b2b'
    elif 'promo' in row['CampaignName']:
        return 'акции'
    elif 'brand' in row['CampaignName']:
        return 'бренд'
    elif 'cat-cv' in row['CampaignName']:
        return 'кат + вендор'
    elif 'categor' in row['CampaignName']:
        return 'категории'
    elif 'compet' in row['CampaignName']:
        return 'конкуренты'
    elif 'config' in row['CampaignName']:
        return 'конфигуратор'
    elif 'rmkt' in row['CampaignName']:
        return 'ремарктеинг'
    elif 'usilenie' in row['CampaignName']:
        return 'усиление'
    else:
        return 'разное'


def get_all_direct_data():
    combined_data = pd.DataFrame()
    current_index = 0
    script_path = os.path.abspath(str(getsourcefile(lambda: 0)))
    temp_cache_path = f'{script_path[:-11]}/data/cashe.csv'

    for current_index, login in enumerate(CLIENT_LOGINS):
        try:
            print(
                f'\rвыгрузка №{current_index + 1}, аккаунт: {login}'
            )
            current_login = CLIENT_LOGINS[current_index]
            data = get_direct_report(
                token, current_login, dates_list[0], dates_list[-1])
            file = open(temp_cache_path, "w")
            file.write(data)
            file.close()
            f = pd.read_csv(temp_cache_path, sep='	',
                            encoding='cp1251', header=1)
            f['акаунт'] = CLIENT_LOGINS[current_index]
            combined_data = pd.concat([combined_data, f])
            time.sleep(1)
            current_index += 1
        except Exception as e:
            print(f'ошибка: {e}')
            current_index += 1

    combined_data['источник'] = 'yandex'
    combined_data['Cost'] = combined_data['Cost']*1.2/1000000
    combined_data = combined_data[~combined_data['Date'].str.contains(
        r'Total', case=False, na=False)]
    combined_data['поиск/сеть'] = combined_data.apply(
        get_platform_type, axis=1)
    combined_data['тип'] = combined_data.apply(get_campaign_category, axis=1)
    return combined_data


def get_filtered_cache_data():
    script_path = os.path.abspath(str(getsourcefile(lambda: 0)))
    temp_cache_path = f'{script_path[:-11]}/data/cashe_new.csv'
    old_df = pd.read_csv(temp_cache_path, sep=';', encoding='cp1251', header=0)
    for dates in dates_list:
        old_df = old_df[~old_df['Date'].fillna('').str.contains(
            fr'{dates}', case=False, na=False
        )]
    return old_df


def save_data(df_new, old_df):
    script_path = os.path.abspath(str(getsourcefile(lambda: 0)))
    temp_cache_path = f'{script_path[:-11]}/data/cashe_new.csv'
    for dates in dates_list:
        old_df = old_df[~old_df['Date'].fillna('').str.contains(
            fr'{dates}', case=False, na=False)]

    old_df = pd.concat([df_new, old_df])
    old_df.to_csv(
        temp_cache_path,
        index=False,
        header=True,
        sep=';',
        encoding='cp1251'
    )

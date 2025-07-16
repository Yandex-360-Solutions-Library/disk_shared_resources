from argparse import ArgumentParser
import argparse
import csv
import logging
import os
from textwrap import dedent
from time import time
from typing import Dict, List
from tqdm import tqdm

from dotenv import load_dotenv
from y360_orglib import DiskAdminClient, DiskClientError, configure_logger
from y360_orglib.disk.models import MacroAccess, ResourceShort, UserAccess



log = configure_logger(
    level=logging.INFO,
    console=False,
    logger_name=__name__,
    log_file="disk_report.log"
)

def arg_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description=dedent("""
        Скрипт выгружает данные о ресурсах на диске, которыми поделился пользователь в файл disk_report.csv
        Параметры:
        --users <file.csv> - файл со списком пользователей получаемый скриптом listusers.py
        """),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--users', type=str, required=True, help='CSV файл со списком пользователей')
    return parser

def get_user_shared_resources(user_id: str, user_email: str, client: DiskAdminClient, w: csv.DictWriter):

    resource_items: List[ResourceShort] = client.get_user_public_resources(user_id)
        
    for resource in resource_items:
        try:
            log.debug(f'*** Ресурс {resource.type}: {resource.name} путь: {resource.path}')

            try:
                res = client.get_public_settings_by_path(user_id, resource.path)
            except DiskClientError:
                if not resource.public_hash:
                    log.error(f'Не удалось получить настройки для ресурса {resource.path}: нет public_hash')
                    continue
                log.debug(f'Попытка получить настройки по public_hash для ресурса {resource.path}')
                res = client.get_public_settings_by_key(resource.public_hash)

            accesses = res.public_accesses
            
            for access in accesses:
                if type(access) is MacroAccess:
                    log.debug(f"Права: {access.rights}")
                    access_type = access.macros[0]
                    #if access_type == 'macro':
                        #access_type = 'public'
    
                    w.writerow({
                        'email': user_email,
                        'path':  resource.path,
                        'access_type': access_type,
                        'rights': access.rights,
                        'user_id': '',
                        'external_user': ''
                    })

                elif type(access) is UserAccess:
                    log.debug(f"Доступ сотруднику: {access.user_id} права: {access.rights}")
                    w.writerow({
                        'email': user_email,
                        'path': resource.path,
                        'access_type': access.access_type,
                        'rights': access.rights,
                        'user_id': access.user_id,
                        'external_user': not access.org_id
                    })
        except DiskClientError as e:
            log.error(f'Ошибка при получении публичных настроек для ресурса {resource.path}: {e}')


def main(users: List[Dict]):

    token = os.getenv('TOKEN')
    org_id = os.getenv('ORG_ID')
    if not token or not org_id:
        raise ValueError('Не указаны переменные окружения TOKEN и/или ORG_ID')

    log.info(f'Загрузка пользователей завершена. Загружено {len(users)} пользователей.')
    client = DiskAdminClient(token=token, org_id=org_id)
    processed = 0
    with tqdm(total=len(users), unit="User") as progress:
        with open('disk_report.csv', 'a', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, ['email',
                                'path',
                                'access_type',
                                'rights',
                                'user_id',
                                'external_user'])
            for user in users:
                if user.get('ID', '')[:3] == '113':
                    
                    user_id = user.get('ID', '')
                    user_email = user.get('Email', '')
                    log.info(f'Обработка ресурсов пользователя: {user_id}')
                    try:
                        get_user_shared_resources(user_id, user_email, client, w)
                        processed += 1
                    
                    except Exception as e:
                        log.error(f'Ошибка при обработке ресурсов пользователя: {user_email}')
                        log.error(f'{type(e)} - {e}')
                        
                        #raise e
                else:
                    log.warning(f'Пропуск пользователя: {user.get('Email')}')
                progress.update(1)
    client.close()
    return processed, users


def read_users_csv(file_path: str) -> List[Dict]:
    users:List[Dict] = []
    try:
        with open(file_path, 'r', encoding='utf8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                user_id = row.get('ID')
                if user_id and len(user_id) == 16:
                    users.append(row)
        return users
    except FileNotFoundError:
        print(f'Файл {file_path} не найден')
        exit(1)

if __name__ == '__main__':
    load_dotenv()
    print('Запуск...\n')
    parser = arg_parser()
    args = parser.parse_args()
    log.info('Загрузка пользователей...')
    users = read_users_csv(args.users)
    
    start_time = time()

    with open('disk_report.csv', 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, ['email',
                               'path',
                               'access_type',
                               'rights',
                               'user_id',
                               'external_user'
                               ])
        w.writeheader()    

    try:
        processed, users = main(users=users)
        end_time = time()
        log.info(f'Завершено. Обработано пользователей: {processed} из {len(users)} за {round(end_time - start_time)} секунд.')
        print(f'\nЗавершено. Обработано пользователей: {processed} из {len(users)} за {round(end_time - start_time)} секунд.')
        print('Отчет: disk_report.log')
        print('Результат: disk_report.csv')
    except Exception as e:
        log.error(f'Ошибка: {e}')
    exit(0)
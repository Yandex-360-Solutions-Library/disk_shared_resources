from argparse import ArgumentParser, RawDescriptionHelpFormatter
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

# Constants
CSV_HEADERS = [
    'email',
    'path',
    'access_type',
    'rights',
    'user_id',
    'shared_email',
    'external_user'
]

USER_ID_PREFIX_FILTER = '113'  # Только пользователи из организации
VALID_USER_ID_LENGTH = 16      # Исключаем роботов и пр.



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
        formatter_class = RawDescriptionHelpFormatter
    )
    parser.add_argument('--users', type=str, required=True, help='CSV файл со списком пользователей')
    return parser

def process_access(w: csv.DictWriter, user_email: str, resource_path: str,
                  access_type: str, rights: str,
                  user_id: str = '', shared_email: str = '',
                  external_user: bool = False) -> None:
    """Записывает информацию о доступе в CSV файл."""
    w.writerow({
        'email': user_email,
        'path': resource_path,
        'access_type': access_type,
        'rights': rights,
        'user_id': user_id,
        'shared_email': shared_email,
        'external_user': external_user
    })

def get_user_shared_resources(user_id: str, user_email: str, client: DiskAdminClient, w: csv.DictWriter, users_lookup: Dict[str, str]):
    """Обрабатываем публичные ресурсы пользователя"""
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
                if isinstance(access, MacroAccess):
                    log.debug(f"Права: {access.rights}")
                    access_type = access.macros[0]
                    process_access(w, user_email, resource.path,
                                 access_type, access.rights[0])

                elif isinstance(access, UserAccess):
                    log.debug(f"Доступ сотруднику: {access.user_id} права: {access.rights}")
                    if access.access_type != 'owner':
                        # Use optimized lookup instead of linear search
                        shared_user_email = users_lookup.get(str(access.user_id), '')
                        process_access(w, user_email, resource.path,
                                     access.access_type, access.rights[0],
                                     str(access.user_id), shared_user_email,
                                     not access.org_id)
        except DiskClientError as e:
            log.error(f'Ошибка при получении публичных настроек для ресурса {resource.path}: {e}')


def main(users_list: List[Dict]):

    token = os.getenv('TOKEN')
    org_id = os.getenv('ORG_ID')
    if not token or not org_id:
        raise ValueError('Не указаны переменные окружения TOKEN и/или ORG_ID')

    log.info(f'Загрузка пользователей завершена. Загружено {len(users_list)} пользователей.')
    
    # Create user lookup dictionary for performance optimization
    users_lookup = {user.get('ID', ''): user.get('Email', '') for user in users_list}
    
    client = DiskAdminClient(token=token, org_id=org_id)
    processed = 0
    
    try:
        with tqdm(total=len(users_list), unit="User") as progress:
            with open('disk_report.csv', 'a', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, CSV_HEADERS)
                for user in users_list:
                    user_id = user.get('ID', '')
                    if user_id[:3] == USER_ID_PREFIX_FILTER:
                        user_email = user.get('Email', '')
                        log.info(f'Обработка ресурсов пользователя: {user_id}')
                        try:
                            get_user_shared_resources(user_id, user_email, client, w, users_lookup)
                            processed += 1
                        except DiskClientError as e:
                            log.error(f'Ошибка API при обработке ресурсов пользователя {user_email}: {e}')
                        except Exception as e:
                            log.error(f'Неожиданная ошибка при обработке пользователя {user_email}: {type(e).__name__} - {e}')
                    else:
                        log.warning(f"Пропуск пользователя: {user.get('Email')}")
                    progress.update(1)
    finally:
        client.close()
    
    return processed


def read_users_csv(file_path: str) -> List[Dict]:
    """Загружаем список пользователей из CSV файла"""
    users: List[Dict] = []
    try:
        with open(file_path, 'r', encoding='utf8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                user_id = row.get('ID')
                if user_id and len(user_id) == VALID_USER_ID_LENGTH:
                    users.append(row)
        return users
    except FileNotFoundError:
        log.error(f'Файл {file_path} не найден')
        raise FileNotFoundError(f'Файл пользователей {file_path} не найден')
    except Exception as e:
        log.error(f'Ошибка при чтении файла {file_path}: {e}')
        raise

if __name__ == '__main__':
    load_dotenv()
    print('Запуск...\n')
    parser = arg_parser()
    args = parser.parse_args()
    log.info('Загрузка пользователей...')

    try:
        users_from_csv = read_users_csv(args.users)
        
        start_time = time()

        # Initialize CSV file with headers
        with open('disk_report.csv', 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, CSV_HEADERS)
            w.writeheader()

        processed = main(users_from_csv)
        end_time = time()
        msg = f'Завершено. Обработано пользователей: {processed} из {len(users_from_csv)} за {round(end_time - start_time)} секунд.'
        log.info(msg)
        print(f'\n{msg}')
        print('Отчет: disk_report.log')
        print('Результат: disk_report.csv')
        
    except FileNotFoundError as e:
        log.error(f'Файл не найден: {e}')
        print(f'Ошибка: {e}')
        exit(1)
    except ValueError as e:
        log.error(f'Ошибка конфигурации: {e}')
        print(f'Ошибка конфигурации: {e}')
        exit(1)
    except Exception as e:
        log.error(f'Неожиданная ошибка: {type(e).__name__} - {e}')
        print(f'Неожиданная ошибка: {e}')
        exit(1)
    
    exit(0)
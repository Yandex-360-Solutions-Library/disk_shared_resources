from argparse import ArgumentParser, RawDescriptionHelpFormatter
import csv
import logging
import os
from textwrap import dedent
from time import time
from typing import Dict, List
from tqdm import tqdm

from dotenv import load_dotenv
from y360_orglib import DiskAdminClient, DiskClientError, DirectoryClient, configure_logger
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
    log_file="disk_report_by_email.log"
)

def arg_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description=dedent("""
        Скрипт выгружает данные о ресурсах на диске, которыми поделился пользователь в файл disk_report_by_email.csv
        Параметры:
        --emails <file.csv> - файл со списком email адресов пользователей
        """),
        formatter_class = RawDescriptionHelpFormatter
    )
    parser.add_argument('--emails', type=str, required=True, help='CSV файл со списком email адресов')
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


def load_all_users_lookup() -> tuple[Dict[str, str], Dict[str, str]]:
    """Загружает всех пользователей из API для создания lookup словарей"""
    token = os.getenv('TOKEN')
    org_id = os.getenv('ORG_ID')
    if not token or not org_id:
        raise ValueError('Не указаны переменные окружения TOKEN и/или ORG_ID')
    
    log.info('Загрузка всех пользователей из API...')
    directory_client = DirectoryClient(api_key=token, org_id=org_id, ssl_verify=True)
    
    try:
        org_users = directory_client.get_all_users()
        users_lookup = {}  # user_id -> email
        email_to_id = {}   # email -> user_id
        
        for org_user in org_users:
            user_id = str(org_user.uid)
            email = org_user.email
            users_lookup[user_id] = email
            email_to_id[email] = user_id
        
        log.info(f'Загружено {len(users_lookup)} пользователей из API')
        return users_lookup, email_to_id
        
    except Exception as e:
        log.error(f"Ошибка при получении списка пользователей из API: {e}")
        raise e
    finally:
        directory_client.close()

def main(email_list: List[str]):

    token = os.getenv('TOKEN')
    org_id = os.getenv('ORG_ID')
    if not token or not org_id:
        raise ValueError('Не указаны переменные окружения TOKEN и/или ORG_ID')

    log.info(f'Загрузка email адресов завершена. Загружено {len(email_list)} адресов.')
    
    # Load all users from API for lookup dictionaries
    users_lookup, email_to_id = load_all_users_lookup()
    
    client = DiskAdminClient(token=token, org_id=org_id)
    processed = 0
    not_found = 0
    
    try:
        with tqdm(total=len(email_list), unit="Email") as progress:
            with open('disk_report_by_email.csv', 'a', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, CSV_HEADERS)
                for email in email_list:
                    user_id = email_to_id.get(email)
                    if user_id:
                        if user_id[:3] == USER_ID_PREFIX_FILTER and len(user_id) == VALID_USER_ID_LENGTH:
                            log.info(f'Обработка ресурсов пользователя: {email} (ID: {user_id})')
                            try:
                                get_user_shared_resources(user_id, email, client, w, users_lookup)
                                processed += 1
                            except DiskClientError as e:
                                log.error(f'Ошибка API при обработке ресурсов пользователя {email}: {e}')
                            except Exception as e:
                                log.error(f'Неожиданная ошибка при обработке пользователя {email}: {type(e).__name__} - {e}')
                        else:
                            log.warning(f"Пропуск пользователя {email}: неподходящий ID {user_id}")
                    else:
                        log.warning(f"Пользователь с email {email} не найден в организации")
                        not_found += 1
                    progress.update(1)
    finally:
        client.close()
    
    return processed, not_found


def read_emails_csv(file_path: str) -> List[str]:
    """Загружаем список email адресов из CSV файла"""
    emails: List[str] = []
    try:
        with open(file_path, 'r', encoding='utf8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                email = row.get('Email', '').strip()
                if email:
                    emails.append(email)
        return emails
    except FileNotFoundError:
        log.error(f'Файл {file_path} не найден')
        raise FileNotFoundError(f'Файл с email адресами {file_path} не найден')
    except Exception as e:
        log.error(f'Ошибка при чтении файла {file_path}: {e}')
        raise

if __name__ == '__main__':
    load_dotenv()
    print('Запуск...\n')
    parser = arg_parser()
    args = parser.parse_args()
    log.info('Загрузка email адресов из CSV...')

    try:
        emails_from_csv = read_emails_csv(args.emails)
        
        start_time = time()

        # Initialize CSV file with headers
        with open('disk_report_by_email.csv', 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, CSV_HEADERS)
            w.writeheader()

        processed, not_found = main(emails_from_csv)
        end_time = time()
        msg = f'Завершено. Обработано пользователей: {processed} из {len(emails_from_csv)} (не найдено: {not_found}) за {round(end_time - start_time)} секунд.'
        log.info(msg)
        print(f'\n{msg}')
        print('Отчет: disk_report_by_email.log')
        print('Результат: disk_report_by_email.csv')
        
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
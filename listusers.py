import csv
import logging
import os

from dotenv import load_dotenv
from y360_orglib import DirectoryClient
from y360_orglib import configure_logger

log = configure_logger(
    level=logging.DEBUG,
    console=True,
    logger_name=__name__,
    log_file="listusers.log"
)

def main():
    token = os.getenv('TOKEN', '')
    org_id = os.getenv('ORG_ID', '')

    if not token or not org_id:
        raise ValueError('Не указаны переменные окружения TOKEN и/или ORG_ID')
    client = DirectoryClient(api_key=token, org_id=org_id, ssl_verify=True)
    
    with open('users.csv', 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, [
                'ID',
                'Email',
                'Login',
                'Fname',
                'Lname',
                'Mname',
                'DisplayName',
                'Position',
                'Language',
                'Timezone',
                'Admin',
                'Enabled'
        ])
        w.writeheader()
        try:
            users = client.get_all_users()

            for org_user in users:
                w.writerow({
                    'ID': org_user.uid,
                    'Email': org_user.email,
                    'Login': org_user.nickname,
                    'Fname': org_user.name.first,
                    'Lname': org_user.name.last,
                    'Mname': org_user.name.middle,
                    'DisplayName': org_user.display_name,
                    'Position': org_user.position,
                    'Language': org_user.language,
                    'Timezone': org_user.timezone,
                    'Admin': org_user.is_admin,
                    'Enabled': org_user.is_enabled
            })

        except Exception as e:
            log.error(f"Ошибка при получении списка пользователей: {e}")
            raise e


if __name__ == '__main__':
    load_dotenv()
    main()
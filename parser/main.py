import os

from dotenv import load_dotenv

from parser.constants import CITILINK_CLIENT_LOGINS, EAPTEKA_CLIENT_LOGINS
from parser.decorators import time_of_script
from parser.ya_direct import DirectSaveClient
from parser.utils import get_date_list

load_dotenv()


@time_of_script
def main():
    """Основная логика скрипта."""
    token = str(os.getenv('YANDEX_DIRECT_TOKEN'))
    date_list = get_date_list()
    saver = DirectSaveClient(token, date_list, EAPTEKA_CLIENT_LOGINS)
    saver.save_data(
        filename_temp='temp_eapteka_direct.csv',
        filename_data='eapteka_direct.csv'
    )


if __name__ == "__main__":
    main()

import os

from dotenv import load_dotenv

from parser.decorators import time_of_script
from parser.yad_news import DataSaveClient
from parser.utils import get_date_list

load_dotenv()


@time_of_script
def main():
    """Основная логика скрипта."""
    token = str(os.getenv('YANDEX_AUTH_TOKEN'))
    date_list = get_date_list()
    saver = DataSaveClient(token, date_list)
    saver.save_data()


if __name__ == "__main__":
    main()

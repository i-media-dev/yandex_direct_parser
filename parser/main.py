import os

from dotenv import load_dotenv

from parser.decorators import time_of_script
from parser.yad_news import DataSaveClient
from parser.utils import get_date_list

load_dotenv()


@time_of_script
def main():
    token = str(os.getenv('YANDEX_DIRECT_TOKEN'))
    date_list = get_date_list()
    saver = DataSaveClient(token, date_list)
    itog = saver.get_all_direct_data()
    old_df = saver.get_filtered_cache_data()
    saver.save_data(itog, old_df)


if __name__ == "__main__":
    main()

import datetime as dt
import os

from dotenv import load_dotenv

from parser.yad_news import (
    get_all_direct_data,
    get_filtered_cache_data,
    save_data

)

load_dotenv()


def main():
    itog = get_all_direct_data()
    old_df = get_filtered_cache_data()
    save_data(itog, old_df)


if __name__ == "__main__":

    mytoken = str(os.getenv('MYTOKEN'))

    fieldsname = ["Date",
                  "CampaignName",
                  "CampaignId",
                  "Device",
                  "Impressions",
                  "Clicks",
                  "Cost"],

    dates_list = []
    # for i in range(20, 0, -1):
    for i in range(45, 0, -1):
        tempday = dt.datetime.now()
        tempday -= dt.timedelta(days=i)
        tempday_str = tempday.strftime('%Y-%m-%d')
        dates_list.append(tempday_str)

    main()

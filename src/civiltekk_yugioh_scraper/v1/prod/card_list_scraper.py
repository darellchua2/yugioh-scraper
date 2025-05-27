import datetime
import pandas as pd
from ..utilities.yugipedia_utilities import get_yugioh_set_cards
from ..utilities.aws_utilities import upload_data

from dotenv import load_dotenv


def card_list_scraper() -> tuple[pd.DataFrame, pd.DataFrame]:
    start = datetime.datetime.now()
    yugioh_set_cards, yugioh_set_card_image_file_and_image_url_with_missing_links_overall_list = get_yugioh_set_cards()
    yugioh_set_card_dicts = [yugioh_set_card.get_tekkx_wordpress_dict_from_yugioh_set_card()
                             for yugioh_set_card in yugioh_set_cards if yugioh_set_card.get_tekkx_wordpress_dict_from_yugioh_set_card() is not None]
    missing_links_dict_list = [{"image_file": obj['image_file']}
                               for obj in yugioh_set_card_image_file_and_image_url_with_missing_links_overall_list]
    df: pd.DataFrame = pd.DataFrame(yugioh_set_card_dicts)
    df_missing_links: pd.DataFrame = pd.DataFrame(missing_links_dict_list)

    df.to_csv("./output/yugioh_set_cards_to_db.csv", index=False)
    df_missing_links.to_csv("./output/missing_links.csv", index=False)

    # upload_data(df, TABLE_YUGIOH_OVERALL_CARD_CODE_LISTS,
    #             "replace", db_name="yugioh_data")

    end = datetime.datetime.now()
    difference = end - start
    print(f"The time difference between the 2 time is: {difference}")
    return df, df_missing_links


if __name__ == "__main__":
    load_dotenv()
    card_list_scraper()

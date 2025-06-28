import os

from .yugipedia.yugipedia_scraper_card_v2 import get_yugioh_cards

from ..utilities.yugipedia.yugipedia_scraper_rarity_v2 import get_yugioh_rarities_v2

from .yugipedia.yugipedia_scraper_set_v2 import get_yugioh_sets_v2

from .aws_utilities import upload_data
import pandas as pd
from ..models.yugipedia_models import YugiohCard
from ..config import TABLE_YUGIOH_CARDS, TABLE_YUGIOH_SETS, TABLE_YUGIOH_RARITIES


def yugipedia_main():
    # Get the current project path
    project_path = os.getcwd()

    # Define the output folder relative to the project path
    output_folder = os.path.join(project_path, "output")
    # Check if the folder exists, and create it if it doesn't
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    #################################
    yugioh_rarities = get_yugioh_rarities_v2()
    db_data = [yugioh_rarity.get_dict() for yugioh_rarity in yugioh_rarities]
    df = pd.DataFrame(db_data)
    df.to_csv(os.path.join(output_folder, "yugioh_rarities.csv"), index=False)
    upload_data(df, table_name=TABLE_YUGIOH_RARITIES,
                if_exist="replace", db_name='yugioh_data')

    #################################

    yugioh_sets = get_yugioh_sets_v2()
    db_data = [yugioh_set.get_dict() for yugioh_set in yugioh_sets]
    df = pd.DataFrame(db_data)
    df.to_csv(os.path.join(output_folder, "yugioh_sets.csv"), index=False)

    upload_data(df, table_name=TABLE_YUGIOH_SETS,
                if_exist="replace", db_name='yugioh_data')
    ################################
    yugioh_cards = get_yugioh_cards()

    db_data = [yugioh_card.get_dict()
               for yugioh_card in yugioh_cards]

    if db_data == []:
        print("db_data is None")
        pass
    else:
        df = pd.DataFrame(db_data)
        df.to_csv(os.path.join(output_folder, "yugioh_cards.csv"), index=False)
        upload_data(df, table_name=TABLE_YUGIOH_CARDS,
                    if_exist="replace", db_name='yugioh_data')

        tekkx_cards = [
            YugiohCard.get_yugipedia_dict_from_yugioh_card(card) for card in yugioh_cards]

        df2 = pd.DataFrame(tekkx_cards)
        df2.to_csv(os.path.join(output_folder, "tekkx_cards.csv"), index=False)
    #################################


if __name__ == "__main__":
    yugipedia_main()

import sys
from civiltekk_yugioh_scraper.v1.prod import export_inventory_excel, upload_inventory_main, card_list_scraper, scrape_main
from civiltekk_yugioh_scraper.v1.utilities import yugipedia_main


if __name__ == "__main__":
    if len(sys.argv) > 1:
        action = sys.argv[1]
        if action == "export":
            export_inventory_excel()
        if action == "upload":
            upload_inventory_main()
        if action == "scrape_yugipedia":
            yugipedia_main()
        if action == "scrape_yugipedia_set_cards":
            card_list_scraper()
        if action == "test_scrape":
            scrape_main()

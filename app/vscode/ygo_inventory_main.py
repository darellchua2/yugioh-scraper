import sys
from civiltekk_yugioh_scraper.v1.prod import upload_inventory_main, card_list_scraper, export_inventory_excel_v2, bigweb_scrape, yuyutei_scrape
from civiltekk_yugioh_scraper.v1.utilities import yugipedia_main


if __name__ == "__main__":
    if len(sys.argv) > 1:
        action = sys.argv[1]
        if action == "export_v2":
            export_inventory_excel_v2(True, True)
        if action == "upload":
            upload_inventory_main(
                is_to_save_to_mysql=True, is_to_save_to_s3=True)
        if action == "scrape_yugipedia":
            yugipedia_main()
        if action == "scrape_yugipedia_set_cards":
            card_list_scraper(to_csv=True, to_sql=True)
        if action == 'scrape_bigweb':
            bigweb_scrape()
        if action == 'scrape_yuyutei':
            yuyutei_scrape()

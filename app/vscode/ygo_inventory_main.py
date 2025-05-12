import sys
from civiltekk_yugioh_scraper.v1.prod import export_inventory_excel, upload_inventory_excel, combine_ae_price  # type: ignore

if __name__ == "__main__":
    if len(sys.argv) > 1:
        action = sys.argv[1]
        if action == "export":
            export_inventory_excel()
        if action == "upload":
            upload_inventory_excel()


from yugioh_scraper.v2.prod.tcg_corner_scraper import TcgCornerScraper, TcgCornerRarity


def test_scrape():
    TEST_URL = "https://tcg-corner.com/collections/rota-ae/products.json?limit=250&page=1"

    scraper = TcgCornerScraper()
    products = scraper.scrape(TEST_URL)

    if products:
        # Displaying first 5 products as an example
        for idx, product in enumerate(products[:5], start=1):
            print(f"{idx}. {product}")


if __name__ == "__main__":
    test_scrape()

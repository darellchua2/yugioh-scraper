from typing import TypedDict, List

cols = ["region", "set_card_name_combined", "set_name", "set_card_code_updated",
        "rarity_name", "quantity", "price", "post_name", "post_title"]


class TekkxProductData(TypedDict):
    region: str
    set_card_name_combined: str
    set_name: str
    set_card_code_updated: str
    rarity_name: str
    quantity: int
    price: float
    post_name: str
    post_title: str


def process_products(data: List[TekkxProductData]) -> None:
    for item in data:
        print(item["set_name"], item["price"])


def get_image_links_from_image_file_mediawiki_params(image_card_urls: list[str]) -> dict:
    image_card_url_list_string = "|".join(image_card_urls)
    obj = {
        "action": "query",
        "format": "json",
        "prop": "links",
        "titles": image_card_url_list_string,
        "pilimit": "500",
        "plnamespace": "0"
    }
    return obj


def get_card_names_from_card_set_codes_redirect_mediawiki_params(card_set_codes: list[str]) -> dict[str, str | int]:
    card_set_codes_string = "|".join(card_set_codes)
    obj = {
        "action": "query",
        "format": "json",
        "prop": "pageprops",
        "titles": card_set_codes_string,
        "redirects": 1,
    }
    return obj

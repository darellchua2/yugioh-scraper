
from typing import Any, Dict, Mapping


def get_image_links_from_image_file_mediawiki_params(image_card_urls: list[str]) -> Mapping:
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


def get_card_names_from_card_set_codes_redirect_mediawiki_params(card_set_codes: list[str]) -> Mapping[str, str | int]:
    card_set_codes_string = "|".join(card_set_codes)
    obj = {
        "action": "query",
        "format": "json",
        "prop": "pageprops",
        "titles": card_set_codes_string,
        "redirects": 1,
    }
    return obj


def get_wikitext_params(titles_str: str) -> Dict[str, str]:
    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "titles": titles_str,
        "rvprop": "content"
    }
    return params


def get_set_card_gallery_mediawiki_params(set_card_galleries: list[str]) -> dict[str, str]:
    set_card_gallery_list_string = "|".join(set_card_galleries)
    obj = {
        "action": "query",
        "format": "json",
        "prop": "images",
        "titles": set_card_gallery_list_string,
        "imlimit": "500"
    }
    return obj


def get_page_images_from_image_file_mediawiki_params(image_card_urls: list[str]) -> dict:
    image_card_url_list_string = "|".join(image_card_urls)
    obj = {
        "action": "query",
        "format": "json",
        "prop": "pageimages",
        "titles": image_card_url_list_string,
        "pilimit": "50",
        "piprop": "name|original"
    }
    return obj


def card_semantic_search_params(character: str, offset: int, limit: int = 500) -> Dict[str, Any]:
    """
    Generate search parameters for Yugipedia semantic search API.
    """
    return {
        "q": f"[[Page type::Card page]][[Page name::~{character}*]][[Release::Yu-Gi-Oh! Official Card Game]]",
        "p": "format=json",
        "po": (
            "|?Password"
            "|?Card type"
            "|?Level"
            "|?Primary type"
            "|?Type"
            "|?Archetype support"
            "|?Property"
            "|?Lore"
            "|?Attribute"
            "|?ATK"
            "|?ATK string"
            "|?DEF string"
            "|?DEF"
            "|?Link Arrows"
            "|?Link Rating"
            "|?Materials"
            "|?Archseries"
            "|?Pendulum Scale"
            "|?Pendulum Effect"
            "|?Rank"
            "|?English name"
            "|?Page name"
            "|?OCG status"
            "|?Modification date"
            "|?Card image name"
            "|?Class 1"
            "|?Release"
        ),
        "title": "Special:Ask",
        "order": "asc",
        "offset": offset,
        "limit": limit,
        "eq": "yes",
        "link": "none"
    }


def card_semantic_search_params_v2(character: str, offset: int, limit: int = 500) -> Dict[str, Any]:
    """
    Generate search parameters for Yugipedia MediaWiki API.
    """
    return {
        "action": "query",                   # Action for querying the API
        "format": "json",                    # Desired response format
        "list": "search",                    # Perform a search query
        "srsearch": f'[[Page type::Card page]][[Page name::~{character}*]][[Release::Yu-Gi-Oh! Official Card Game]]',
        # Search within the main namespace (0 = articles)
        "srnamespace": "0",
        "srlimit": limit,                    # Limit the number of results returned
        "sroffset": offset,                  # Offset for pagination
        "prop": "info|pageimages|extracts",   # Retrieve page info, image, and extract
        "inprop": "url",                     # Include the URL in the result
        "pithumbsize": 100,                  # Thumbnail size for images
    }

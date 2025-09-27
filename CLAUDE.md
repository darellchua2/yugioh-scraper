# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Core Commands
- `poetry install` - Install dependencies
- `poetry run pytest` - Run tests (may exit with code 5 if no tests are collected)
- `python app/vscode/ygo_inventory_main.py <action>` - Run main application with different actions

### Main Application Actions
The primary entry point is `app/vscode/ygo_inventory_main.py` which accepts these actions:
- `export_v2` - Export inventory to Excel format
- `upload` - Upload inventory data to MySQL and S3
- `scrape_yugipedia` - Scrape Yugipedia for card data
- `scrape_yugipedia_set_cards` - Scrape set card lists from Yugipedia
- `scrape_bigweb` - Scrape BigWeb for inventory data
- `scrape_yuyutei` - Scrape Yuyutei for inventory data

## Architecture

### Package Structure
- `src/civiltekk_yugioh_scraper/v1/` - Main package directory
  - `models/` - Data models for different sources (yugipedia, ygo, bigweb)
  - `prod/` - Production scrapers and processors
  - `utilities/` - Helper utilities including AWS, logging, and scraping tools
  - `config.py` - Configuration constants and environment variables

### Key Components

#### Data Models (`v1/models/`)
- `ygo_models.py` - Core Yu-Gi-Oh! product data models with TypedDict definitions
- `yugipedia_models.py` - Models for Yugipedia API responses
- `bigweb_models.py` - Models for BigWeb scraping data

#### Production Scripts (`v1/prod/`)
- `ygo_inventory_upload.py` - Handles inventory data upload to MySQL/S3
- `ygo_inventory_export.py` - Excel export functionality
- `card_list_scraper.py` - Scrapes card lists from various sources
- `bigwebscrape.py` - BigWeb inventory scraper
- `yuyuteiscrape2.py` - Yuyutei inventory scraper
- `tcgcorner_scraper.py` - TCG Corner scraper

#### Utilities (`v1/utilities/`)
- `aws_utilities.py` - S3 and MySQL database operations
- `misc_utilities.py` - General utility functions
- `yugipedia_utilities.py` - Yugipedia API interaction utilities
- `yugipedia/` - Specialized Yugipedia scrapers for sets, cards, rarities
- `logger.py` - Logging configuration

### Data Flow
1. **Scraping** - Various scrapers collect data from Yugipedia, BigWeb, Yuyutei
2. **Processing** - Data is normalized using models and utilities
3. **Storage** - Data is saved to MySQL database and/or S3 bucket
4. **Export** - Processed data can be exported to Excel format

### Configuration
- Environment variables loaded via `python-dotenv`
- Database credentials via `RDS_HOST`, `DB_USER`, `DB_PASSWORD`
- AWS S3 bucket: `yugioh-storage`
- Default file paths configured for Windows and Linux export locations

### Database Tables
- `yugioh_cards2` - Card data
- `yugioh_sets3` - Set information
- `yugioh_rarities3` - Rarity data
- `overall_card_code_list2` - Card code mappings
- `ygo_inventory_data` - Inventory data

## Dependencies
Uses Poetry for dependency management. Key dependencies include:
- `beautifulsoup4` & `lxml` - Web scraping
- `pandas` - Data processing
- `sqlalchemy` & `pymysql` - Database operations
- `boto3` - AWS S3 operations
- `requests` - HTTP requests
- `mwparserfromhell` - MediaWiki parsing for Yugipedia
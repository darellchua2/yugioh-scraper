from sqlalchemy import (
    create_engine, Float, String, Boolean, DateTime, Integer
)
from sqlalchemy.orm import (
    DeclarativeBase, Mapped, mapped_column, sessionmaker, Session as SessionType
)
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from typing import Optional, List, Dict, Any

# ----------------------------
# DATABASE CONFIGURATION
# ----------------------------

# 🔁 Replace with your actual DB URL
DATABASE_URL = 'mysql+pymysql://user:password@host/dbname'

engine = create_engine(DATABASE_URL, echo=False)
Session = sessionmaker(bind=engine)


# ----------------------------
# BASE MODEL
# ----------------------------

class Base(DeclarativeBase):
    """Base class for all models."""
    pass


# ----------------------------
# DATA MODELS
# ----------------------------

class CardPriceLatest(Base):
    """Stores the most recent pricing per card/version."""
    __tablename__ = 'card_prices_latest'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    set_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    card_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    rarity_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    price: Mapped[float] = mapped_column(nullable=False)
    currency: Mapped[str] = mapped_column(nullable=False)
    source: Mapped[str] = mapped_column(nullable=False)
    region: Mapped[Optional[str]] = mapped_column(nullable=True)
    is_alternate_artwork: Mapped[bool] = mapped_column(
        default=False, nullable=False)
    set_card_name_combined: Mapped[str] = mapped_column(nullable=False)
    updated_on: Mapped[datetime] = mapped_column(
        default=datetime.utcnow, nullable=False)
    source_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    source_id: Mapped[Optional[str]] = mapped_column(nullable=True)
    set_card_code: Mapped[Optional[str]] = mapped_column(nullable=True)
    set_card_code_combined: Mapped[str] = mapped_column(
        nullable=False, unique=True)


class CardPriceHistory(Base):
    """Stores all historical price entries."""
    __tablename__ = 'card_prices_history'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    set_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    card_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    rarity_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    price: Mapped[float] = mapped_column(nullable=False)
    currency: Mapped[str] = mapped_column(nullable=False)
    source: Mapped[str] = mapped_column(nullable=False)
    region: Mapped[Optional[str]] = mapped_column(nullable=True)
    is_alternate_artwork: Mapped[bool] = mapped_column(
        default=False, nullable=False)
    set_card_name_combined: Mapped[str] = mapped_column(nullable=False)
    updated_on: Mapped[datetime] = mapped_column(
        default=datetime.utcnow, nullable=False)
    source_name: Mapped[Optional[str]] = mapped_column(nullable=True)
    source_id: Mapped[Optional[str]] = mapped_column(nullable=True)
    set_card_code: Mapped[Optional[str]] = mapped_column(nullable=True)
    set_card_code_combined: Mapped[str] = mapped_column(nullable=False)


# ----------------------------
# UTILITY FUNCTIONS
# ----------------------------

def build_set_card_code_combined(base_code: Optional[str], is_alt: bool) -> str:
    """
    Generates a unique combined card code to identify alternate artworks or variants.

    Args:
        base_code (Optional[str]): Base set_card_code from API.
        is_alt (bool): Whether it's an alternate artwork.

    Returns:
        str: Normalized unique card code.
    """
    if not base_code:
        return "UNKNOWN"
    return f"{base_code}-ALT" if is_alt else base_code


# ----------------------------
# MAIN SAVE FUNCTION
# ----------------------------

def save_card_data(cards: List[Dict[str, Any]]) -> None:
    """
    Saves card pricing data to both 'latest' and 'history' tables.

    Args:
        cards (List[Dict[str, Any]]): List of card dictionaries.
    """
    session: SessionType = Session()
    now = datetime.utcnow()

    try:
        for item in cards:
            is_alt: bool = item.get("is_alternate_artwork", False)
            set_card_code: Optional[str] = item.get("set_card_code")
            set_card_code_combined: str = build_set_card_code_combined(
                set_card_code, is_alt)

            # --- History entry ---
            history_entry = CardPriceHistory(
                set_name=item.get("set_name"),
                card_name=item.get("set_card_name_combined"),
                rarity_name=item.get("rarity_name"),
                price=item["price"],
                currency=item.get("currency", "SGD"),
                source=item.get("source", "unknown"),
                region=item.get("region"),
                is_alternate_artwork=is_alt,
                set_card_name_combined=item["set_card_name_combined"],
                updated_on=now,
                source_name=item.get(
                    "source_name", item.get("set_card_name_combined")),
                source_id=item.get("source_id"),
                set_card_code=set_card_code,
                set_card_code_combined=set_card_code_combined
            )
            session.add(history_entry)

            # --- Upsert to latest ---
            existing = session.query(CardPriceLatest).filter_by(
                set_card_code_combined=set_card_code_combined,
                source=item.get("source", "unknown")
            ).first()

            if existing:
                existing.set_name = item.get("set_name")
                existing.card_name = item.get("set_card_name_combined")
                existing.rarity_name = item.get("rarity_name")
                existing.price = item["price"]
                existing.currency = item.get("currency", "SGD")
                existing.region = item.get("region")
                existing.is_alternate_artwork = is_alt
                existing.set_card_name_combined = item["set_card_name_combined"]
                existing.updated_on = now
                existing.source_name = item.get(
                    "source_name", item.get("set_card_name_combined"))
                existing.source_id = item.get("source_id")
                existing.set_card_code = set_card_code
            else:
                latest_entry = CardPriceLatest(
                    set_name=item.get("set_name"),
                    card_name=item.get("set_card_name_combined"),
                    rarity_name=item.get("rarity_name"),
                    price=item["price"],
                    currency=item.get("currency", "SGD"),
                    source=item.get("source", "unknown"),
                    region=item.get("region"),
                    is_alternate_artwork=is_alt,
                    set_card_name_combined=item["set_card_name_combined"],
                    updated_on=now,
                    source_name=item.get(
                        "source_name", item.get("set_card_name_combined")),
                    source_id=item.get("source_id"),
                    set_card_code=set_card_code,
                    set_card_code_combined=set_card_code_combined
                )
                session.add(latest_entry)

        session.commit()
        print("✅ Card data saved to both latest and history tables.")

    except SQLAlchemyError as e:
        session.rollback()
        print(f"❌ Error while saving card data: {e}")

    finally:
        session.close()


# ----------------------------
# INIT TABLES
# ----------------------------

def init_tables() -> None:
    """
    Initializes the database tables if they don't already exist.
    """
    try:
        Base.metadata.create_all(engine)
        print("✅ Tables created or verified.")
    except SQLAlchemyError as e:
        print(f"❌ Table creation failed: {e}")


# ----------------------------
# MAIN ENTRY POINT
# ----------------------------

if __name__ == '__main__':
    init_tables()

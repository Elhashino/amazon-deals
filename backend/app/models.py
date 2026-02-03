from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy import String, Float, Integer, BigInteger, DateTime, Boolean, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .db import Base


class Product(Base):
    __tablename__ = "products"

    asin: Mapped[str] = mapped_column(String(10), primary_key=True)

    title: Mapped[str] = mapped_column(String(600), default="")
    brand: Mapped[str] = mapped_column(String(200), default="")

    # IMPORTANT: DB has NOT NULL constraint on image_url, so keep a safe default.
    image_url: Mapped[str] = mapped_column(String(600), default="")

    root_category_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    root_category_name: Mapped[str] = mapped_column(String(200), default="")

    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    snapshots: Mapped[list["PriceSnapshot"]] = relationship(
        back_populates="product", cascade="all, delete-orphan"
    )
    deals: Mapped[list["Deal"]] = relationship(
        back_populates="product", cascade="all, delete-orphan"
    )


class PriceSnapshot(Base):
    __tablename__ = "price_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asin: Mapped[str] = mapped_column(String(10), ForeignKey("products.asin"), index=True)

    # IMPORTANT: your model uses captured_at (NOT taken_at)
    captured_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    price_current: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_median_90d: Mapped[float | None] = mapped_column(Float, nullable=True)
    discount_pct_90d: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)

    product: Mapped["Product"] = relationship(back_populates="snapshots")


class Deal(Base):
    __tablename__ = "deals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asin: Mapped[str] = mapped_column(String(10), ForeignKey("products.asin"), index=True)

    category_slug: Mapped[str] = mapped_column(String(32), index=True)

    # DB column: timestamp without time zone
    published_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    price_current: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_median_90d: Mapped[float | None] = mapped_column(Float, nullable=True)
    discount_pct_90d: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Demand signals (derived from Keepa data)
    sales_rank_current: Mapped[float | None] = mapped_column(Float, nullable=True)
    sales_rank_median_30d: Mapped[float | None] = mapped_column(Float, nullable=True)
    sales_rank_trend_30d: Mapped[float | None] = mapped_column(Float, nullable=True)
    rank_drops_7d: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    review_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    demand_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    hot_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)

    # DB column: timestamp with time zone
    ingested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    product: Mapped["Product"] = relationship(back_populates="deals")


Index("ix_deals_active_cat_score", Deal.is_active, Deal.category_slug, Deal.score)
Index("ix_deals_active_cat_hot", Deal.is_active, Deal.category_slug, Deal.hot_score)



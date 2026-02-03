from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Core
    DATABASE_URL: str = "postgresql+psycopg2://deals:deals@localhost:5432/deals"
    KEEPA_API_KEY: str = ""

    # Ingestion controls
    DEALS_PAGES_PER_ROOT_CATEGORY: int = 2

    # Minimum discount thresholds per category slug
    MIN_DISCOUNT_HOME: float = 0.25
    MIN_DISCOUNT_KITCHEN: float = 0.25
    MIN_DISCOUNT_DIY: float = 0.25
    MIN_DISCOUNT_ELECTRICAL: float = 0.25
    MIN_DISCOUNT_TOYS: float = 0.25

    # New slugs you added/changed
    MIN_DISCOUNT_GROCERY: float = 0.25
    MIN_DISCOUNT_HEALTH: float = 0.25
    MIN_DISCOUNT_BEAUTY: float = 0.25
    MIN_DISCOUNT_PET: float = 0.25
    MIN_DISCOUNT_SPORTS: float = 0.25
    MIN_DISCOUNT_BABY: float = 0.25
    MIN_DISCOUNT_AUTOMOTIVE: float = 0.25

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()


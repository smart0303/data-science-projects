import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )


def load_settings() -> Settings:
    return Settings(
        api_base_url=os.getenv(
            "JSONPLACEHOLDER_BASE_URL",
            "https://jsonplaceholder.typicode.com",
        ).rstrip("/"),
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB", "de_portfolio"),
        postgres_user=os.getenv("POSTGRES_USER", "de_user"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "de_password"),
    )

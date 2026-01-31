"""Configuration management for ETL Agent."""

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings."""
    
    # OpenAI Configuration
    openai_api_key: str
    openai_model: str = "gpt-4-turbo-preview"
    temperature: float = 0.7
    
    # GitHub Configuration
    github_token: str
    github_repo: str
    github_base_branch: str = "main"
    
    # Output Configuration
    output_dir: str = "generated"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


def get_settings() -> Settings:
    """Get application settings."""
    return Settings()

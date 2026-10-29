from dataclasses import asdict, astuple, dataclass, field
from pathlib import Path
from unittest.mock import patch

import pytest
from ctaops.utils.config import NOT_SET, CtaOpsConfigLoader
from ctaops.utils.logging import LoggingConfiguration


@dataclass
class DatabaseConfig:
    username: str = NOT_SET
    password: str = NOT_SET
    name: str = NOT_SET
    host: str = NOT_SET
    port: int = NOT_SET
    table: str = NOT_SET


@dataclass
class WorkflowConfig:
    disk_endpoint: str = "ctaeos"
    frontend_endpoint: str = "localhost:10956"
    timeout: int = 30


@dataclass
class AppConfig:
    timeout: int = 10
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    logging: LoggingConfiguration = field(default_factory=LoggingConfiguration)
    workflow: WorkflowConfig = field(default_factory=WorkflowConfig)

    def validate(self) -> "AppConfig":
        print(asdict(self.database))
        if any(v is NOT_SET for v in astuple(self.database)):
            raise ValueError("Database configuration is incomplete")

        return self


@pytest.fixture
def file_config_path() -> Path:
    return Path(__file__).parent / "data" / "config.toml"


def unchecked_config(config: AppConfig) -> AppConfig:
    """Let loading tests inspect partial configs; test validation separately."""
    return config


class TestCtaOpsConfigLoader:
    @pytest.fixture(autouse=True)
    def set_config_loader(self):
        self.config_loader = CtaOpsConfigLoader(AppConfig)

    @patch.object(AppConfig, "validate", unchecked_config)
    def test_load_from_dict(self):
        self.config_loader.load_from_dict(
            {
                "database": {"username": "test_user"},
                "logging": {"file_path": "overide_file"},
                "workflow": {"frontend_endpoint": "grpc.example:10956"},
            }
        )

        assert self.config_loader.get_safe_config() == AppConfig(
            database=DatabaseConfig(username="test_user"),
            logging=LoggingConfiguration(file_path="overide_file"),
            workflow=WorkflowConfig(disk_endpoint="ctaeos", frontend_endpoint="grpc.example:10956"),
        )

    @patch.object(AppConfig, "validate", unchecked_config)
    def test_load_toml(self, file_config_path: Path):
        self.config_loader.load_from_toml(file_config_path, file_exist_error="raise")
        assert self.config_loader.get_safe_config() == AppConfig(
            timeout=2,
            database=DatabaseConfig(password="fake_password"),  # noqa: S106 - TOML fixture value, not a credential.
            logging=LoggingConfiguration(level="DEBUG"),
        )

    def test_get_safe_config(self):
        with pytest.raises(ValueError, match="Database configuration is incomplete"):
            self.config_loader.get_safe_config()

# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Configuration management for CTA operations tools.

This module provides a generic and flexible configuration management system for loading
and managing configuration from TOML files and dictionaries into dataclass instances.

It provides a fluent interface for configuration loading, allowing tools to:
- Load configurations from TOML files with validation
- Load configurations from dictionaries
- Set individual configuration values programmatically
- Validate configurations before use

The module also defines common protocols and utilities used by all configuration
implementations, including the NOT_SET singleton for distinguishing unset values
from None or default values.

Key Classes:
    CtaOpsConfigLoader: Generic configuration loader with fluent API
    DataclassWithValidation: Protocol for validatable dataclasses
    NotSetType: Sentinel value for unset configuration fields

Usage:
    Designed for use by tools (not libraries). Tools should create dataclass
    configuration classes and use CtaOpsConfigLoader to load and validate them.
"""

import os
import warnings
from dataclasses import is_dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, Optional, Protocol, TypeVar

import tomli

if TYPE_CHECKING:
    from typing_extensions import Self


from . import dataclasses as dc


class DataclassWithValidation(Protocol):
    """
    A protocol for dataclasses that support validation.

    This protocol defines the interface for dataclasses that can be validated.
    It requires the dataclass to have `__dataclass_fields__` and a `validate`
    method, which is expected to check the validity of the fields and return
    a validated instance of the dataclass.
    """

    __dataclass_fields__: ClassVar[dict[str, Any]]

    def validate(self) -> "Self": ...


class NotSetType:
    """A singleton class to guarantee only one NOT_SET object exists."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(NotSetType, cls).__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        """
        Return the string representation of the NOT_SET singleton.

        Returns:
            str: The string "NOT_SET".
        """
        return "NOT_SET"


NOT_SET: Any = NotSetType()
"""A singleton value used to represent unset configuration fields. This allows distinguishing between fields that are explicitly set to None
and fields that have not been set at all. The NOT_SET value is used as a default for configuration fields for field where a default hardcoded value has no sense
like password or secret keys."""

T = TypeVar("T", bound=DataclassWithValidation)


class CtaOpsConfigLoader(Generic[T]):
    """
    A generic configuration loader for dataclasses.

    This class provides a fluent interface for loading configuration from various
    sources like TOML files or dictionaries, and for setting individual values.
    It is designed to work with dataclasses that conform to the
    `DataclassWithValidation` protocol.
    """

    def __init__(self, config_class: type[T]):
        """
        Initializes the config loader with a dataclass type.

        Args:
            config_class: The dataclass type to be used for configuration.
                          It must be a dataclass.

        Raises:
            TypeError: If `config_class` is not a dataclass.
        """
        if not is_dataclass(config_class):
            raise TypeError(f"{type(config_class)} must be a dataclass")

        self._config: T = config_class()

    def load_from_toml(
        self, toml_path: Optional[Path], file_exist_error: Literal["raise", "warn", "ignore"] = "raise"
    ) -> "CtaOpsConfigLoader[T]":
        """
        Load configuration from a TOML file.

        If the file does not exist, a warning is issued, and the loader continues
        with default values. If the file is marked as required and does not
        exist, a `FileNotFoundError` is raised.

        Args:
            toml_path: The path to the TOML configuration file.
            file_exist_error: The behavior when the file does not exist.

        Returns:
            The loader instance for method chaining.

        Raises:
            FileNotFoundError: If `file_exist_error` is "raise" and the file does not exist.
        """

        if toml_path is None or not Path(toml_path).exists():
            if file_exist_error == "raise":
                raise FileNotFoundError(f"Config file '{toml_path}' does not exist.")
            elif file_exist_error == "warn":
                warnings.warn(f"Optional config file '{toml_path}' not found; using defaults.", stacklevel=1)
            return self

        with open(toml_path, "rb") as f:
            toml_data = tomli.load(f)

        dc.load_data(self._config, toml_data)

        return self

    def load_from_dict(self, raw_dict: dict[str, Any]) -> "CtaOpsConfigLoader[T]":
        """
        Load configuration from a dictionary.

        Args:
            raw_dict: A dictionary containing configuration data.

        Returns:
            The loader instance for method chaining.
        """
        dc.load_data(self._config, raw_dict)
        return self

    def set(self, *keys, value) -> "CtaOpsConfigLoader[T]":
        """
        Set a configuration value for a nested key.

        This method allows setting a value deep within the configuration structure
        by providing a sequence of keys. If the value is `None`, the method does
        nothing.

        Example:
            ```python
            loader.set("database", "host", value="localhost") # is equivalent to
            loader.load_from_dict({"database": {"host": "localhost"}})
            ```

        Args:
            *keys: A sequence of keys representing the path to the value.
            value: The value to set.

        Returns:
            The loader instance for method chaining.
        """
        if value is None:
            return self
        top_level_dic = {}
        dic = top_level_dic
        for key in keys[:-1]:
            dic[key] = {}
            dic = dic[key]
        dic[keys[-1]] = value
        dc.load_data(self._config, top_level_dic)

        return self

    def get_safe_config(self) -> T:
        """
        Returns the validated configuration object.

        This method calls the `validate()` method on the configuration object,
        which is expected to perform all necessary validation checks, such as
        ensuring all required fields are set.

        Returns:
            The validated configuration object.
        """

        return self._config.validate()

    @staticmethod
    def find_default_config_file(
        project_name: str,
        file_name: str,
    ) -> Optional[Path]:
        files: list[Path] = []

        if (xdg_config_home := os.environ.get("XDG_CONFIG_HOME")) is not None:
            files.append(Path(xdg_config_home) / project_name / file_name)
        if (home := os.environ.get("HOME")) is not None:
            files.append(Path(home) / ".config" / project_name / file_name)

        files.append(Path(f"/etc/{project_name}/") / file_name)

        return next((f for f in files if f.exists()), None)

# Overview

`ctaops-utils` is a foundational utility library that provides consistent configuration management, logging, and dataclass utilities for every CTA operations tool and application.

It is the shared dependency that ensures every CTA ops package — `ctaops-cli`, `ctaops-sdk`, the upcoming apps, and your own scripts — agrees on how configuration is loaded, how logs are formatted and rotated, and how nested dataclass structures are introspected and serialized.

**Key features:**

- 🔧 **Configuration management** — type-safe, layered loading from TOML files and dictionaries with structured validation.
- 📝 **Logging** — rotating, optionally compressed log files with UNIX group ownership support.
- 📦 **Dataclass utilities** — recursive loading and dot-notation flattening of nested dataclass structures.
- 🔤 **Type utilities** — normalisation and introspection of `Optional`, `Union`, `Literal`, `Annotated`, …

## Layout

The library is organised in four small, independent submodules:

| Submodule | Purpose | Headline API |
| --- | --- | --- |
| `ctaops.utils.config` | Layered configuration loader | `CtaOpsConfigLoader` |
| `ctaops.utils.logging` | Stdlib-logging configuration | `LoggingConfiguration`, `configure_logging` |
| `ctaops.utils.dataclasses` | Nested-dataclass helpers | `load_data`, `flatten_dataclass_instance`, `flatten_dataclass_class` |
| `ctaops.utils.typing` | Type-annotation normalisation | `normalize_types` |

> Every submodule is independently usable; you can pull in only what you need.

## Installation

!!! abstract "Command"
    ```bash
    python3 -m pip install ctaops-utils \
        --extra-index-url https://cta-public-repo.web.cern.ch/cta-operations/stable/pypi/simple/
    ```

## Type hints

The package is fully type-annotated and ships a `py.typed` marker so static checkers like [pyright](https://microsoft.github.io/pyright/) and [mypy](https://mypy-lang.org/) pick up its types out of the box.

## Usage

Please see the [API Usage](api-usage.md) page.

## License

GPL-3.0-or-later Copyright © 2026 CERN

# Overview

`ctaops-utils` provides shared configuration, logging, dataclass, and type utilities for CTA tools and scripts.
It supports layered TOML configuration, rotating log files with compression and group ownership, and loading and flattening nested dataclasses.

## Layout

The library provides these submodules:

| Submodule | Purpose | API |
| --- | --- | --- |
| `ctaops.utils.config` | Layered configuration loader | `CtaOpsConfigLoader` |
| `ctaops.utils.logging` | Stdlib-logging configuration | `LoggingConfiguration`, `configure_logging` |
| `ctaops.utils.dataclasses` | Nested-dataclass helpers | `load_data`, `flatten_dataclass_instance`, `flatten_dataclass_class` |
| `ctaops.utils.typing` | Type-annotation normalisation | `normalize_types` |

Each submodule can be used independently.

## Installation

!!! abstract "Command"
    ```bash
    python3 -m pip install ctaops-utils \
        --extra-index-url https://cta-public-repo.web.cern.ch/cta-operations/stable/pypi/simple/
    ```

## Type hints

The package includes type annotations and a `py.typed` marker for static type checkers.

## Usage

See [Usage](api-usage.md) for examples.

## License

GPL-3.0-or-later Copyright © 2026 CERN

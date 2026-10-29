# Python Libraries and Tools

CTA's Python workspace contains libraries for scripting CTA and operating tape hardware, plus the `ctaops` command-line tool.

| Package | Source directory | Purpose |
| --- | --- | --- |
| [cta-sdk](cta-sdk/index.md) | `lib/python/cta-sdk` | Typed CTA client, resource models and backend interfaces. |
| [ctaops-sdk](ctaops-sdk/index.md) | `lib/python/ctaops-sdk` | Local tape-drive and library discovery and control. |
| [ctaops-utils](ctaops-utils/index.md) | `lib/python/ctaops-utils` | Configuration, logging and dataclass utilities. |
| [ctaops-cli](../../../ops/tools/ctaops.md) | `tools/ctaops-cli` | Operator commands built on these libraries. |

See the [Python API reference](../../../api/python/) for classes and functions, [Extending ctaops](ctaops-cli.md) to add commands, and [ctaops-udev](../../../ops/deploy-and-configure/deployment/ctaops-udev.md) for hardware discovery setup.

## Development environment

Run from the repository root:

```bash
git submodule update --init lib/protobuf/external/xrootd-ssi-protobuf-interface
uv sync --locked --all-packages --all-extras --all-groups --python 3.12
uv run --no-sync ctaops --help
```

Dependencies and tool configuration are managed by the root `pyproject.toml` and `uv.lock`. Package versions come from CTA Git tags; SDK builds generate protobuf bindings from the submodule.

## Checks and packaging

```bash
uv run --no-sync pytest
uv run --no-sync ruff check .
uv run --no-sync ruff format . --check
uv run --no-sync pyright --pythonpath .venv/bin/python
uv build --all-packages --out-dir build/python
```

Use Google-style docstrings for API documentation.
See [Documentation Changes](../../contributing/documentation.md) for build instructions.

## Hardware compatibility

The hardware tools target IBM TS4500 and Spectra Logic TFinity libraries, IBM 3592 and LTO media, and their corresponding drives. Other hardware may need additional implementation and validation. See the [CTA community forum](https://cta-community.web.cern.ch/) for discussion and support.

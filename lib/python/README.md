# Python workspace

Shared Python libraries for CTA and [ctaops-cli](../../tools/ctaops-cli/README.md).
Dependencies and tool configuration are managed in the root [pyproject.toml](../../pyproject.toml).

## Setup

Run from the repository root:

```sh
git submodule update --init lib/protobuf/external/xrootd-ssi-protobuf-interface
uv sync --locked --all-packages --all-extras --all-groups --python 3.12
```

## Development

```sh
uv run --no-sync pytest
uv run --no-sync ruff check .
uv run --no-sync ruff format . --check
uv run --no-sync pyright --pythonpath .venv/bin/python
uv run --no-sync ctaops --help
uv build --all-packages --out-dir build/python
```

- System tests run separately; see [ci/system_tests](../../ci/system_tests/README.md).
- Keep existing requirements files and dependency groups in sync, then run `uv lock`.
- Package versions come from CTA Git tags through the shared [version helper](../../ci/utils/cta_version.py).
  Hatch builds the packages; explicit CI release tags supply release versions.
  Local builds always use `<major>.dev1`; CI development builds use `<major>.dev<pipeline>+g<sha>[.dirty]`.
- SDK builds generate protobuf bindings from the submodule above.
- `ctaops-udev` is packaged separately as an RPM.

Imported from [cta-operations-utilities](https://gitlab.cern.ch/cta/cta-operations-utilities) at commit `af96ab9dc8893a1d2e5f988ad8da9f6f7fa05c1a`.

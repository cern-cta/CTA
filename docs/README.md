# CTA documentation

Published at <https://cta.docs.cern.ch/>.

## Preview locally

From the repository root, with [`uv`](https://docs.astral.sh/uv/), Python 3.12 and the Rust toolchain installed:

```bash
docs/serve.sh  # add --skip-rust to reuse target/doc when editing prose
```

Then open <http://127.0.0.1:8000/>. Run `docs/.venv/bin/python docs/build.py build` for a strict one-off build into `build/docs/site/`.

See [Documentation Changes](content/dev/contributing/documentation.md) for local preview and build instructions and content organization.

For generated references, publishing status, and theme maintenance, see [Documentation Site](content/dev/contributing/maintainers/documentation-site.md).

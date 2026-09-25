# CTA documentation

Published at <https://cta.docs.cern.ch/>. Edit pages in `content/` alongside code changes in the same merge request.

## Preview locally

From the CTA repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r docs/requirements.txt
mkdocs serve --config-file docs/mkdocs.yml
```

Open <http://127.0.0.1:8000/>.
You can also run `mkdocs serve` from `docs/` with the environment activated.
To check the build:

```bash
mkdocs build --strict --config-file docs/mkdocs.yml
```

## How it works

MkDocs Material builds the site, including manpages and example configurations from the current checkout.
Dependencies are maintained in `requirements.txt`; generated files go into `build/docs/`.

Shared branding images and favicons live in `content/assets/images/`; the repository README also uses these assets. Keep `overrides/` for templates.

The theme in `content/stylesheets/extra.css` follows the main CTA website's colors and theme.

Pages with ambiguous navigation labels use an explicit `title` in their Markdown
front matter for browser tabs. Keep the short navigation labels in `mkdocs.yml`.
The header override keeps the Documentation identity visible while scrolling.
The footer override uses a local CERN logo and compact resource links consistent
with the project website, retaining the Material credit and omitting a self-link.

CI builds relevant changes and tags, and publishes only in tag pipelines. Publishing happens to the protected `gl-pages` branch.
Publication uses `CI_JOB_TOKEN`; repository pushes must be enabled for job tokens, and the triggering user must be allowed to push to `gl-pages` (maintainer or higher).
Generally speaking, this `gl-pages` branch should never be updated manually.

Mike stores the versioned site on `gl-pages`: `v6.12.0.0-1` updates `6.12`, and `latest` points to the highest published series.
Older releases cannot overwrite newer documentation; prerelease and variant tags do not publish. CI helpers live in [`ci/docs/`](../ci/docs/).

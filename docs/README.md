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
Restart `mkdocs serve` after changing `hooks.py`: MkDocs caches imported hooks,
so live reload does not pick up their Python changes. This includes the hook
that removes man-page metadata from the website output.
To check the build:

```bash
mkdocs build --strict --config-file docs/mkdocs.yml
```

## How it works

MkDocs Material builds the site, including manpages and example configurations from the current checkout.
Dependencies are maintained in `requirements.txt`; generated files go into `build/docs/`.

Shared branding images and favicons live in `content/assets/images/`; the repository README also uses these assets. Keep `overrides/` for templates.

Navigation groups shared Concepts, Operations, Development, and Release Notes.
Page paths follow those audiences under `concepts/`, `ops/`, and `dev/`.
Use lowercase hyphenated names and `index.md` for section landing pages.
Use descriptive component names for guides and executable names for command references.
Keep related diagrams beside their pages and update relative links when moving them.
Navigation scope comments in `mkdocs.yml` define where each topic belongs.
Conventions belong to Development; the glossary belongs to Concepts. Navigation
labels and groups can change independently of paths, but moving a page changes its URL.
Administration groups operator procedures and outlines for missing topics.

Release Notes includes the root `CHANGELOG.md` from the current checkout through
the existing snippets extension. Edit that source file rather than maintaining a
second release history. The page hook limits its sidebar to release headings.

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
Older releases cannot overwrite newer documentation; prerelease and variant tags do not publish. CI helpers live in [`ci/docs/`](../ci/docs).

## Content boundaries

- **Concepts** explains CTA responsibilities, terminology, component roles, and file lifecycles. It distinguishes CTA's tape metadata from the disk system's namespace and disk-copy policies.
- **Operations** contains installation, configuration, administration, monitoring, upgrades, and recovery. An operator must not need Development to complete a procedure.
- **Development** contains source structure, implementation, interfaces, contribution rules, tests, and maintainer procedures. Shared explanations should link back to Concepts.

Keep the shared part of each page independent of the disk system. Put system-specific behavior under an `EOS` or `dCache` heading, or in a page nested beneath that system in navigation. Concrete examples may name a system; label that scope explicitly. Preserve literal API/configuration names such as `eos.instance` when those are actual identifiers.

Documentation describes the selected CTA release. Remove historical feature-introduction qualifiers and comparisons with older CTA releases. Retain dependency compatibility requirements, schema migration examples, protocol/format versions, and literal package/configuration identifiers where they affect meaning. Release history stays in the root changelog.

New skeleton pages use a **Documentation outline** notice and describe the intended coverage of each section. They do not claim that procedures are complete. Existing deprecated material moved between sections retains its review warning until the content pass validates it. The old developer buffer-cleanup URL is retained as a pointer to its Operations page.

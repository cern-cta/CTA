# CTA documentation

The public operator and developer documentation lives in this repository and is published at <https://cta.docs.cern.ch/>.
Update documentation alongside the feature or fix in the same merge request.

## Import provenance

Imported as a snapshot from <https://gitlab.cern.ch/cta/eoscta-docs>, commit `66ececd3cbf40b51b7a92a6d7661574425f5f7ed`.
The original repository retains historical blame and the deprecated material excluded from this import; archive it after the hosting cutover.
The existing `content/overview/components/db_schema.svg` is a snapshot from that import; builds do not download a newer diagram.
A later change can regenerate it from the catalogue schema revision pinned by CTA.

## Build and preview locally

Use Python 3.9, matching the AlmaLinux 9 default CI image, and Git.
Run the following from the CTA repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r docs/requirements.txt
mkdocs serve --config-file docs/mkdocs.yml
```

After activating the environment, you can also run `mkdocs serve` directly from `docs/`.
Snippet paths are anchored to the repository location, regardless of your working directory.

Open <http://127.0.0.1:8000/>.
For the strict CI build and publication tests:

```bash
mkdocs build --strict --config-file docs/mkdocs.yml
python -m unittest discover --start-directory docs/scripts --pattern 'test_*.py'
```

Generated files live beneath the ignored `build/docs/` directory.
No CTA compilation, submodules, publishing credentials, or `gl-pages` branch are needed to build locally.
The version selector is populated when mike publishes; the ordinary local preview contains only the current checkout.

`content/` holds Markdown and static assets; `includes/` holds shared snippets; `overrides/` holds theme customizations.
Snippet paths are relative to the CTA repository root and missing snippets fail the build.
The MkDocs hook runs the existing lightweight `tools/compile_man_md.py` generator for the `cta-admin` manpage.
Other manpages and configuration examples are included directly from the checkout.
Two removed standalone tools retain their page URLs with notices instead of silently empty snippets.

## Dependencies and CI

Direct dependencies are listed in `requirements.in`; `requirements.txt` locks their transitive dependencies for Python 3.9.
Update the lock with:

```bash
uv pip compile --python 3.9 docs/requirements.in --output-file docs/requirements.txt
```

The existing default CI image installs the lock into `/opt/docs`, isolated from its other Python utilities.
Documentation jobs activate that environment.
If the checkout's lock differs, or the image predates this migration, the job creates a fresh environment from the checkout's lock.
Thus a dependency change is tested immediately, without waiting for the pipeline-image refresh.
The usual image refresh bakes the updated lock into the next default image.

`build-docs` validates relevant changes in MR and default-branch push pipelines, every tag, and manually started normal pipelines.
It skips child variant, regression, scheduled, and image-refresh pipelines.
HTML artifacts remain available for seven days.
When adding a new external snippet or generator input, extend both the job's change paths and MkDocs watch paths.
Existing CTA compilation/test job selection is unchanged.

## Release publication

`publish-docs` runs for protected canonical release tags after documentation validation, successful internal RPM publication, and all private tagged image publications.
The existing manual internal RPM publication remains the release gate; no additional manual docs action is needed.
Its success-only artifact prevents publication if that allow-failure job fails.
Public RPM channels and optional public image publications are not dependencies.

Canonical tags have the form `vN.N.N-N` or `vN.N.N.N-N`.
The first two numbers identify the documentation series: both `v6.12.0-1` and `v6.12.0.0-1` publish `6.12`.
Release candidates and scheduler/catalogue variant tags build without publishing.
Documentation-only corrections become public on the next canonical tag; old tags are never changed or backfilled automatically.

Each numeric series records `source_tag` and `source_commit` in mike's `public/versions.json` properties.
Only the highest release within that series can replace its content, with package revisions compared numerically.
A three-component tag is compared as though its fourth component were zero.
Retrying the same tag and commit is supported; moving that release to another commit is rejected.
A numeric series without provenance must be investigated and explicitly repaired before it can be overwritten.
Historical `v4` and `v5` are preserved as imported and do not require new metadata.

The `latest` redirect points to the highest numeric series, including when an older maintenance series is updated.
The site root redirects to `latest`.
Publication fetches `gl-pages` inside the `cta-docs-publication` resource group, performs the mike operations locally, and pushes once without force.
Push conflicts fail rather than losing other publications.
The complete `public/` tree is exported for GitLab Pages, including on an older-tag retry.
The branch must already exist and contain `public/versions.json`; this migration does not create or transfer it.

## Hosting cutover checklist

1. Transfer the existing `gl-pages` branch separately, retaining its complete `public/` tree and historical versions.
2. Refresh the default CI image through the existing pipeline-image workflow; until then, jobs install the lock into a fresh environment.
3. Configure a masked, protected `DOCS_PUBLISH_TOKEN` with repository write access, and allow its identity to push `gl-pages`.
   Protect canonical release tags so they receive the token and match the publication rule.
4. Disable the old repository's publisher before activating CTA publication, and associate CTA's GitLab Pages deployment with the existing `cta.docs.cern.ch` hosting configuration.
5. Publish the next canonical release through the existing internal RPM/image release flow.
   Check the Pages deployment, root redirect, version selector, historical URLs, configuration examples, and source provenance.
6. Add a relocation notice to the old repository and archive it after the first successful CTA publication.
   Retain its history and disable obsolete automation and credentials.

Do not delete historical versions or force-push the publication branch during cutover.
To recover a publication problem, preserve a copy of `gl-pages` and retry the same release pipeline after fixing the cause.
For a content rollback, revert the source change and publish a newer canonical release rather than deploying an older tag over newer documentation.

## Future API references

Reserve version-relative `api/cpp/`, `api/python/`, and `api/rust/` destinations.
No generators or empty navigation entries are enabled yet.
Doxygen HTML is the initial C++ option, with MkDoxy available for a separate integration trial; mkdocstrings can render Python references and `cargo doc` can provide Rust HTML.
Generate references from the same checkout as the surrounding documentation in both validation and publication.
Keep generated output outside source control and extend CI input detection when these generators are introduced.

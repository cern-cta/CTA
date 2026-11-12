# Documentation Site

This page covers site configuration and publication. For page edits, local preview, and validation, see [Documentation Changes](../documentation.md).

## Site configuration

MkDocs Material builds the site. Paths below are relative to `docs/` unless stated otherwise.

| Location | Purpose |
| --- | --- |
| `mkdocs.yml` | Site configuration and navigation |
| `requirements.txt` | Python dependencies |
| `content/stylesheets/extra.css`, `overrides/` | Shared styling and template overrides |
| `content/assets/images/` | Shared images and favicons |
| `hooks.py` | Generated content and include handling |
| `content/api/python/` | Python API reference module pages |
| `ci/docs/` (repository root) | Publication scripts and tests |

Restart `mkdocs serve` after changing hooks; live reload does not reload the Python module.

### Generated references

The build generates the Python reference with `mkdocstrings` and the Rust reference with `cargo doc`.
Both are included in each published documentation version under `api/python/` and `api/rust/`.

Keep `requirements.txt` and the repository root’s `pyproject.toml` docs group synchronized, and update `uv.lock` after dependency changes.

### Diagram colors

Mermaid colors are configured in `content/stylesheets/extra.css`. Prefer the default theme colors and check diagrams in both light and dark mode after styling changes.

### Fonts

Self-hosted fonts and their licenses live in `content/assets/fonts/`, sourced from:

- [Atkinson Hyperlegible Next](https://github.com/googlefonts/atkinson-hyperlegible-next/tree/7925f50f649b3813257faf2f4c0b381011f434f1/fonts/webfonts)
- [Atkinson Hyperlegible Mono](https://github.com/googlefonts/atkinson-hyperlegible-next-mono/tree/154d50362016cc3e873eb21d242cd0772384c8f9/fonts/webfonts)

## Publishing

`publish-docs` runs automatically for protected canonical release tags after `build-docs` succeeds. It runs independently of RPM and image publication, so documentation may appear before the software is published. Development and release-candidate tags do not publish documentation.

Publication uses `CI_JOB_TOKEN` to push to the protected `gl-pages` branch. Repository pushes must be enabled for job tokens, and the triggering user must have permission to push to that branch (maintainer or higher). Use the publication workflow rather than editing `gl-pages` manually.

Each tag updates its minor-series documentation: for example, `v6.1.0` updates `6.1`. The `latest` alias points to the highest published series. After publication, check the affected version and `latest` on the [public site](https://cta.docs.cern.ch/).

If publication fails, inspect the `publish-docs` log and resolve the cause, such as missing push permissions. Retry the job in the original tag pipeline when the source contents are unchanged. If a source fix is needed, use a new tag; do not move the existing tag. Check the published version after the retry.

See [Release Procedure](releases.md) for the surrounding release workflow.

## Documentation backports

Documentation corrections for an older minor series can be published without releasing new RPMs or container images:

1. Backport the correction through a reviewed MR based on the appropriate release source. Keep included manuals and configurations consistent with that series.
2. Create a new protected canonical release tag on the corrected commit, higher than the tag currently published for that series. For example, if `6.9` was published from `v6.9.2`, use an unused patch version such as `v6.9.3`. For this documentation-only update, create the tag directly with Git on the reviewed commit (replace the example version and SHA):

    ```bash
    git tag -a v6.9.3 REVIEWED_COMMIT_SHA -m "Documentation update for CTA 6.9"
    git push origin refs/tags/v6.9.3
    ```

    Use the CERN GitLab remote and ensure the new tag matches the project’s protection rules. Do not move existing tags; older versions cannot replace newer published documentation, and reuse of a version for a different commit is rejected.
3. Let `build-docs` and `publish-docs` run. Leave `internal-release-cta` and the software publication jobs untriggered.
4. Verify the updated series on the public site. Updating an older series leaves `latest` pointing to the highest published series.

!!! note "Current limitation"

    Documentation-only updates still require a software release tag and consume a release version. Coordinate the version with release maintainers. Software build and test jobs may still run, but RPM and image publication is not required.

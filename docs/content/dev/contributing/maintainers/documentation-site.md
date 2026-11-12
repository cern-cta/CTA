# Documentation Site

This page covers site configuration and publication. For page edits, local preview, and validation, see [Documentation Changes](../documentation.md).

## Site configuration

Zensical builds the site with Python 3.12. Paths below are relative to the repository root.

| Location | Purpose |
| --- | --- |
| `zensical.toml` | Site configuration, navigation, Markdown extensions, and theme |
| `pyproject.toml`, `uv.lock` | Shared dependency declarations, cooldown policy, and lockfile; docs use the `docs` group |
| `docs/.python-version`, `docs/.venv/` | Python 3.12 selection and separate docs environment |
| `docs/build.py` | Generate the command manual and Rust reference, then invoke Zensical |
| `docs/zensical_content.py` | Repository substitutions, manual metadata stripping, and diagram defaults |
| `docs/content/stylesheets/extra.css`, `docs/overrides/` | CTA branding |
| `docs/content/api/python/` | Python API reference module pages |

Restart the preview after changing the build helper, Markdown extension, or generated-reference inputs. Markdown, stylesheet, and template edits reload automatically.

For environment setup, see [Preview locally](../documentation.md#preview-locally). Update the `docs` dependency group in `pyproject.toml` and regenerate `uv.lock` with `uv lock`.

### Generated references

`docs/build.py` generates the command manual in `build/docs/generated/` and runs `cargo doc`. It copies Rust output into the ignored `docs/content/api/rust/` directory so Zensical includes it in both builds and previews. `--skip-rust` reuses existing `target/doc/` output; it still regenerates the manual.

The Python API reference uses `mkdocstrings`. Its Python 3.9 inventory matches the supported CTA API, independently of the docs builder’s Python 3.12 runtime.

The final site is written to `build/docs/site/`. Generated references must be prepared before invoking Zensical directly.

### Theme

Theme customizations live in `docs/content/stylesheets/extra.css` and `docs/overrides/`. Self-hosted fonts and their licenses are under `docs/content/assets/fonts/`. Keep template overrides small so upstream theme updates remain easy to adopt.

### Diagram colors

Mermaid colors are defined in `docs/content/stylesheets/extra.css`. The Markdown extension sets sequence arrowheads to follow the selected palette. Check diagrams in both light and dark modes after changes.

## Publishing

!!! warning "CI migration pending"

    The existing build and publication jobs still invoke MkDocs and `mike` and are not compatible with this setup. They are intentionally left for a separate migration. The workflow below documents the legacy publication behavior, not a supported Zensical deployment command.

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

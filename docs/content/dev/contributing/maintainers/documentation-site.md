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
| `ci/docs/` (repository root) | Publication scripts and tests |

Restart `mkdocs serve` after changing hooks; live reload does not reload the Python module.

### Diagram colors

Mermaid colors are configured in `content/stylesheets/extra.css`. Prefer the default theme colors and check diagrams in both light and dark mode after styling changes.

## Generated API reference

The API Reference page links to native generator output inside each published version:
`api/cpp/` contains Doxygen HTML. It has its own search and navigation; symbols are not
indexed by MkDocs search. The return link stays within the selected release series.

`hooks.py` runs `docs/api/Doxyfile` from the repository root before every build,
including local preview and `mike deploy`. It replaces `build/docs/generated/api/cpp/`
and copies the complete HTML tree into the configured site directory after MkDocs builds.
Generated files must not be committed. Missing Doxygen, generation failures, and missing
output fail the build; existing Doxygen documentation warnings are reported but are not fatal.

The shared pipeline image includes Doxygen. Rebuild that image before using these hooks
in CI. C++ source changes trigger `build-docs`, and local preview watches the source inputs.

Future generators should produce self-contained HTML trees under
`build/docs/generated/api/<language>/`. Add their build step and language to the post-build
copy list, then add a landing-page link. Reserve `api/python/` and `api/rust/` for those
outputs, preserving each generator's assets and internal links. No separate publication
workflow is needed: all outputs are versioned together by `mike`.

## Publishing

`publish-docs` runs automatically for protected canonical release tags after `build-docs` succeeds. It runs independently of RPM and image publication, so documentation may appear before the software is published. Prerelease and variant tags do not publish documentation.

Publication uses `CI_JOB_TOKEN` to push to the protected `gl-pages` branch. Repository pushes must be enabled for job tokens, and the triggering user must have permission to push to that branch (maintainer or higher). Use the publication workflow rather than editing `gl-pages` manually.

Each tag updates its minor-series documentation: for example, `v6.12.0.0-1` updates `6.12`. The `latest` alias points to the highest published series. After publication, check the affected version and `latest` on the [public site](https://cta.docs.cern.ch/).

If publication fails, inspect the `publish-docs` log and resolve the cause, such as missing push permissions. Retry the job in the original tag pipeline when the source contents are unchanged. If a source fix is needed, use a new tag; do not move the existing tag. Check the published version after the retry.

See [Release Procedure](releases.md) for the surrounding release workflow.

## Documentation backports

Documentation corrections for an older minor series can be published without releasing new RPMs or container images:

1. Backport the correction through a reviewed MR based on the appropriate release source. Keep included manuals and configurations consistent with that series.
2. Create a new protected canonical release tag on the corrected commit, higher than the tag currently published for that series. For example, if `6.9` was published from `v6.9.2.0-1`, use an unused revision such as `v6.9.2.0-2`. For this documentation-only update, create the tag directly with Git on the reviewed commit (replace the example version and SHA):

    ```bash
    git tag -a v6.9.2.0-2 REVIEWED_COMMIT_SHA -m "Documentation update for CTA 6.9"
    git push origin refs/tags/v6.9.2.0-2
    ```

    Use the CERN GitLab remote and ensure the new tag matches the project’s protection rules. Do not move existing tags; older versions cannot replace newer published documentation, and reuse of a version for a different commit is rejected.
3. Let `build-docs` and `publish-docs` run. Leave `internal-release-cta` and the software publication jobs untriggered.
4. Verify the updated series on the public site. Updating an older series leaves `latest` pointing to the highest published series.

!!! note "Current limitation"

    Documentation-only updates still require a software release tag and consume a release version. Coordinate the version with release maintainers. Software build and test jobs may still run, but RPM and image publication is not required.

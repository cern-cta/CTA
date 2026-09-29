# Documentation Changes

The CTA documentation is published at <https://cta.docs.cern.ch/>. Update documentation alongside related code changes in the same contribution. A local preview does not require a running CTA instance.

## Find the source

Most pages live under `docs/content/`. Some reference pages include manuals or example configurations from elsewhere in the repository; check the page’s include directive and edit the original source rather than duplicating it.

The Changelog page includes the repository’s `CHANGELOG.md`. For ordinary contributions, follow [Changelog Entries](changelog.md) to prepare commit metadata; maintainers generate the changelog during release preparation.

## Preview locally

Install Doxygen first (`brew install doxygen` on macOS or `dnf install doxygen` on AlmaLinux).
The preview generates the C++ API reference directly from source; compiling CTA is not required.

From the repository root:

```bash
cd docs
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
mkdocs serve
```

Open <http://127.0.0.1:8000/>. For later sessions, enter `docs/`, activate the same environment, and run `mkdocs serve`.

## Organize the content

Choose the section by its audience:

| Section | Location | Purpose |
| --- | --- | --- |
| Concepts | `docs/content/concepts/` | Explain responsibilities, terminology, and system behaviour. |
| Operations | `docs/content/ops/` | Help operators deploy, configure, run, and recover CTA. Operating CTA should not require reading Development. |
| Development | `docs/content/dev/` | Explain development workflows, contribution procedures, general software architecture, and maintainer tasks. |

Keep source-level implementation details, such as class-by-class descriptions, function walkthroughs, and algorithm internals, beside the code rather than in the documentation site. General architecture belongs here when it explains component responsibilities, interfaces, or how the system fits together.

Use `docs/mkdocs.yml` and its scope comments to place pages in the navigation. Use lowercase hyphenated filenames, `index.md` for section landing pages, and relative links between pages. Use descriptive component names for guides and executable names for command references. An explicit front-matter `title` can clarify a short or ambiguous navigation label.

Keep diagram files beside their pages. For Mermaid diagrams, use the default theme colors where possible; these adapt to light and dark mode. See [Diagram colors](maintainers/documentation-site.md#diagram-colors) when custom styling is needed. Moving a page changes its URL: update navigation, incoming and outgoing links, and asset paths together.

When writing:

- Link to shared explanations instead of repeating them.
- Use **disk system** for EOS/dCache services, namespace ownership, integration, and workflow reporting. Use **disk buffer** for the storage holding archive sources and retrieve destinations, including capacity, cleanup, and data transfers. See [Disk System](../../concepts/components/disk-system.md).
- Describe how to use the software in the documentation’s selected release series (see [When changes appear online](#when-changes-appear-online)). Avoid historical notes such as “Feature X was introduced in CTA 5.11”; the changelog records those changes. Include version information when it affects the instructions, such as a minimum supported EOS version or an intermediate CTA release required during an upgrade.
- Label EOS- or dCache-specific instructions clearly; keep shared guidance independent of the disk system.
- Mark unfinished material with explicit TODOs and add the standard **Documentation incomplete** notice at the top of the page, after any YAML frontmatter. Remove the notice once all TODOs are resolved; retain separate review warnings until the content has been validated.

    ```markdown
    !!! info "Documentation incomplete"

        This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.
    ```

## README or documentation page?

A `README.md` is an entry point for someone browsing the repository. Keep it short and link to fuller documentation:

- The root README introduces CTA and points readers to documentation, support, contribution guidance, and licensing.
- A component or tool README explains the directory’s purpose, any local setup needed, and a minimal usage or test example where useful.
- Put shared workflows, tutorials, operational instructions, and detailed command references in `docs/`, where readers can find them through the site navigation. Keep implementation rationale and code-specific constraints close to the source, in comments or a focused local README.

Maintain one source for each explanation. When moving a guide into `docs/`, replace the README content with a link. Manuals and configuration examples included by the site can remain beside their tools; include them rather than maintaining a second copy.

## Check before submitting

Preview the affected pages, check their links and navigation, and inspect diagrams in both light and dark modes. From `docs/`, with the Python environment activated, run:

```bash
mkdocs build --strict
```

The strict build must pass before submission. Use the normal [contribution workflow](index.md) for review.

## When changes appear online

Documentation is published from protected release tags, with one documentation version per minor release series (for example, `6.12`). A subsequent release in that series updates the same documentation version; `latest` points to the highest published series.

Merging a documentation change into `main` does not publish it immediately. It appears online when the documentation publication job succeeds for a release tag containing the change. Until then, use the local preview to see it. Readers selecting an older documentation version continue to see the documentation published for that series.

Maintainers can also publish [documentation backports](maintainers/documentation-site.md#documentation-backports) using a new release tag without publishing RPMs or images. This currently shares software release numbering rather than using separate documentation revisions.

For site configuration and publication, see [Documentation Site](maintainers/documentation-site.md).

# AGENTS.md — CTA (CERN Tape Archive)

Operating instructions for AI coding agents working in this repository.
CTA is CERN’s tape archive system, primarily written in C++ with Python and Rust components.
Read the relevant component documentation and conventions before editing.
Ask when unresolved ambiguity materially affects scope or correctness.

## Finding the relevant code

Use [Project Structure](docs/content/dev/getting-started/project-structure.md) to locate components and [Development](docs/content/dev/index.md) for workflows and architecture.
Build defaults, language versions and platform configuration live in `project.json`; compiler options live in the root `CMakeLists.txt`.

- `app/` contains the admin and workflow API services; `frontend/grpc/` contains the `cta-admin` client.
- Shared service infrastructure lives under `lib/runtime/` and `lib/cpp/`; reusable authentication belongs in `lib/cpp/cta-grpc-server/`.
- `catalogue/cta-catalogue-schema/` is the catalogue schema submodule; see the schema policy under [Source constraints](#source-constraints).

Scope searches to relevant source directories and read targeted symbols or line ranges rather than entire large files.
Exclude `build/`, `build_rpm/`, `build_srpm/`, `target/` and other generated trees from normal source searches; they can contain extracted source copies.
Inspect specific build logs, generated files and staged installation output when needed for diagnosis or verification.

## Build and validation

Prefer the supported build-container workflow managed by `ci/cta-dev.sh`.
Inspect the available environment before choosing commands; verify missing-include diagnostics in the configured build environment before changing source code to address them.
For setup, platform options and full pipeline commands, consult [CTA development environment](docs/content/dev/guides/tools-and-environment/cta-dev.md) and `ci/cta-dev.sh <command> --help`.

`cta-dev.sh build` uses a packaging tree under `build/<platform>/`; RPM builds compile an extracted source copy, so editing the checkout does not update that copy.
Use a separate compile tree against the live sources for the fast loop below.
The repository is mounted at `/shared/CTA` in the build container.
The EL9 image already selects its compiler through `PATH`, `CC` and `CXX`; use `cmake3` there.

### Fast loop (preferred): C++ build and test

Reuse an existing build container and compile tree where available.
For a new compile tree, `CTA_BUILD_MODE=compile` is the default and `CTA_VERSION` requires two nonempty parts separated by one hyphen.
The following EL9 example builds and tests authentication; select targets and test filters appropriate to the change.

```bash
# Find the build container (or use docker).
podman ps -a --filter name=cta-build

# Configure once against the live sources.
podman exec <container> cmake3 -G Ninja -S /shared/CTA -B /shared/CTA/build/dev-el9 \
  -D CMAKE_BUILD_TYPE=RelWithDebInfo -D CTA_VERSION=6-dev

# Build incrementally and run the relevant tests.
podman exec <container> cmake3 --build /shared/CTA/build/dev-el9 \
  --target cta-grpc-server-auth cta-unit-tests
podman exec <container> /shared/CTA/build/dev-el9/tests/cta-unit-tests \
  --gtest_filter='ValidateJwtTestFixture.*'
```

Discover build targets with `ninja -t targets all` in the compile tree and tests with the runner's `--gtest_list_tests` option.
Unit tests are compiled into shared libraries linked into `tests/cta-unit-tests`.
Add new test files to the relevant unit-test library in the component's `CMakeLists.txt`.
There is no `ctest` registration: invoke the appropriate runner in `tests/` directly.

### Deployment and system tests

Local Kubernetes is required for deployment and system tests, but not for compilation or unit tests.
Image builds, deployments and system tests can each take several minutes; a full `cta-dev all client` chain can take upwards of ten minutes.
Choose validation proportional to the change: start with focused builds, unit tests or rendering checks, and run the broader chain when required checks or unresolved integration risks justify it.
Do not run the full chain routinely or repeat it after successful validation without a relevant change or unresolved failure.

`cta-dev.sh deploy`, `up`, and `all` replace the managed deployment and reset its catalogue and scheduler.
Combined commands can start deleting the old deployment before the build succeeds.
Preserve logs, core dumps and other diagnostic evidence before invoking them, and use the intended namespace.
See [deployment options](docs/content/dev/guides/tools-and-environment/cta-dev.md#deployment-options) and [System Tests](docs/content/dev/guides/testing/system-tests.md) for the relevant workflow.

### Formatting and language checks

Follow the relevant [coding conventions](docs/content/dev/guides/conventions/coding/general.md) and configured [Pre-commit Hooks](docs/content/dev/guides/tools-and-environment/pre-commit.md).
For C++ changes, apply `clang-format` and run `clang-format --dry-run -Werror <files...>` before finishing; the tool is available in the build container.
Formatting rules are defined by `.clang-format`.
For Python and Rust changes, follow the [Python](docs/content/dev/guides/conventions/coding/python.md) and [Rust](docs/content/dev/guides/conventions/coding/rust.md) validation guidance.
Consult [.gitlab/ci/lint.gitlab-ci.yml](.gitlab/ci/lint.gitlab-ci.yml) for the current CI checks and their environments.

### Verifying Helm charts (`ci/orchestration/helm/`)

The charts are currently CI/development infrastructure, not a documented operator deployment interface.
Subcharts depend on shared templates and global values supplied by the combined deployment.
Prefer rendering the actual umbrella chart in `ci/orchestration/helm/cta/` with representative values from `ci/orchestration/presets/` and the configuration used by the deployment scripts.
Prepare local chart dependencies first; use a temporary copy when dependency preparation would alter generated files in the checkout.
Do not substitute helper stubs for the real integration when validating a chart change.
Standalone rendering is useful only where the chart supports it.

Parse the rendered YAML and inspect the affected fields, including defaults and overrides when changing configurable values.
Run YAML checks on rendered manifests rather than treating Helm template syntax as plain YAML.
`helm template` verifies template expansion; it does not establish Kubernetes API validity or runtime behavior.
Use appropriate schema checks or deployment/system tests when the change requires them.

## Source constraints

- **Catalogue schema:** changes are exceptional and normally undertaken only at a developer's explicit instruction.
  Implement general features against the existing schema rather than assuming it can be changed.
  If a schema change would significantly improve the solution or simplify the implementation, explain the benefit to the developer before proceeding with that change.
  Schema changes require coordinated Liquibase migration work and deployment testing; follow [Schema Development](docs/content/dev/internals/components/catalogue/schema-development.md).
- **C++ naming:** use the `m_` prefix for class data members.
- **Layering:** `common/` must not include headers from higher layers such as `frontend/`, `scheduler/` or `taped/`.
  Move shared types down into `common/` when needed.
- **Configuration:** TOML loading uses reflection through `lib/runtime/include/runtime/config/ConfigLoader.hpp`.
  Config fields use snake_case matching the TOML keys; keep `memberCount()` equal to the number of data members.
- **Errors:** user/configuration errors throw `cta::exception::UserError` with actionable context such as the file path and offending value.
  Shared exceptions belong in `common/exception/`; module-specific exceptions belong beside the code that throws them.
- **Test helpers:** prefer `tests/TempFile.hpp` and `tests/Argv.hpp`, in namespace `unitTests`, over hand-rolled equivalents.
- **Helm configuration:** when a value appears repeatedly in a template, bind it to a template variable once, as with `$jwtRevokeListPath` in `frontend-wfe`.

### Code comments and documentation

Follow [C++ documentation conventions](docs/content/dev/guides/conventions/coding/cpp.md#documentation) when adding or changing code.
Keep Doxygen comments at declarations brief: describe class purpose and method contracts, adding ownership, errors or thread-safety details only where relevant.
Avoid boilerplate documentation for trivial methods or parameters and duplicating header documentation in implementations.
Use short internal comments beside non-obvious logic to explain intent, assumptions or ordering constraints; do not narrate obvious code.
Update comments when behaviour changes, and wrap code comments at 120 columns, preferring sentence boundaries.

## Change-impact checklist

Use this checklist to identify affected files; update only the parts relevant to the change.
Do not add test exceptions merely to accommodate an inconsistent installation layout.

| When changing… | Check and update, where applicable |
| --- | --- |
| Runtime configuration, CLI options, signals, logging, health or shutdown | Runtime and service tests; example configurations; manuals; systemd units; logrotate rules; container commands; CI Helm templates; lifecycle/system tests; runtime documentation. |
| Services or installed artifacts | CMake install rules; RPM `%files`, dependencies and scriptlets; Debian manifests, control files and scripts; image package lists; `ci/checks/check_installed_layout.sh`; installation CI jobs. |
| User/group identities, permissions or capabilities | Account creation; systemd; container users/directories; CI pod security contexts; logrotate ownership; install/runtime checks. Document upgrade requirements separately. |
| File names, paths or package ownership | Producers and consumers: build/install rules, both package formats, scripts, tests, examples, manuals, documentation links/includes, navigation and relevant REUSE annotations. Search for the old path afterward. |
| Configuration keys or API contracts | Producers, consumers, validation, defaults, examples and compatibility tests. Update reflection metadata such as `memberCount()` where required. |
| Documentation | Read neighbouring pages and the documentation contribution guide; preserve terminology, prose style and audience; update navigation, links, includes and assets when moving content. |

Report the checks actually run and any remaining validation gaps.
Do not use `git diff --check` as evidence that functionality works.

## Documentation changes

Follow [Documentation Changes](docs/content/dev/contributing/documentation.md) and read nearby pages before editing.

- Match the surrounding prose and established terminology.
- Maintain one authoritative explanation; link rather than duplicate it.
- Keep steady-state operation in deployment/configuration guides and version-specific migration in upgrade guides.
- Keep CI Helm guidance in development/CI documentation, not operator deployment instructions.
- Edit the original manual or example when a documentation page includes it.
- Do not hard-wrap Markdown prose; split naturally at sentence boundaries.

## Licensing

Every file needs REUSE coverage through an appropriate SPDX header, sidecar or `REUSE.toml` annotation.
New source files should use SPDX headers in the file format's comment syntax unless an applicable annotation provides coverage.
Markdown and JSON are covered repository-wide by `REUSE.toml`.
Preserve existing attribution and third-party licensing; follow [Copyright and Licensing](docs/content/dev/guides/conventions/coding/copyright.md).

## Git and approval boundaries

Do not modify vendored code or commit secrets.
Ask before destructive actions not already authorized, formatting-policy changes, or disabling `-Werror` or suppressing compiler warnings.
Routine CMake, package metadata, dependency and CI changes required by the authorized task do not need separate approval; explain material dependency or compatibility changes.

Work on feature branches; changes reach `main` through reviewed merge requests, never direct pushes.
Do not commit unless explicitly asked.
Follow [Changelog Entries](docs/content/dev/contributing/changelog.md) and the contribution guidelines for commit metadata; the MR title and intended final commit title must match and use a prefix accepted by `ci/danger/Dangerfile`.
For commits containing substantial AI assistance, add an `Assisted-by` trailer accurately identifying the tool and model; do not guess an unavailable model identifier.

## Task-specific references

Read these when the change touches the corresponding area:

- Packaging: [CTA RPM conventions](docs/content/dev/guides/conventions/rpm-packaging.md) and [Debian packaging](packaging/deb/README.md).
- Service lifecycle and permissions: installed `systemd.exec`, `sysusers.d` and `logrotate` manuals matching the target platform; Debian's `debhelper` and `dh_installsystemd` manuals for package integration.

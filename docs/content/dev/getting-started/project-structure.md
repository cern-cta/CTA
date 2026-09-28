# CTA Project Structure

This page is a short map of the CTA repository. Paths are relative to the repository root. For component responsibilities, see the [component overview](../../concepts/components/index.md); for implementation details, see [Internals](../reference/internals/index.md).

## Direction of the layout

The planned structure separates reusable libraries, command-line tools, and service applications:

| Path | Intended contents |
| --- | --- |
| `lib/<language>/<library>/` | Reusable libraries, grouped by implementation language. |
| `tools/` | Command-line tools. |
| `app/` | Service applications. |

This reorganization is not yet complete. The map below describes where code lives in the current checkout. Some tooling in the wider CTA ecosystem is maintained in separate repositories.

## Current source layout

| Path | Contents |
| --- | --- |
| `frontend/` | Shared administrative and workflow request handling, plus the gRPC frontend and client support. |
| `catalogue/` | Catalogue interfaces, implementations, schema tooling, and tests. |
| `scheduler/` | Scheduling logic, jobs, mounts, and backend implementations in `OStoreDB/` and `rdbms/`. |
| `objectstore/` | Persistent objects, queues, and storage machinery used by the objectstore scheduler backend. |
| `taped/` | Tape daemon, sessions, drive access, SCSI, tape files, and recommended access order. |
| `maintd/` | Maintenance daemon and background work. |
| `mediachanger/` | Media-changer interfaces and communication, including the daemon in `rmcd/`. |
| `disk/` | Disk-file access and transfer support. |
| `common/` | Shared types and utilities, including authentication, configuration, logging, and checksums. |
| `rdbms/` | Shared relational database access. This is distinct from catalogue and scheduler implementations in their own `rdbms/` directories. |
| `lib/` | Runtime, telemetry, plugin-manager, and protobuf libraries. These are not yet grouped by language. |
| `tools/` | Administrative and diagnostic command-line tools, including `cta-admin`. |

## Build, tests, and documentation

| Path | Contents |
| --- | --- |
| `CMakeLists.txt`, `cmake/`, and `project.json` | Build configuration, helpers, and shared project metadata. Components also have their own `CMakeLists.txt`. |
| `packaging/` | Package templates and service integration files, such as systemd units. |
| `ci/` | Development tooling, builds, container definitions, deployment orchestration, and automation. `ci/cta-dev.sh` is the local development entry point. |
| `tests/` | Shared C++ test runners and helpers. Component tests also live beside their implementation or in component-local test directories. |
| `ci/system_tests/` | Python/pytest tests for deployed CTA instances. |
| `.gitlab-ci.yml` and `.gitlab/ci/` | GitLab pipeline configuration and jobs. |
| `.pre-commit-config.yaml` | Local contribution checks. |
| `docs/content/` | Documentation pages, grouped into `concepts/`, `ops/`, and `dev/`. |
| `docs/mkdocs.yml` | Site navigation and build configuration. |

Use [Development Workflow](../reference/tools/development-workflow.md), [Testing CTA](../reference/testing/index.md), and [Documentation Changes](../contributing/documentation.md) for task-specific guidance.

## Submodules and generated files

The checkout includes three Git submodules: `catalogue/cta-catalogue-schema/`, `lib/protobuf/external/xrootd-ssi-protobuf-interface/`, and `common/jwt-cpp/`. Initialize them using the [checkout instructions](prerequisites.md#clone-the-repository). Each is a separate repository pinned to a commit by CTA.

The build directory `build/` contains generated output and build state. Edit the source definitions or templates rather than generated files; for example, protobuf/gRPC code is generated from protocol definitions, and files ending in `.in` are build templates.

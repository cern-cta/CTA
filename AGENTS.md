# AGENTS.md — CTA (CERN Tape Archive)

Operating instructions for AI coding agents working in this repository. Complements the
human-focused `README.md` and `CONTRIBUTING.md` with executable commands, conventions, and
boundaries. When unsure about a change, consult the referenced docs or ask the user — prefer
asking over acting on assumptions.

## Project overview

CTA is CERN's tape archive system: large-scale C++23 services (tape daemon, scheduler, admin and
workflow APIs) built with CMake (3.23+) and CI deployed on EL9. Python is used for system tests and
orchestration; Rust for some clients and tools; Protobuf/gRPC for interfaces; Kubernetes
(minikube/k3s) for local dev clusters; PostgreSQL for the optional scheduler backend.

- **License:** GPL-3.0-or-later — every file needs REUSE/SPDX licensing metadata.
- **Build type:** `RelWithDebInfo` by default (see `project.json`).
- **Repository:** <https://gitlab.cern.ch/cta/CTA> · **Docs:** <https://cta.docs.cern.ch/latest/dev/overview/>

## Repository map

| Directory | Purpose |
| ----------- | --------- |
| `app/` | gRPC services: `cta-admin-api` (admin commands, `src/commands/{sync,stream}/`) and `cta-workflow-api` (WFE events) |
| `catalogue/` | Catalogue schemas, archive/retrieve file builders, database-layer objects |
| `common/` | Shared libraries (checksum, config, dataStructures, exception, log, semconv, utils, …) — lowest layer |
| `lib/runtime/` | Application runtime: TOML config loading by reflection, CLI, signals, health |
| `lib/cpp/cta-grpc-server/` | Reusable gRPC server code: auth (Kerberos, JWT), config, request tracking |
| `lib/cpp/cta-service-resources/` | `ServiceResources`: catalogue/scheduler/log context shared by the services |
| `lib/protobuf/` | Protobuf definitions, incl. `external/` EOS gRPC and XRootD SSI interfaces |
| `lib/telemetry/`, `lib/plugin-manager/` | OpenTelemetry setup; plugin interface and manager |
| `lib/rust/`, `tools-rs/` | Rust crates (CTA/EOS clients, protobuf bindings) and Rust tools |
| `scheduler/` | Archive/mount scheduling (objectstore / postgres scheduler interfaces) |
| `taped/` | Tape daemon (`cta-taped`) and tape-related services |
| `maintd/` | Maintenance daemon (`cta-maintd`) with routine interfaces |
| `mediachanger/` | Media changer command-line and common marshaling |
| `objectstore/` | "Legacy" objectstore-based storage backend |
| `disk/` | Disk file implementations, disk reporters, disk systems |
| `frontend/grpc/` | `cta-admin` gRPC client (the command-line tool talking to `cta-admin-api`) |
| `rdbms/` | RDBMS connection pool, auto-commit, rollback utilities |
| `tests/` | Unit test runner (`cta-unit-tests` and variants), `TempFile`/`Argv` helpers, integration tests |
| `ci/` | `cta-dev.sh`, CI checks, Helm charts (`ci/orchestration/helm/`), docker configs, system tests |
| `cmake/`, `tools/` | CMake modules; utility scripts and CLI tools (`cta-admin` parser, `cta-readtp`, …) |

Do **not** read or grep `build/`, `build_rpm/`, `build_srpm/`, `target/` or other build trees — they
contain full source copies and only pollute search results.

## Dev environment

**The host machine does not have the build dependencies** (toml++, gRPC, XRootD, …). Do not
compile on the host, and do not "fix" missing-include diagnostics reported there — they are
environment artifacts. All compilation happens inside a persistent build container managed by
`ci/cta-dev.sh` (see `ci/cta-dev.sh --help`):

- Container name: `cta-build-<repo-path>-el9`; the repo is mounted at `/shared/CTA`.
- The container has no system `g++`: the compiler comes from `gcc-toolset-14`
  (`source /opt/rh/gcc-toolset-14/enable`).
- `cta-dev.sh build` writes to `build/<platform>/` (e.g. `build/el9/`), which is a *packaging*
  tree: it compiles an extracted copy of the sources under `build/el9/RPM/BUILD/cta/`, so editing
  the repo does not affect it. Use a separate compile tree for the fast loop (below).
- `clang-format` is available inside the container.
- Local Kubernetes (`minikube` or `k3s`) is required only for `deploy` and system tests.

## Build & test

### Fast loop (preferred)

If the build container already exists, use it directly — much cheaper than a full
`cta-dev.sh` run. Set up a compile tree (`CTA_BUILD_MODE=compile`, the default) once;
`CTA_VERSION` is mandatory and must look like `<version>-<suffix>`:

```bash
# Is the container there? (podman or docker)
podman ps -a | grep cta-build

# One-off: configure a compile tree on the live sources
podman exec <container> bash -c 'source /opt/rh/gcc-toolset-14/enable && \
  cmake -G Ninja -S /shared/CTA -B /shared/CTA/build/dev-el9 -D CMAKE_BUILD_TYPE=RelWithDebInfo -D CTA_VERSION=6-dev'

# Incremental build of specific targets
podman exec <container> bash -c 'cd /shared/CTA/build/dev-el9 && ninja libcta-grpc-server-auth.a tests/cta-unit-tests'

# Discover target names
podman exec <container> bash -c 'cd /shared/CTA/build/dev-el9 && ninja -t targets all | grep <keyword>'

# Run a focused unit test (gtest filters save minutes over running everything)
podman exec <container> bash -c \
  '/shared/CTA/build/dev-el9/tests/cta-unit-tests --gtest_filter="ValidateJwtTestFixture.*"'
```

Unit tests are compiled into shared libs (e.g. `ctacommonunittests`, `ctaruntimeunittests`,
`cta-admin-api-unit-tests`) loaded by `tests/cta-unit-tests`. New test files must be added to the
unit-test library of the relevant `CMakeLists.txt` (e.g. `COMMON_UNIT_TESTS_LIB_SRC_FILES` in
`common/CMakeLists.txt`, or the `add_library(<x>-unit-tests SHARED …)` list in `app/*/CMakeLists.txt`).
There is no `ctest` registration: run `tests/cta-unit-tests` directly.

### Full pipeline (`cta-dev.sh`)

```bash
./ci/cta-dev.sh up      # build RPMs -> container images -> deploy to local K8s
./ci/cta-dev.sh build   # build only (reuses the persistent build container)
./ci/cta-dev.sh build --platform el9 --scheduler-type objectstore --cmake-build-type RelWithDebInfo
./ci/cta-dev.sh build --reset --clean-build-dirs   # reset container + remove build/<platform>/
./ci/cta-dev.sh images  # container images from locally built RPMs
./ci/cta-dev.sh deploy  # deploy to minikube/k3s (namespace "dev"; --with-dcache, --local-telemetry, ...)
./ci/cta-dev.sh test    # system tests (interactive; or e.g. `test client --teardown-first`)
./ci/cta-dev.sh debug   # debug build + cta-debug image for attaching to pods
./ci/cta-dev.sh all     # build + images + deploy + system tests
```

### Viewing cluster logs (`ci/log_tui.py`)

Interactive Python TUI (`textual`, `rich`; install them first) streaming logs from the CTA pods of
the development cluster:

```bash
python3 ci/log_tui.py                              # all pods in the dev namespace
python3 ci/log_tui.py --pod taped                  # one pod type (taped, frontend-wfe, …)
python3 ci/log_tui.py --with-special-logs --pod eos-mgm   # include non-stdout logs (EOS MGM fanout)
python3 ci/log_tui.py --namespace staging --lines 200     # other namespace, more history
```

### Format / lint

```bash
clang-format -i <files...>                  # apply (LLVM-based, 120 columns)
clang-format --dry-run -Werror <files...>   # check — do this before finishing any C++ change
```

### Verifying Helm charts (`ci/orchestration/helm/`)

The frontend charts (`frontend-admin`, `frontend-wfe`, …) are deployed as subcharts: named
templates like `scheduler.config` and `.Values.global.*` come from the parent chart, so a bare
`helm lint` on a single chart fails for reasons unrelated to your change. To validate template
changes standalone, copy the chart to a temp dir, add stub `define`s for missing parent helpers,
and render with stubbed globals:

```bash
helm template test /tmp/copy-of-chart -f globals-stub.yaml
# globals needed: configuration.scheduler.backend, image.pullPolicy, kerberos.*, extraVolumes, extraEnv
```

Then grep the rendered output for your change. YAML linters flag every `{{ }}` line in Helm
templates — false positives; `helm template` is the authoritative check.

## Navigating the source

- **Layering rule:** `common/` must never include headers from `frontend/`, `scheduler/`,
  `taped/`, etc. — the dependency arrow points *into* `common/`. If common code needs a type from
  a higher layer, move the type down into `common/`.
- **Config loading:** TOML configs are parsed by reflection via
  `cta::runtime::loadFromToml<T>(path, strict)` (`lib/runtime/include/runtime/config/ConfigLoader.hpp`).
  Config structs mirror the TOML structure, use snake_case fields matching the TOML keys, and must
  keep `static constexpr std::size_t memberCount()` equal to the number of data members. Example
  schemas: `app/cta-admin-api/src/AdminApiConfig.hpp`, `app/cta-workflow-api/src/WorkflowApiConfig.hpp`,
  `lib/cpp/cta-grpc-server/include/grpc-server/auth/jwt/RevokeList.hpp`.
- **Errors:** user/config errors throw `cta::exception::UserError`; include enough context
  (file path, offending value) for an operator to act on the message. Shared exceptions live in
  `cta::exception` (`common/exception/`); module-specific ones are declared next to the code that
  throws them, in that module's namespace (often via `CTA_GENERATE_(USER_)EXCEPTION_CLASS`).
- **Test helpers:** `tests/TempFile.hpp` (auto-removed temp file) and `tests/Argv.hpp` (argv built
  from an initialiser list), both in namespace `unitTests` — prefer these over hand-rolled equivalents.
- **Service layout:** each service in `app/<name>/` has `src/<Name>Config.hpp` (config schema),
  `src/<Name>App.*` (wiring into services) and `src/cta-<name>.cpp` (entry point); the admin API's
  RPC handlers are in `app/cta-admin-api/src/rpc/` and its commands in `src/commands/{sync,stream}/`.
  Reusable auth logic lives in `lib/cpp/cta-grpc-server/{include/grpc-server,src}/auth/`
  (`Authentication.*`, `jwt/`: `JwtAuthManager`, `JwkCache`, revoke list).
- **Helm/K8s config:** `ci/orchestration/helm/<component>/values.yaml` holds per-component
  defaults; configmap templates render the TOML/conf files consumed by the daemons. When a value
  must appear several times in one template (config path + configmap key + volume mount), bind it
  to a template variable once (see `$jwtRevokeListPath` in the `frontend-wfe` chart).

## Token-saving tips

- **Search scoped, not repo-wide.** Use the repository map to pick a subtree
  (e.g. `grep -rn <pat> lib/cpp/cta-grpc-server/src/auth`), and always exclude build trees
  (`... | grep -v '^build'`).
- **Read symbols, not files.** Many sources exceed 1000 lines; prefer outline/symbol tools or
  `sed -n 'X,Yp'` ranges. For diff reviews, start from `git diff --stat` and read hunks per file.
- **Build/test narrowly.** `ninja <target>` + `--gtest_filter=Fixture.*` in the existing container
  gives a verdict in seconds; full `cta-dev.sh build` or full unit-test runs cost many minutes of output.
- **Don't chase analyzer noise.** Missing-include diagnostics on the host and YAML lint errors on
  Helm templates are environment artifacts — verify inside the build container / with
  `helm template` instead.

## Coding conventions

Check the coding conventions available under `docs/content/dev/guides/conventions/coding`, for the
various programming languages. Make sure any new code respects them.

### SPDX headers (required in every source file)

```cpp
/*
 * SPDX-FileCopyrightText: <year> CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
```

Files created by an agent must include these lines (in the file format's comment syntax);
significant modifications should retain or add copyright lines for new authors. Markdown and JSON
are covered repository-wide by `REUSE.toml`; see
`docs/content/dev/guides/conventions/coding/copyright.md`.

### Style

- `.clang-format` (LLVM style, 120-column limit, 2-space indent, `AccessModifierOffset: -2`).
- Descriptive names, const-correctness, `m_` prefix for data members:

```cpp
namespace cta {

class ArchiveRequestValidator {
public:
  explicit ArchiveRequestValidator(const std::string& requestId);
  bool validate() const;

private:
  std::string m_requestId;
};

}  // namespace cta
```

### Git

- Don't commit anything unless explicitly asked by the user;
- Keep commits short and use on of the prefixes defined in `ci/danger/Dangerfile`;
- When committing code which contains substantial help from an AI-model, add an "Assisted-by" tag
with the name of the model used. e.g. `Assisted-by: Claude Code:claude-opus-5-5`

### CMake / build rules

- `cmake_minimum_required(VERSION 3.23)`; C++ standard comes from `project.json` (currently C++23).
- Default flags: `-fPIC -Wall -Wextra -Werror -Wno-unused-parameter -fstack-protector-all -pedantic`.
- Never disable `-Werror` or suppress compiler warnings without user approval.

### Branching workflow

Trunk-based development: feature branches only, all changes reach `main` via reviewed Merge
Requests. Direct pushes to `main` are forbidden.

## Boundaries

- ✅ **Always:** include SPDX headers; format with `clang-format`; write to feature branches;
  verify builds/tests inside the build container; ask when unsure instead of acting on
  hypothetical assumptions.
- ⚠️ **Ask first:** changing `.clang-format`, `CMakeLists.txt`, or `project.json`; adding
  dependencies; modifying CI pipelines (`.gitlab-ci.yml`); any destructive action; commiting code.
- 🚫 **Never:** remove or alter SPDX license identifiers; commit secrets/API keys; push directly
  to `main`; disable compiler warnings without explicit approval; modify vendored code.

## References

- `README.md` — project overview · `CONTRIBUTING.md` — MR process
- `project.json` — build config, C++ version, container images
- `.clang-format` — formatting rules · root `CMakeLists.txt` — build flags and modules
- Contributing: <https://cta.docs.cern.ch/latest/dev/contributing/>
- Getting started / `cta-dev.sh`: <https://cta.docs.cern.ch/latest/dev/getting-started/cta-dev/>
- Deployment internals: `ci/orchestration/` (`create_instance.sh`); Dockerfiles in
  `ci/docker/cta/<platform>/` (`build`, `prod`, `test` Dockerfiles); see `build_cta()`, `images_cta()`,
  `debug_cta()` in `ci/cta-dev.sh` and `ci/build/build_packages.sh`

# Debian packaging

- Debian 13 amd64; PostgreSQL catalogue and scheduler only.
- Build dependency: debhelper ≥ 13.25 from `trixie-backports`, using compatibility level 13.
- `cta` installs the services and tools; development files and tests are separate packages.
- Services are not enabled or started on first installation.

| File | Purpose |
| --- | --- |
| `control.in` | Package definitions, dependencies, and optional test-package profile. |
| `rules.in` | Build, installation, and debhelper overrides. |
| `<package>.install` / `.dirs` | Files assigned to each package and directories to create. |
| `<package>.postinst` | Package setup hooks; debhelper generates service lifecycle hooks. |
| `changelog.in` | Package version and build timestamp. |
| `copyright` | Copyright notices and license texts. |
| `source/format` | Debian source-package format. |

- CMake expands `.in` templates and `@...@` placeholders in filenames; other files are copied automatically.
- The root `debian/` directory is generated during binary packaging; edit files here instead.

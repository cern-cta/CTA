---
date: 2026-02-18
section: 1
title: CTA-ADMIN-API
header: The CERN Tape Archive (CTA)
---

# NAME

cta-admin-api --- CTA admin gRPC API server

# SYNOPSIS

**cta-admin-api** [OPTIONS]

**cta-admin-api** --help
**cta-admin-api** --version

# DESCRIPTION

**cta-admin-api** serves the administration commands of the CTA frontend (reachable through the `cta-admin` client) over gRPC.

The configuration is read from a TOML file (by default `/etc/cta/cta-admin-api.toml`). The configuration is
validated at start-up; use `--config-check` to validate it without starting the server. Unknown keys are only reported
with `--config-strict`.

Authentication uses JSON Web Tokens (`[auth.jwt]`) and/or Kerberos (`[auth.kerberos]`). At least one of them must be
enabled.

# OPTIONS

**-c**, **--config** PATH
: Path to the configuration file. Defaults to `/etc/cta/cta-admin-api.toml`.

**--config-strict**
: Treat unknown keys, missing keys and type mismatches in the configuration file as errors.

**--config-check**
: Validate the configuration, then exit.

**--runtime-dir** PATH
: Directory for state metadata such as the consumed configuration and version information.

**-l**, **--log-file** PATH
: Write logs to PATH instead of stdout/stderr.

**-v**, **--version**
: Print version information and exit.

**-h**, **--help**
: Print the help message and exit.

# FILES

`/etc/cta/cta-admin-api.toml`
: Default configuration file, also used by the `cta-admin-api` systemd service. See the `cta-admin-api.example.toml`
  documentation file for all options.

# SEE ALSO

**cta-workflow-api**(1)

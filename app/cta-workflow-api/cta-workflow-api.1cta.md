---
date: 2026-02-18
section: 1
title: CTA-WORKFLOW-API
header: The CERN Tape Archive (CTA)
---

# NAME

cta-workflow-api --- CTA workflow gRPC API server

# SYNOPSIS

**cta-workflow-api** [OPTIONS]

**cta-workflow-api** --help
**cta-workflow-api** --version

# DESCRIPTION

**cta-workflow-api** serves the workflow (EOS) requests of CTA over gRPC: archive, retrieve, cancel retrieve and delete.

The configuration is read from a TOML file (by default `/etc/cta/cta-workflow-api.toml`). The configuration is
validated at start-up; use `--config-check` to validate it without starting the server. Unknown keys are only reported
with `--config-strict`.

Authentication uses JSON Web Tokens (`[auth.jwt]`) or mutual TLS (`[auth.mtls]`). Exactly one of them must be enabled.
With mutual TLS, the certificate identities accepted for each EOS instance are configured in `[auth.mtls]`.

# OPTIONS

**-c**, **--config** PATH
: Path to the configuration file. Defaults to `/etc/cta/cta-workflow-api.toml`.

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

`/etc/cta/cta-workflow-api.toml`
: Default configuration file, also used by the `cta-workflow-api` systemd service. See the `cta-workflow-api.example.toml`
  documentation file for all options.

# SEE ALSO

**cta-admin-api**(1)

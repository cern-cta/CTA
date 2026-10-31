---
section: 1
title: CTA-SCHEDULER-SCHEMA-CREATE
header: The CERN Tape Archive (CTA)
---
<!-- SPDX-FileCopyrightText: 2026 CERN -->
<!-- SPDX-License-Identifier: GPL-3.0-or-later -->

# NAME

cta-scheduler-schema-create --- Create the CTA scheduler database schema

# SYNOPSIS

**cta-scheduler-schema-create** *databaseConnectionFile* [**--help**] [**--version** *schemaVersion*]

# DESCRIPTION

**cta-scheduler-schema-create** creates the scheduler schema in the PostgreSQL database identified by *databaseConnectionFile*.
It refuses to proceed if the `CTA_SCHEDULER` table already exists.

# OPTIONS

-h, --help

:   Display usage information and exit.

-v, --version *schemaVersion*

:   Request a particular schema version.
    This option is currently unimplemented and causes the command to fail; omit it to create the built-in schema.

# EXIT STATUS

Returns zero on success and nonzero on failure.

# SEE ALSO

**cta-scheduler-schema-drop**(1)

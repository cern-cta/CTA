---
section: 1
title: CTA-SCHEDULER-SCHEMA-DROP
header: The CERN Tape Archive (CTA)
---
<!-- SPDX-FileCopyrightText: 2026 CERN -->
<!-- SPDX-License-Identifier: GPL-3.0-or-later -->

# NAME

cta-scheduler-schema-drop --- Drop the CTA scheduler database schema

# SYNOPSIS

**cta-scheduler-schema-drop** *databaseConnectionFile* [**--help**]

# DESCRIPTION

**cta-scheduler-schema-drop** removes the scheduler schema from the database identified by *databaseConnectionFile*.
It refuses to drop a database marked as production and prompts for `yes` or `no` before removing database objects.
An empty database is treated as already dropped.

Dropping the schema deletes its data.
Use a connection file for a dedicated scheduler database.

# OPTIONS

-h, --help

:   Display usage information and exit.

# EXIT STATUS

Returns zero on success, when the database is already empty, or when the user declines confirmation.
Returns nonzero on failure.

# SEE ALSO

**cta-scheduler-schema-create**(1)

---
section: 1
title: CTA-STATISTICS-UPDATE
header: The CERN Tape Archive (CTA)
---
<!-- SPDX-FileCopyrightText: 2026 CERN -->
<!-- SPDX-License-Identifier: GPL-3.0-or-later -->

# NAME

cta-statistics-update --- Update per-tape statistics in the CTA catalogue

# SYNOPSIS

**cta-statistics-update** *databaseConnectionFile* [**--help**]

# DESCRIPTION

**cta-statistics-update** connects to the catalogue identified by *databaseConnectionFile* and updates its per-tape statistics.
It checks that the required catalogue tables and columns exist before performing the update.
Progress and the number of updated tapes are written to standard output.

# OPTIONS

-h, --help

:   Display usage information and exit.

# EXIT STATUS

Returns zero on success and nonzero on failure.

# SEE ALSO

**cta-statistics-save**(1)

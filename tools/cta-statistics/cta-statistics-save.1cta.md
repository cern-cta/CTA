---
section: 1
title: CTA-STATISTICS-SAVE
header: The CERN Tape Archive (CTA)
---
<!-- SPDX-FileCopyrightText: 2026 CERN -->
<!-- SPDX-License-Identifier: GPL-3.0-or-later -->

# NAME

cta-statistics-save --- Export CTA catalogue statistics as JSON

# SYNOPSIS

**cta-statistics-save** **--catalogueconf** *databaseConnectionFile*

# DESCRIPTION

**cta-statistics-save** reads statistics from the CTA catalogue and writes them as JSON to standard output.
Redirect standard output to a file to save a snapshot.

# OPTIONS

-c, --catalogueconf *databaseConnectionFile*

:   Required path to the catalogue database connection configuration.

-h, --help

:   Display usage information and exit.

-j

:   Accepted for backward compatibility and ignored; output is always JSON.

# EXIT STATUS

Returns zero on success and nonzero on failure.

# SEE ALSO

**cta-statistics-update**(1)

---
section: 1
title: CTA-TEST-OSM-READER
header: The CERN Tape Archive (CTA)
---
<!-- SPDX-FileCopyrightText: 2026 CERN -->
<!-- SPDX-License-Identifier: GPL-3.0-or-later -->

# NAME

cta-test-osm-reader --- Test reading OSM tapes

# SYNOPSIS

**cta-test-osm-reader** *driveName* *devicePath* [*GoogleTest options*]

# DESCRIPTION

**cta-test-osm-reader** runs hardware-dependent tests of CTA's OSM tape reader.
It requires a suitable tape loaded in an accessible drive and permissions to operate that drive.
The OSM tests expect a tape with volume identifier `L08033` and exercise reading and drive cleanup, including unloading and dismounting the tape.
They also check drive identity against the test fixture.

These programs are test tools with fixture-specific assumptions, rather than general-purpose file retrieval commands.

# ARGUMENTS

*driveName*

:   Logical name of the tape drive.

*devicePath*

:   Path to the non-rewinding tape device, for example `/dev/nst0`.

Both positional arguments must precede GoogleTest options, including options that only list tests.

# OPTIONS

--gtest_list_tests

:   List the available tests without running them.

--gtest_filter=*pattern*

:   Run only tests matching the GoogleTest filter.

# EXIT STATUS

Returns zero when all selected tests pass and nonzero if a test fails.

# SEE ALSO

**cta-readtp**(1)

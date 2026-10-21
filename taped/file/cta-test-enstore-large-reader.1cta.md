---
section: 1
title: CTA-TEST-ENSTORE-LARGE-READER
header: The CERN Tape Archive (CTA)
---
<!-- SPDX-FileCopyrightText: 2026 CERN -->
<!-- SPDX-License-Identifier: GPL-3.0-or-later -->

# NAME

cta-test-enstore-large-reader --- Test reading Enstore large-file tapes

# SYNOPSIS

**cta-test-enstore-large-reader** *driveName* *devicePath* [*GoogleTest options*]

# DESCRIPTION

**cta-test-enstore-large-reader** runs hardware-dependent tests of CTA's Enstore large-file tape reader.
It requires a suitable tape loaded in an accessible drive and permissions to operate that drive.
The test reads the tape volume identifier, reads the first file to end-of-file, and checks that data blocks were returned.
It also checks drive identity against the test fixture.

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

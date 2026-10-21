---
section: 1
title: CTA-EOSDF.SH
header: The CERN Tape Archive (CTA)
---
<!-- SPDX-FileCopyrightText: 2026 CERN -->
<!-- SPDX-License-Identifier: GPL-3.0-or-later -->

# NAME

cta-eosdf.sh --- Report free space in an EOS disk buffer

# SYNOPSIS

**cta-eosdf.sh** [*diskInstance* [*spaceName*]]

# DESCRIPTION

**cta-eosdf.sh** queries an EOS instance with **xrdfs** and writes its available space in bytes as a JSON object to standard output, for example `{"freeSpace":123456789}`.
CTA's tape daemon uses this command through the `externalFreeDiskSpaceScript` configuration setting.

# ARGUMENTS

*diskInstance*

:   EOS host name, optionally including a port, without the `root://` prefix.
    Defaults to `ctaeos`.

*spaceName*

:   EOS space to query.
    Defaults to `retrieve`.

# NOTES

The command requires **xrdfs** and access to the specified EOS instance.
It does not implement command-line options; arguments are interpreted as the instance and space names.

# SEE ALSO

**cta-taped**(1), **xrdfs**(1)

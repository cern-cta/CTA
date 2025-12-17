#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e
/usr/bin/cta-valgrindUnitTests.sh
/usr/bin/cta-integrationTests

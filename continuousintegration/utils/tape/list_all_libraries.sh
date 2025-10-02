#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later


set -e

lsscsi -g | grep mediumx | awk {'print $7'} | sed -e 's%/dev/%%' | sort

#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

echo "Sleeping..."
# sleep forever but exit immediately when pod is deleted
exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"

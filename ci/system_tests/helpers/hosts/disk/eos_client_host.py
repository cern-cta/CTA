# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from .disk_client_host import DiskClientHost


class EosClientHost(DiskClientHost):
    def __init__(self, conn):
        super().__init__(conn)

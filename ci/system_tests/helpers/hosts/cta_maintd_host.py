# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from functools import cached_property

from .remote_host import RemoteHost


class CtaMaintdHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)

    @cached_property
    def log_file_location(self) -> str:
        return "/var/log/cta/cta-maintd.log"

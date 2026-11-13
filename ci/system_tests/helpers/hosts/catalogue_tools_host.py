# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from .remote_host import RemoteHost


class CatalogueToolsHost(RemoteHost):
    """Catalogue maintenance tools with direct database access."""

    def get_schema_version(self) -> str:
        return self.exec_with_output(
            r"cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf | grep -o -E '[0-9]+\.[0-9]'"
        )

    def verify_schema(self) -> str:
        return self.exec_with_output(r"cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf")

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from pathlib import Path

# Defaults matching the CTA dCache wrapper chart.
DCACHE_INSTANCE_NAME = "dcache"
DCACHE_BASE_DIR = Path("/data")
DCACHE_ARCHIVE_GROUP = "dcacheusers"
# dCache combines the store, storage group, and HSM into this storage class.
DCACHE_STORAGE_CLASS = "test:tape@cta"
DCACHE_ADMIN_AUTH = "admin:dickerelch"
DCACHE_WEBDAV_PORT = 8083
DCACHE_REST_PORT = 3881

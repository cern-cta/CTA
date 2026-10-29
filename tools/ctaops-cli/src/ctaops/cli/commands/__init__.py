# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Cli command modules for CTA Operations.

This package contains all Cli command groups and subcommands for managing
CTA (CERN Tape Archive) operations. Commands are organized by resource type:

- tape: Tape management (create, list, get, delete, mount, unmount, etc.)
- drive: Drive management and statistics
- mount_policy: Mount policy configuration
- tape_pool: Tape pool management
- storage_class: Storage class configuration
- logical_library: Logical library operations
- physical_library: Physical library management
- admin: Administrative operations
- request: Retrieve and archive request management
- And many more specialized commands for CTA resources

Each command module uses Click command decorators with the ClickAliasedGroup
extension to support command aliases for improved user experience. All commands
inherit the AppContext containing configuration and CTA client from the main Cli.
"""

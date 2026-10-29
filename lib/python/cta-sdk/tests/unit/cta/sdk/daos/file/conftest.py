# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Shared fixtures for the file-backend DAO unit tests.

The ``data`` directory holds a referentially consistent snapshot of every model, laid out the
way the file backend stores it (``<model-name-lower>/<primary-keys-joined-by-__>.json``). It also
contains resources that no DAO public interface can create (drives, tape files, recycle-bin files,
failed requests, user requests) and reference chains that make some deletes fail with
``BackendError``.

The snapshot is read-only; the ``handler`` fixture copies it into a fresh temporary directory for
every test so that mutating operations stay isolated.
"""

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest
from cta.sdk.daos.file._handler import CtaFileHandler

DATA_DIR = Path(__file__).parent / "data"

# Fixed timestamps used when the data snapshot was generated (see the module docstring); tests
# import these to build reference objects and to assert that creation metadata is preserved.
SEED_CREATION_TIME = datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
SEED_UPDATE_TIME = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def data_root(tmp_path: Path) -> Path:
    """A writable copy of the data snapshot."""
    root = tmp_path / "cta_root"
    shutil.copytree(DATA_DIR, root)
    return root


@pytest.fixture
def handler(data_root: Path) -> CtaFileHandler:
    return CtaFileHandler(str(data_root))

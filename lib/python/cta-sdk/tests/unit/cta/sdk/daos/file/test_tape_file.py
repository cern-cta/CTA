# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.file import TapeFileDaoFile
from cta.sdk.exceptions import ResourceNotFoundError


@pytest.fixture
def dao(handler) -> TapeFileDaoFile:
    return TapeFileDaoFile(handler)


class TestGetAllCopies:
    def test_by_archive_id(self, dao):
        # archive 100 has two seeded copies (V00001/fseq1 and V00002/fseq2).
        copies = dao.get_all_copies(100)
        assert {(c.vid, c.fseq) for c in copies} == {("V00001", 1), ("V00002", 2)}

    def test_by_disk_file(self, dao):
        copies = dao.get_all_copies("df100", "instance_ci")
        assert len(copies) == 2

    def test_unknown_archive_id_returns_empty(self, dao):
        assert dao.get_all_copies(999) == ()


class TestGet:
    def test_by_archive_id_and_vid(self, dao):
        tape_file = dao.get(100, vid="V00002")
        assert tape_file.fseq == 2

    def test_by_disk_file_and_vid(self, dao):
        tape_file = dao.get("df100", "instance_ci", vid="V00001")
        assert tape_file.fseq == 1

    def test_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.get(100, vid="V99999")


class TestList:
    def test_list_by_vid(self, dao):
        vids = {tf.archive_id for tf in dao.list(vid="V00001")}
        # archive 100 (copy) and 101 (single) both sit on V00001.
        assert vids == {100, 101}

    def test_list_by_fids_file(self, dao, tmp_path):
        fids = tmp_path / "fids.txt"
        fids.write_text("df100\n")
        results = list(dao.list(fids_file=str(fids), disk_instance_name="instance_ci"))
        assert {tf.archive_id for tf in results} == {100}

    def test_list_bad_kwargs_raises(self, dao):
        with pytest.raises(TypeError):
            list(dao.list(unknown="x"))


class TestDelete:
    def test_delete_multi_copy_file(self, dao):
        dao.delete(100, vid="V00001", reason="drop one copy")
        assert {c.vid for c in dao.get_all_copies(100)} == {"V00002"}

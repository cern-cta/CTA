# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.file import TapeDaoFile
from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceNotFoundError
from cta.sdk.models import TapeMetadata, TapeState


@pytest.fixture
def dao(handler) -> TapeDaoFile:
    return TapeDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao):
        tape = dao.get("V00001")
        assert tape.media_type_name == "LTO8"
        assert tape.virtual_organisation_name == "vo_ci"
        assert isinstance(tape.metadata, TapeMetadata)

    def test_get_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost")


class TestList:
    def test_list_all(self, dao):
        assert {t.vid for t in dao.list()} == {"V00001", "V00002", "V00009"}

    def test_filter_full(self, dao):
        assert {t.vid for t in dao.list(full=True)} == {"V00002"}

    def test_filter_tape_pool(self, dao):
        assert {t.vid for t in dao.list(tape_pool_name="tp_ci")} == {"V00001", "V00002", "V00009"}

    def test_filter_state(self, dao):
        assert {t.vid for t in dao.list(state=TapeState.ACTIVE)} == {"V00001", "V00002", "V00009"}
        assert list(dao.list(state=TapeState.BROKEN)) == []

    def test_unsupported_filter_raises(self, dao):
        with pytest.raises(NotImplementedError):
            list(dao.list(missing_file_copies=True))


class TestCreate:
    def test_create_derives_linked_fields(self, dao):
        dao.create(
            "V01000", media_type_name="LTO8", vendor="IBM", logical_library_name="loglib_ci", tape_pool_name="tp_ci"
        )
        tape = dao.get("V01000")
        # capacity from media type, physical library from logical library, vo from tape pool.
        assert tape.capacity == 12_000_000_000_000
        assert tape.physical_library_name == "physlib_ci"
        assert tape.virtual_organisation_name == "vo_ci"
        assert tape.state is TapeState.ACTIVE

    def test_create_missing_media_type_raises(self, dao):
        with pytest.raises(BackendError):
            dao.create(
                "V01001",
                media_type_name="ghost",
                vendor="IBM",
                logical_library_name="loglib_ci",
                tape_pool_name="tp_ci",
            )

    def test_create_missing_logical_library_raises(self, dao):
        with pytest.raises(BackendError):
            dao.create(
                "V01001", media_type_name="LTO8", vendor="IBM", logical_library_name="ghost", tape_pool_name="tp_ci"
            )

    def test_create_missing_tape_pool_raises(self, dao):
        with pytest.raises(BackendError):
            dao.create(
                "V01001", media_type_name="LTO8", vendor="IBM", logical_library_name="loglib_ci", tape_pool_name="ghost"
            )

    def test_create_duplicate_raises(self, dao):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create(
                "V00001", media_type_name="LTO8", vendor="IBM", logical_library_name="loglib_ci", tape_pool_name="tp_ci"
            )


class TestUpdate:
    def test_update_full_and_dirty(self, dao):
        dao.update("V00001", full=True, dirty=True, comment="c2")
        tape = dao.get("V00001")
        assert tape.statistics.is_full is True
        assert tape.statistics.is_dirty is True
        assert tape.metadata.comment == "c2"

    def test_update_media_type_updates_capacity(self, dao):
        # Switching onto another media type refreshes the derived capacity.
        from cta.sdk.daos.file import MediaTypeDaoFile

        MediaTypeDaoFile(dao._handler).create("LTO9", cartridge_type="LTO-9", capacity=18_000_000_000_000)
        dao.update("V00001", media_type_name="LTO9")
        assert dao.get("V00001").capacity == 18_000_000_000_000

    def test_update_missing_media_type_raises(self, dao):
        with pytest.raises(BackendError):
            dao.update("V00001", media_type_name="ghost")

    def test_update_missing_resource_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", vendor="x")


class TestChangeStateReclaim:
    def test_change_state(self, dao):
        dao.change_state("V00001", state=TapeState.REPACKING, reason="going to repack")
        tape = dao.get("V00001")
        assert tape.state is TapeState.REPACKING
        assert tape.state_reason == "going to repack"

    def test_reclaim_resets_statistics(self, dao):
        dao.reclaim("V00002")
        stats = dao.get("V00002").statistics
        assert stats.is_full is False
        assert stats.physical_usage == 0
        assert stats.file_count == 0


class TestDelete:
    def test_delete_unreferenced(self, dao):
        # V00009 is not referenced by any tape file or recycle-bin entry.
        dao.delete("V00009")
        with pytest.raises(ResourceNotFoundError):
            dao.get("V00009")

    def test_delete_referenced_raises_backend_error(self, dao):
        # V00001 is referenced by tape files and a recycle-bin entry.
        with pytest.raises(BackendError):
            dao.delete("V00001")

    def test_delete_missing_raises(self, dao):
        with pytest.raises(ResourceNotFoundError):
            dao.delete("ghost")

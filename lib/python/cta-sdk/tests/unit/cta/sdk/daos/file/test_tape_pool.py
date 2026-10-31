# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from cta.sdk.daos.file import TapePoolDaoFile
from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.exceptions import BackendError, ResourceAlreadyExistsError, ResourceNotFoundError


@pytest.fixture
def dao(handler: CtaFileHandler) -> TapePoolDaoFile:
    return TapePoolDaoFile(handler)


class TestGet:
    def test_get_seeded(self, dao: TapePoolDaoFile):
        tp = dao.get("tp_ci")
        assert tp.virtual_organisation_name == "vo_ci"
        assert tp.encryption_key_name is None

    def test_get_encrypted_seed(self, dao: TapePoolDaoFile):
        tp = dao.get("tp_encrypted")
        assert tp.encryption_key_name == "key_ci"
        assert tp.supply_tape_pool_names == ["tp_ci"]

    def test_get_missing_raises(self, dao: TapePoolDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.get("ghost")


class TestList:
    def test_list_all(self, dao: TapePoolDaoFile):
        assert {"tp_ci", "tp_encrypted"} <= {tp.name for tp in dao.list()}

    def test_filter_by_vo(self, dao: TapePoolDaoFile):
        assert {tp.name for tp in dao.list(virtual_organisation_name="vo_ci")} == {"tp_ci", "tp_encrypted"}

    def test_filter_encrypted(self, dao: TapePoolDaoFile):
        assert {tp.name for tp in dao.list(encrypted=True)} == {"tp_encrypted"}

    def test_filter_not_encrypted(self, dao: TapePoolDaoFile):
        assert {tp.name for tp in dao.list(encrypted=False)} == {"tp_ci"}


class TestCreate:
    def test_create_then_get(self, dao: TapePoolDaoFile):
        dao.create("tp_new", virtual_organisation_name="vo_ci", min_writable_tape_count=4)
        assert dao.get("tp_new").min_writable_tape_count == 4

    def test_create_with_supply(self, dao: TapePoolDaoFile):
        dao.create("tp_supply", virtual_organisation_name="vo_ci", supply_tape_pool_names=["tp_ci"])
        assert dao.get("tp_supply").supply_tape_pool_names == ["tp_ci"]

    def test_create_missing_vo_raises(self, dao: TapePoolDaoFile):
        with pytest.raises(BackendError):
            dao.create("tp_bad", virtual_organisation_name="ghost")

    def test_create_missing_supply_raises(self, dao: TapePoolDaoFile):
        with pytest.raises(BackendError):
            dao.create("tp_bad", virtual_organisation_name="vo_ci", supply_tape_pool_names=["ghost"])

    def test_create_duplicate_raises(self, dao: TapePoolDaoFile):
        with pytest.raises(ResourceAlreadyExistsError):
            dao.create("tp_ci", virtual_organisation_name="vo_ci")


class TestUpdate:
    def test_update_min_writable(self, dao: TapePoolDaoFile):
        dao.update("tp_ci", min_writable_tape_count=9)
        assert dao.get("tp_ci").min_writable_tape_count == 9

    def test_update_missing_vo_raises(self, dao: TapePoolDaoFile):
        with pytest.raises(BackendError):
            dao.update("tp_ci", virtual_organisation_name="ghost")

    def test_update_missing_resource_raises(self, dao: TapePoolDaoFile):
        with pytest.raises(ResourceNotFoundError):
            dao.update("ghost", min_writable_tape_count=1)


class TestEncryption:
    def test_set_encryption_key(self, dao: TapePoolDaoFile):
        dao.set_encryption_key_name("tp_ci", encryption_key_name="new_key")
        assert dao.get("tp_ci").encryption_key_name == "new_key"

    def test_remove_encryption(self, dao: TapePoolDaoFile):
        dao.remove_encryption("tp_encrypted")
        assert dao.get("tp_encrypted").encryption_key_name is None


class TestDelete:
    def test_delete_unreferenced(self, dao: TapePoolDaoFile):
        dao.create("tp_free", virtual_organisation_name="vo_ci")
        dao.delete("tp_free")
        with pytest.raises(ResourceNotFoundError):
            dao.get("tp_free")

    def test_delete_referenced_by_tape_raises(self, dao: TapePoolDaoFile):
        # tp_ci is referenced by tapes and an archive route.
        with pytest.raises(BackendError):
            dao.delete("tp_ci")

    def test_delete_referenced_as_supply_raises(self, dao: TapePoolDaoFile):
        # tp_ci is listed as a supply pool of tp_encrypted.
        with pytest.raises(BackendError):
            dao.delete("tp_ci")

# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import asdict
from datetime import timedelta
from pathlib import Path

import pytest
from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.exceptions import ResourceAlreadyExistsError, ResourceNotFoundError
from cta.sdk.models import (
    AdminUser,
    ArchiveRoute,
    ArchiveRouteType,
    DiskInstanceSpace,
    DiskSystem,
    MountPolicy,
    Tape,
    TapeMetadata,
)

from .conftest import SEED_CREATION_TIME


class TestConstruction:
    def test_missing_directory_raises(self, tmp_path: Path):
        with pytest.raises(ValueError, match="not a valid directory"):
            CtaFileHandler(str(tmp_path / "does-not-exist"))

    def test_existing_directory_ok(self, data_root: Path):
        handler = CtaFileHandler(str(data_root))
        assert handler.root_dir == data_root


class TestResourceId:
    def test_single_key(self):
        assert CtaFileHandler.resource_id("afonso") == "afonso"

    def test_composite_keys_joined(self):
        assert CtaFileHandler.resource_id("space", "instance") == "space__instance"

    def test_enum_uses_name(self):
        assert CtaFileHandler.resource_id("sc", 1, ArchiveRouteType.DEFAULT) == "sc__1__DEFAULT"

    def test_class_to_dir_name_is_lowercased(self):
        assert CtaFileHandler.class_to_dir_name(DiskInstanceSpace) == "diskinstancespace"


class TestGet:
    def test_get_existing(self, handler: CtaFileHandler):
        admin = handler.get(AdminUser, "admin_ci")
        assert admin.name == "admin_ci"
        assert admin.metadata.comment == "Admin CI"
        assert admin.metadata.creation_time == SEED_CREATION_TIME

    def test_get_missing_raises(self, handler: CtaFileHandler):
        with pytest.raises(ResourceNotFoundError):
            handler.get(AdminUser, "nobody")

    def test_get_composite_key(self, handler: CtaFileHandler):
        space = handler.get(DiskInstanceSpace, "space_ci", "instance_ci")
        assert space.disk_instance_name == "instance_ci"

    def test_get_with_enum_key(self, handler: CtaFileHandler):
        route = handler.get(ArchiveRoute, "sc_ci", 1, ArchiveRouteType.DEFAULT)
        assert route.tape_pool_name == "tp_ci"


class TestCreate:
    def test_create_then_get_roundtrip(self, handler: CtaFileHandler):
        user = AdminUser("brand_new", metadata=handler.generate_metadata(comment="hi"))
        handler.create(user)
        assert asdict(handler.get(AdminUser, "brand_new")) == asdict(user)

    def test_create_duplicate_raises(self, handler: CtaFileHandler):
        with pytest.raises(ResourceAlreadyExistsError):
            handler.create(AdminUser("admin_ci", metadata=handler.generate_metadata()))

    def test_create_writes_expected_path(self, handler: CtaFileHandler, data_root: Path):
        handler.create(AdminUser("path_check", metadata=handler.generate_metadata()))
        assert (data_root / "adminuser" / "path_check.json").is_file()


class TestDelete:
    def test_delete_removes_file(self, handler: CtaFileHandler, data_root: Path):
        handler.delete(AdminUser, "admin_ci")
        assert not (data_root / "adminuser" / "admin_ci.json").is_file()
        with pytest.raises(ResourceNotFoundError):
            handler.get(AdminUser, "admin_ci")

    def test_delete_missing_raises(self, handler: CtaFileHandler):
        with pytest.raises(ResourceNotFoundError):
            handler.delete(AdminUser, "nobody")


class TestIter:
    def test_iter_yields_all(self, handler: CtaFileHandler):
        assert {t.vid for t in handler.iter(Tape)} == {"V00001", "V00002", "V00009"}

    def test_iter_empty_model(self, handler: CtaFileHandler):
        # DiskSystem has a single seed; delete it and the iterator empties without error.
        handler.delete(DiskSystem, "system_ci")
        assert list(handler.iter(DiskSystem)) == []


class TestUpdateContextManager:
    def test_update_persists_changes(self, handler: CtaFileHandler):
        with handler.update(AdminUser, "admin_ci") as admin:
            admin.metadata.comment = "changed"
        assert handler.get(AdminUser, "admin_ci").metadata.comment == "changed"

    def test_update_missing_raises(self, handler: CtaFileHandler):
        with pytest.raises(ResourceNotFoundError), handler.update(AdminUser, "nobody"):
            pass


class TestTimedeltaSerialization:
    def test_timedelta_roundtrips(self, handler: CtaFileHandler):
        policy = MountPolicy(
            "roundtrip_mp",
            archive_priority=1,
            min_archive_age=timedelta(hours=3, minutes=30),
            retrieve_priority=2,
            min_retrieve_age=timedelta(seconds=42),
            metadata=handler.generate_metadata(),
        )
        handler.create(policy)
        loaded = handler.get(MountPolicy, "roundtrip_mp")
        assert loaded.min_archive_age == timedelta(hours=3, minutes=30)
        assert loaded.min_retrieve_age == timedelta(seconds=42)


class TestMetadataHelpers:
    def test_generate_metadata_default_comment(self):
        meta = CtaFileHandler.generate_metadata()
        assert meta.comment == "From python cta-sdk"
        assert meta.creation_time == meta.update_time or meta.creation_time <= meta.update_time

    def test_refresh_metadata_preserves_creation_and_updates_update(self, handler: CtaFileHandler):
        original = handler.get(AdminUser, "admin_ci").metadata
        refreshed = handler.refresh_metadata(original, comment="new comment")
        assert refreshed.creation_time == original.creation_time
        assert refreshed.creation_user_name == original.creation_user_name
        assert refreshed.comment == "new comment"
        assert refreshed.update_time >= original.update_time

    def test_refresh_metadata_keeps_comment_when_none(self, handler: CtaFileHandler):
        original = handler.get(AdminUser, "admin_ci").metadata
        refreshed = handler.refresh_metadata(original)
        assert refreshed.comment == original.comment

    def test_refresh_metadata_preserves_subclass(self, handler: CtaFileHandler):
        tape_meta = handler.get(Tape, "V00001").metadata
        assert isinstance(tape_meta, TapeMetadata)
        refreshed = handler.refresh_metadata(tape_meta, comment="x")
        # replace() must keep the TapeMetadata type and its extra fields.
        assert isinstance(refreshed, TapeMetadata)
        assert refreshed.write_mount_count == tape_meta.write_mount_count

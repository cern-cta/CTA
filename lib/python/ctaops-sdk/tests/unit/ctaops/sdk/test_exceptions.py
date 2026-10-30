"""Tests for the exception hierarchy exposed by ctaops.sdk."""

import pytest
from ctaops.sdk import (
    CtaopsSdkError,
    DriveError,
    DriveIOError,
    DriveOperationError,
    DriveStateError,
    EODError,
    EOTError,
    LibraryOperationError,
    ResourceNotFoundError,
    TapeError,
    TapeLabelError,
    TapeStateError,
)


@pytest.mark.parametrize(
    ("exc_cls", "parent"),
    [
        (ResourceNotFoundError, CtaopsSdkError),
        (LibraryOperationError, CtaopsSdkError),
        (DriveError, CtaopsSdkError),
        (TapeError, CtaopsSdkError),
        (TapeStateError, TapeError),
        (TapeLabelError, TapeError),
        (DriveStateError, DriveError),
        (DriveOperationError, DriveError),
        (DriveIOError, DriveError),
        (EOTError, DriveIOError),
        (EODError, DriveIOError),
    ],
)
def test_exception_hierarchy(exc_cls: type, parent: type) -> None:
    assert issubclass(exc_cls, parent)
    assert issubclass(exc_cls, CtaopsSdkError)


def test_eot_error_carries_checksum_and_written_files() -> None:
    err = EOTError("tape full", checksum=0xDEADBEEF, written_files=42)

    assert err.write_checksum == 0xDEADBEEF
    assert err.written_files == 42
    assert "tape full" in str(err)
    assert isinstance(err, DriveIOError)


def test_eot_error_requires_keyword_arguments() -> None:
    with pytest.raises(TypeError):
        EOTError("boom", 1, 2)  # type: ignore[call-arg]


def test_eod_error_is_plain_drive_io_error() -> None:
    err = EODError("end of data")
    assert isinstance(err, DriveIOError)
    assert str(err) == "end of data"

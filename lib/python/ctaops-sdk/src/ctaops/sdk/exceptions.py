# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Exception classes for ctaops-sdk package.

The hierarchy is

    CtaopsSdkError
    ├── ResourceNotFoundError
    ├── LibraryOperationError
    ├── DriveError
    │   ├── DriveStateError
    │   ├── DriveOperationError
    │   └── DriveIOError
    │       ├── EOTError
    │       └── EODError
    └── TapeError
        ├── TapeStateError
        └── TapeLabelError

Catch [CtaopsSdkError][ctaops.sdk.exceptions.CtaopsSdkError] to handle any failure raised by this package;
catch a more specific subclass when you need to react differently per
failure mode (for example, distinguishing physical end-of-tape from an
unexpected drive state).
"""


class CtaopsSdkError(Exception):
    """Base class for every exception raised by [ctaops.sdk][ctaops.sdk].

    Example:
        ```python
        from ctaops.sdk import CtaopsSdkError

        try:
            with library.mount("drive-0", "X12345") as drive:
                ...
        except CtaopsSdkError:
            ...  # any failure originating from ctaops-sdk
        ```
    """


class ResourceNotFoundError(CtaopsSdkError):
    """Raised when a requested tape drive or library device is not found.

    This occurs when attempting to access a tape drive or library that
    does not exist or is not currently available on the system.
    """


class LibraryOperationError(CtaopsSdkError):
    """Error related to library operations.

    Raised when library operations such as mount, unmount, drive listing,
    or other media changer operations fail.
    """


class DriveError(CtaopsSdkError):
    """Base exception for drive-related errors.

    Represents errors that occur during tape drive operations.
    """


class TapeError(CtaopsSdkError):
    """Base exception for tape-related errors.

    Represents errors that occur during tape operations.
    """


class TapeStateError(TapeError):
    """Raised when a tape is not in the required state before/after an operation.

    This occurs when attempting an operation that requires the tape to be
    in a specific state (e.g., mounted, ready for write) but it is not.
    """


class TapeLabelError(TapeError):
    """Raised when tape labeling operation fails.

    This occurs when CTA tape labeling operations fail, including when
    attempting to write or update tape volume labels on cartridges.
    """


class DriveStateError(DriveError):
    """Raised when a drive is not in the required state before/after an operation.

    This occurs when attempting an operation that requires the drive to be
    in a specific state (e.g., free, loaded with tape) but it is not.
    """


class DriveOperationError(DriveError):
    """Raised when a drive operation fails.

    This covers failures in operations like getting temperature, status,
    ejecting tapes, setting compression, or other drive control operations.
    """


class DriveIOError(DriveError):
    """Raised when a read/write I/O operation on tape fails.

    This covers errors that occur during actual data transfer operations
    such as reading from or writing to a tape cartridge.
    """


class EOTError(DriveIOError):
    """Raised when end-of-tape (EOT) is reached during a write.

    Carries the bookkeeping a caller needs to roll forward after the
    failure: the Adler-32 checksum of each successful file and the count
    of files that were written in full before the tape ran out.

    Attributes:
        write_checksum: Adler-32 checksum of each file written before EOT.
        written_files: Number of files fully written before EOT (does not
            include the file that failed mid-write).

    Example:
        ```python
        from ctaops.sdk import EOTError

        try:
            for index, checksum in drive.write_files(file_size=1 << 30, n=None):
                ...
        except EOTError as e:
            print("EOT after", e.written_files, "files; checksum=", e.write_checksum)
        ```
    """

    def __init__(self, *args: object, checksum: int, written_files: int) -> None:
        super().__init__(*args)
        self.write_checksum = checksum
        self.written_files = written_files


class EODError(DriveIOError):
    """Raised when end-of-data (EOD) is reached.

    This indicates that the tape drive has reached the end of recorded
    data on a tape cartridge.
    """

# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import base64
import re
from contextlib import contextmanager
from dataclasses import fields, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from getpass import getuser
from logging import getLogger
from pathlib import Path
from socket import gethostname
from typing import (
    Annotated,
    Any,
    ClassVar,
    Final,
    Generator,
    Optional,
    Protocol,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from cattrs.preconf.json import make_converter
from cta.sdk.exceptions import ResourceAlreadyExistsError, ResourceNotFoundError
from cta.sdk.models import Metadata


class DataClassProtocol(Protocol):
    """Protocol for dataclass objects.

    Attributes:
        __dataclass_fields__: Dictionary of dataclass field definitions.
    """

    __dataclass_fields__: ClassVar[dict]


D = TypeVar("D", bound=DataClassProtocol)
M = TypeVar("M", bound=Metadata)


LOGGER = getLogger("cta.sdk")
FLAG = object()

INVALID_WINDOWS_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1F]')


def as_timedelta(value: Union[int, timedelta]) -> timedelta:
    """Normalise a duration given either as seconds or as a timedelta."""
    return value if isinstance(value, timedelta) else timedelta(seconds=value)


class CtaFileHandler:
    def __init__(
        self,
        root_dir: str,
    ):
        self.root_dir = Path(root_dir)
        if not self.root_dir.is_dir():
            raise ValueError(f"Provided root directory '{self.root_dir}' is not a valid directory.")
        LOGGER.debug(
            "CTA File Handler initialized with root directory: %s",
            self.root_dir,
        )
        # make_converter() registers the preconf JSON hooks (datetime, ...);
        # instantiating JsonConverter() directly leaves them unregistered.
        self.converter = make_converter()
        # cattrs ships no timedelta hook: serialise durations as seconds.
        self.converter.register_unstructure_hook(timedelta, lambda td: td.total_seconds())
        self.converter.register_structure_hook(timedelta, lambda v, _: timedelta(seconds=float(v)))

    def create(
        self,
        obj: DataClassProtocol,
    ) -> None:
        primary_keys = [
            getattr(obj, field.name)
            for field in fields(obj)
            if get_origin(field.type) is Final
            or (get_origin(field.type) is Annotated and get_args(field.type)[0] is Final)
        ]

        resource_file = self._resource_file(type(obj), *primary_keys)
        resource_file.parent.mkdir(parents=True, exist_ok=True)

        if resource_file.is_file():
            raise ResourceAlreadyExistsError(self.class_to_dir_name(type(obj)), self.resource_id(*primary_keys))

        resource_file.write_text(self.converter.dumps(obj))

    def delete(self, resource_type: type[DataClassProtocol], *primary_keys: Any) -> None:
        resource_file = self._resource_file(resource_type, *primary_keys)

        if not resource_file.is_file():
            raise ResourceNotFoundError(self.class_to_dir_name(resource_type), self.resource_id(*primary_keys))

        resource_file.unlink()

    def get(self, resource_type: type[D], *primary_keys: Any) -> D:
        resource_file = self._resource_file(resource_type, *primary_keys)

        if not resource_file.is_file():
            raise ResourceNotFoundError(self.class_to_dir_name(resource_type), self.resource_id(*primary_keys))

        return self.converter.loads(resource_file.read_text(), resource_type)

    def iter(self, resource_type: type[D]) -> Generator[D, None, None]:
        resource_dir = self.root_dir / self.class_to_dir_name(resource_type)
        resource_dir.mkdir(parents=False, exist_ok=True)
        yield from (
            self.converter.loads(f.read_text(), resource_type)
            for f in resource_dir.iterdir()
            if f.is_file() and f.suffix == ".json"
        )

    @contextmanager
    def update(
        self,
        resource_type: type[D],
        *primary_keys: Any,
    ) -> Generator[D, None, None]:
        obj = self.get(resource_type, *primary_keys)
        yield obj

        self._resource_file(resource_type, *primary_keys).write_text(self.converter.dumps(obj))

    @staticmethod
    def class_to_dir_name(resource_type: type[DataClassProtocol]) -> str:
        return resource_type.__name__.lower()

    @staticmethod
    def resource_id(*primary_keys: Any) -> str:
        return "__".join(
            CtaFileHandler._encode_if_needed(str(k.name if isinstance(k, Enum) else k)) for k in primary_keys
        )

    def _resource_file(self, resource_type: type[DataClassProtocol], *primary_keys: Any) -> Path:
        return self.root_dir / self.class_to_dir_name(resource_type) / (self.resource_id(*primary_keys) + ".json")

    @staticmethod
    def gen_creation_metadata() -> dict[str, Any]:
        return {
            "creation_user_name": getuser(),
            "creation_host_name": gethostname(),
            "creation_time": datetime.now(tz=timezone.utc),
        }

    @staticmethod
    def gen_update_metadata() -> dict[str, Any]:
        return {
            "update_user_name": getuser(),
            "update_host_name": gethostname(),
            "update_time": datetime.now(tz=timezone.utc),
        }

    @staticmethod
    def generate_metadata(comment: str = "From python cta-sdk") -> Metadata:
        return Metadata(
            **CtaFileHandler.gen_creation_metadata(), **CtaFileHandler.gen_update_metadata(), comment=comment
        )

    @staticmethod
    def refresh_metadata(metadata: M, /, *, comment: Optional[str] = None) -> M:
        """Return `metadata` with the update fields regenerated, creation fields untouched.

        Uses `replace` so that subclasses (e.g. `TapeMetadata`) keep their own type and extra fields.
        """
        return replace(
            metadata,
            **CtaFileHandler.gen_update_metadata(),
            **({} if comment is None else {"comment": comment}),
        )

    @staticmethod
    def _encode_if_needed(value: str) -> str:
        if INVALID_WINDOWS_CHARS.search(value):
            return base64.urlsafe_b64encode(value.encode()).decode()
        return value

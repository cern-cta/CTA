# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Main module referencing all CTA SDK exceptions"""

from typing import Any, Union


class CtaSdkError(Exception):
    """Base class for all CTA interaction errors"""


class BackendError(CtaSdkError):
    """CTA backend generic error"""

    def __init__(self, error: str, return_code: int, request_info: Any, *args, **kwargs) -> None:
        self.error = error
        self.return_code = return_code
        self.request_info = request_info
        super().__init__(f"CTA Backend critical error ({return_code}): {error.strip()}", *args, **kwargs)


class ResourceNotFoundError(CtaSdkError):
    """Resource not found in CTA"""

    def __init__(self, resource_type: str, id: Union[dict[str, Any], str], *args, **kwargs):
        self.resource_type = resource_type
        self.id = id
        super().__init__(f"{resource_type}({id}) not found", *args, **kwargs)


class ResourceAlreadyExistsError(CtaSdkError):
    """Cta resource already exists"""

    def __init__(self, resource_type: str, id: Union[dict[str, Any], str], *args, **kwargs):
        self.resource_type = resource_type
        self.id = id
        super().__init__(f"{resource_type}({id}) already exists in CTA", *args, **kwargs)


class ResourceConflictError(CtaSdkError):
    """More than one resource found where only one was expected"""

    def __init__(self, resource_type: str, id: Union[dict[str, Any], str], match_count: int, *args, **kwargs):
        self.resource_type = resource_type
        self.id = id
        self.match_count = match_count
        super().__init__(f"{resource_type}({id}) has multiple matches ({match_count})", *args, **kwargs)

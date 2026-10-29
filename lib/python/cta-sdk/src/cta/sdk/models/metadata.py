# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

from cta.sdk.units import FreeText


@dataclass
class Metadata:
    """Metadata information of the resource.

    Attributes:
        creation_user_name: Admin user which created the resource.
        creation_host_name: Server hostname from which resource was created.
        creation_time: Date of the resource creation.
        update_user_name: Last user which updated the resource.
        update_host_name: Server hostname from which resource was last updated.
        update_time: Date of last update of the resource.
        comment: User comment, provides context for operators.
    """

    creation_user_name: str
    creation_host_name: str
    creation_time: datetime
    update_user_name: str
    update_host_name: str
    update_time: datetime
    comment: Annotated[str, FreeText]

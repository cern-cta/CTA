# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Unit annotation classes for CTA SDK model attributes.

This module provides unit marker classes used to annotate numerical fields in CTA SDK models,
indicating the units of measurement for those values. These annotations could be used by a display
module to format output appropriately (e.g., converting bytes to human-readable units).

Classes:
    Unit: Abstract base class for all unit annotations
    Byte: Unit marker for byte quantities
    Percentage: Unit marker for percentage values
    BytePerSecond: Unit marker for bytes per second (speed)
    FreeText: Unit marker for free text

Example:
```python
from typing import Annotated
from dataclasses import dataclass
@dataclass
class Tape:
    vid: str
    tape_usage: Annotated[int, Byte]
```
"""

from abc import ABC, abstractmethod


class Unit(ABC):
    """Abstract base class for unit annotations.

    Unit annotations are used to mark numerical fields in data models with their
    units of measurement. This allows the display layer to format values appropriately
    without needing to store unit information in the model itself.

    Subclasses should be minimal marker classes with no state or behavior.
    """

    @abstractmethod
    def __init__(self):
        """Initialize the unit marker."""
        pass


class Byte(Unit):
    """Unit marker for byte quantities.

    Used to annotate fields containing byte counts or data sizes.
    The display layer will format these values with appropriate units
    (B, KB, MB, GB, TB, PB, EB) based on magnitude.

    Example:
        A field annotated as `Annotated[int, Byte]` containing the value
        1073741824 might be displayed as "1.0 GB".
    """

    def __init__(self):
        """Initialize the Byte unit marker."""
        pass


class Percentage(Unit):
    """Unit marker for percentage values.

    Used to annotate fields containing percentage values (0-100).
    The display layer will format these with appropriate precision
    and percentage symbol.

    Example:
        A field annotated as `Annotated[float, Percentage]` containing 75.5
        might be displayed as "75.5%".
    """

    def __init__(self):
        """Initialize the Percentage unit marker."""
        pass


class BytePerSecond(Unit):
    """Unit marker for throughput values (bytes per second).

    Used to annotate fields containing speed or throughput measurements
    in bytes per second. The display layer will format these with
    appropriate units and descriptive labels.

    Example:
        A field annotated as `Annotated[int, BytePerSecond()]` containing 1048576
        might be displayed as "1.0 MB/s".
    """

    def __init__(self):
        """Initialize the BytePerSecond unit marker."""
        pass


class FreeText(Unit):
    """Unit marker for str representing free text potentially put by a human.

    Used to annotate field which represents descriptive text, with no clear formatting constraints.

    Example:
        A field annotated as `Annotated[int, freeText()]` containing "My super comment"
    """

    def __init__(self):
        """Initialize the FreeText unit marker."""
        pass

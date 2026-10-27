# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

""" """

from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
from typing import Annotated, Literal, Optional, Union

from cta.sdk.units import FreeText
from ctaops.utils.prometheus import Labels

IPAddress = Union[IPv4Address, IPv6Address]
Family = Literal["ipv4", "ipv6"]
FamilyScope = Union[Family, Literal["all"]]


@dataclass(frozen=True)
class Service:
    """A gRPC service name (DNS alias) and port."""

    name: str
    port: int

    @classmethod
    def parse(cls, spec: str, default_port: Optional[int]) -> "Service":
        """Parse "name:port", or "name" with default_port."""
        name, sep, port = spec.rpartition(":")
        if sep and port.isdigit():
            return cls(name, int(port))
        if default_port is None:
            raise ValueError("no port given for service %r (use name:port or --port)" % spec)
        return cls(spec, default_port)

    def labels(self) -> Labels:
        return {"service": self.name, "port": self.port}


@dataclass(frozen=True)
class Backend:
    """One resolved address of a service."""

    service: Service
    address: Annotated[IPAddress, FreeText]

    @property
    def family(self) -> Family:
        return "ipv6" if self.address.version == 6 else "ipv4"

    @property
    def target(self) -> str:
        """gRPC target pointing at this exact address."""
        host = f"[{self.address}]" if self.address.version == 6 else str(self.address)
        return f"{self.family}:{host}:{self.service.port}"


@dataclass(frozen=True)
class ResolvedService:
    service: Service
    addresses: list[IPAddress]

    @property
    def backends(self) -> list[Backend]:
        return [Backend(self.service, ip) for ip in self.addresses]


@dataclass(frozen=True)
class ProbeResult:
    backend: Backend
    hostname: str
    up: bool
    duration: float

    def labels(self) -> Labels:
        return {
            **self.backend.service.labels(),
            "family": self.backend.family,
            "address": str(self.backend.address),
            "hostname": self.hostname,
        }


@dataclass(frozen=True)
class Availability:
    """Aggregated health of a service for one address family ("all" for every family)."""

    service: Service
    family: FamilyScope
    total: int
    up: int

    @property
    def percent(self) -> float:
        return 100.0 * self.up / self.total if self.total else 0.0

    def labels(self) -> Labels:
        return {**self.service.labels(), "family": self.family}

# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import socket
import time
from concurrent.futures import ThreadPoolExecutor
from ipaddress import ip_address
from logging import getLogger
from typing import Literal, Optional

import click
import grpc
from ctaops.utils.prometheus import Labels, Value, sample, write_atomic
from grpc_health.v1 import health_pb2, health_pb2_grpc

from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses
from ctaops.cli.models.frontend import (
    Availability,
    Backend,
    Family,
    FamilyScope,
    IPAddress,
    ProbeResult,
    ResolvedService,
    Service,
)

LOGGER = getLogger("ctaops.cli")
FAMILIES: dict[Family, socket.AddressFamily] = {"ipv4": socket.AF_INET, "ipv6": socket.AF_INET6}


def dns_reverse_lookup(ip: IPAddress) -> str:
    try:
        return socket.gethostbyaddr(str(ip))[0]
    except OSError:
        return ""


def probe_backend(backend: Backend, ca_credentials: grpc.ChannelCredentials, timeout: float) -> ProbeResult:
    # Connect to the IP but present the server name, both as :authority (gRPC requires them to match) and for TLS
    options = [
        ("grpc.default_authority", f"{backend.service.name}:{backend.service.port}"),
        ("grpc.ssl_target_name_override", backend.service.name),
    ]
    start = time.monotonic()
    try:
        with grpc.secure_channel(backend.target, ca_credentials, options=options) as channel:
            response = health_pb2_grpc.HealthStub(channel).Check(health_pb2.HealthCheckRequest(), timeout=timeout)  # type: ignore
        serving = response.status == health_pb2.HealthCheckResponse.SERVING
    except (OSError, grpc.RpcError) as e:  # OSError covers certificate fetch failures, including ssl.SSLError
        LOGGER.debug(f"Health check failed for {backend.target}: {e}")
        serving = False
    duration = time.monotonic() - start
    return ProbeResult(backend=backend, hostname=dns_reverse_lookup(backend.address), up=serving, duration=duration)


def resolve_dns(service: Service, families: list[Family]) -> ResolvedService:
    """Look up the addresses of service for each family (explicit A/AAAA queries: AF_UNSPEC may drop AAAA)."""
    addresses = []
    for family in families:
        try:
            infos = socket.getaddrinfo(service.name, None, FAMILIES[family], socket.SOCK_STREAM)
        except socket.gaierror as e:
            LOGGER.debug("No %s address for %s: %s", family, service.name, e)
            continue
        addresses.extend({ip_address(info[4][0]) for info in infos})
    return ResolvedService(service, addresses)


def generate_availabilities(
    resolved: list[ResolvedService], results: list[ProbeResult], families: list[Family]
) -> list[Availability]:
    by_service: dict[Service, list[ProbeResult]] = {}
    for r in results:
        by_service.setdefault(r.backend.service, []).append(r)
    scopes: list[FamilyScope] = ["all", *families]
    availability: list[Availability] = []
    for rs in resolved:
        service_results = by_service.get(rs.service, [])
        for scope in scopes:
            up = [r.up for r in service_results if scope in ("all", r.backend.family)]
            availability.append(Availability(rs.service, scope, len(up), sum(up)))
    return availability


def render(
    resolved: list[ResolvedService],
    results: list[ProbeResult],
    availabilities: list[Availability],
) -> str:
    prefix = "ctaops_frontend"
    # (metric, help, samples)
    metrics: list[tuple[str, str, list[tuple[Labels, Value]]]] = [
        (
            "dns_resolved",
            "1 if the service name resolved to at least one address.",
            [(rs.service.labels(), int(bool(rs.addresses))) for rs in resolved],
        ),
        (
            "up",
            "1 if the gRPC health check on this address returned SERVING.",
            [(r.labels(), int(r.up)) for r in results],
        ),
        (
            "health_check_duration_seconds",
            "Duration of the gRPC health check on this address.",
            [(r.labels(), f"{r.duration:.6f}") for r in results],
        ),
        (
            "backends_total",
            "Number of addresses resolved for the service.",
            [(a.labels(), a.total) for a in availabilities],
        ),
        (
            "backends_up",
            "Number of addresses of the service whose health check returned SERVING.",
            [(a.labels(), a.up) for a in availabilities],
        ),
        (
            "availability_percent",
            "Percentage of resolved addresses of the service that are SERVING.",
            [(a.labels(), f"{a.percent:.2f}") for a in availabilities],
        ),
    ]
    lines: list[str] = []
    for metric, help_text, samples in metrics:
        lines.append(f"# HELP {prefix}_{metric} {help_text}")
        lines.append(f"# TYPE {prefix}_{metric} gauge")
        lines.extend(sample(metric, labels, value, prefix=prefix) for labels, value in samples)
    return "\n".join(lines) + "\n"


@click.group(cls=ClickAliasedGroup)
def frontend():
    """Manage CTA failed requests"""


@frontend.command()
@click.argument("service", nargs=-1, required=True)
@click.option(
    "-p", "--port", type=click.IntRange(1, 65535), help="Default port for services given without one.", default=10956
)
@click.option(
    "--family",
    "families",
    type=click.Choice(["all", *FAMILIES]),
    default="all",
    show_default=True,
    callback=lambda ctx, param, value: FAMILIES.copy() if value == "all" else [value],
    help="Address families to probe.",
)
@click.option(
    "--tls-ca-cert",
    type=click.Path(dir_okay=False),
    default="/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    show_default=True,
    help="CA bundle to verify server certificates.",
)
@click.option(
    "-t",
    "--timeout",
    type=click.FloatRange(0, min_open=True),
    default=3.0,
    show_default=True,
    help="Health check timeout in seconds.",
)
@click.option(
    "-j", "--jobs", type=click.IntRange(min=1), default=8, show_default=True, help="Number of parallel checks."
)
@click.option(
    "-c",
    "--output-content",
    type=click.Choice(["prom-metrics", "availability", "probe"]),
    default="availability",
    show_default=True,
    help="Choose the content of the output",
)
@click.option("-o", "--prom-output-file", type=click.Path(dir_okay=False), help="Write metrics to this file atomically")
@click.pass_obj
def health_check(
    obj: AppContext,
    service: tuple[str, ...],
    port: Optional[int],
    families: list[Family],
    tls_ca_cert: str,
    timeout: float,
    jobs: int,
    output_content: Literal["prom-metrics", "availability", "probe"],
    prom_output_file: Optional[str],
) -> None:
    """List CTA failed requests"""

    if prom_output_file is not None and output_content != "prom-metrics":
        raise click.UsageError("You can't use --prom-output-file if --output-content/-c is not 'prometheus'")

    try:
        services = [Service.parse(s, port) for s in service]
    except ValueError as e:
        raise click.UsageError(str(e)) from e

    try:
        with open(tls_ca_cert, "rb") as f:
            ca_credentials = grpc.ssl_channel_credentials(root_certificates=f.read())
    except OSError as e:
        raise click.BadParameter(str(e), param_hint="--tls-ca-cert") from e

    with ThreadPoolExecutor(max_workers=jobs) as pool:
        resolved_services = list(pool.map(lambda s: resolve_dns(s, families), services))
        backends = [b for rs in resolved_services for b in rs.backends]
        results = list(pool.map(lambda b: probe_backend(b, ca_credentials, timeout), backends))

    availabilities = generate_availabilities(resolved_services, results, families)
    if output_content == "prom-metrics":
        metrics = render(resolved_services, results, availabilities)
        if prom_output_file is None:
            click.echo(metrics)
        else:
            write_atomic(prom_output_file, metrics)
    elif output_content == "availability":
        print_dataclasses(availabilities, display_config=obj.config.display, title="Availability")
    elif output_content == "probe":
        print_dataclasses(results, display_config=obj.config.display, title="Probes Result")

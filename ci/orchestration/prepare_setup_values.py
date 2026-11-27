#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Prepare matching setup and daemon values, including existing runner database profiles."""

import argparse
import json
from pathlib import Path
from typing import Any

import yaml


def merge_values(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merge_values(base[key], value)
        else:
            base[key] = value
    return base


def read_values(path: Path) -> dict[str, Any]:
    with path.open() as stream:
        values = yaml.safe_load(stream)
    if not isinstance(values, dict):
        raise TypeError(f"Expected a YAML mapping in {path}")
    return values


def apply_profile(values: dict[str, Any], component: str, profile: dict[str, Any]) -> None:
    """Translate profiles installed on CI runners without changing those external files."""
    backend = profile["backend"]
    connection = values[f"{component}Reset"]["connection"]
    databases = values["postgres"]["databases"]
    databases[:] = [database for database in databases if database["secretName"] != connection["secretName"]]

    if component == "scheduler":
        if backend not in {"postgres", "ceph", "vfs"}:
            raise ValueError(f"Unsupported scheduler backend: {backend}")
        values["objectstore"]["enabled"] = backend != "postgres"
        values["objectstore"]["backend"] = backend if backend != "postgres" else ""
        if backend == "ceph":
            values["objectstore"]["ceph"].update(profile["cephConfig"])
            return
        if backend == "vfs":
            config = profile.get("vfsConfig", {})
            vfs = values["objectstore"]["vfs"]
            for key in ("size", "accessModes", "annotations"):
                if key in config:
                    vfs[key] = config[key]
            if "storageClass" in config:
                # The existing profile format uses "-" to disable dynamic provisioning.
                vfs["storageClassName"] = "" if config["storageClass"] == "-" else config["storageClass"]
            return

    if backend == "postgres":
        config = profile["postgresConfig"]
        if not config.get("remote", False):
            databases.append(
                {
                    "name": f"cta-{component}-db",
                    "serviceName": config["server"],
                    "database": config["database"],
                    "username": config["username"],
                    **connection,
                }
            )
            return
        url = (
            f"postgresql:postgresql://{config['username']}:{config['password']}@{config['server']}/{config['database']}"
        )
    elif component == "catalogue" and backend == "oracle":
        config = profile["oracleConfig"]
        url = f"oracle:{config['username']}/{config['password']}@{config['database']}"
    else:
        raise ValueError(f"Unsupported {component} backend: {backend}")

    values.setdefault("extraObjects", []).append(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": connection["secretName"]},
            "type": "Opaque",
            "stringData": {connection["secretKey"]: url},
        }
    )


def runtime_values(values: dict[str, Any]) -> dict[str, Any]:
    # Daemons need connection references and backend metadata, not database credentials in their values.
    objectstore = values["objectstore"]
    ceph = objectstore["ceph"]
    return {
        "global": {
            "catalogue": values["catalogueReset"]["connection"],
            "scheduler": values["schedulerReset"]["connection"],
            "objectstore": {
                "enabled": objectstore["enabled"],
                "backend": objectstore["backend"],
                "ceph": {key: ceph[key] for key in ("id", "pool", "namespace")},
            },
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ci-setup-config", type=Path, required=True)
    parser.add_argument("--catalogue-config", type=Path)
    parser.add_argument("--scheduler-config", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    defaults = Path(__file__).parent / "helm/ci-setup/values.yaml"
    values = merge_values(read_values(defaults), read_values(args.ci_setup_config))
    for component in ("catalogue", "scheduler"):
        if path := getattr(args, f"{component}_config"):
            apply_profile(values, component, read_values(path))

    for name, data in (("setup-values.json", values), ("cta-values.json", runtime_values(values))):
        path = args.output_dir / name
        path.touch(mode=0o600, exist_ok=False)
        path.write_text(json.dumps(data))

    print(values["objectstore"]["backend"] if values["objectstore"]["enabled"] else "postgres")


if __name__ == "__main__":
    main()

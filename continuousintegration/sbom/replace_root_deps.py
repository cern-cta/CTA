#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import sys
import argparse


def get_root_component(sbom):
    meta_comp = (sbom.get("metadata") or {}).get("component") or {}
    project_ref = meta_comp.get("bom-ref")
    if not project_ref:
        print("ERROR: metadata.component.bom-ref not found", file=sys.stderr)
        sys.exit(1)
    return project_ref


def replace_root_dependencies(sbom, prefix) -> None:
    components = sbom.get("components") or []
    dependencies = sbom.get("dependencies") or []
    print("test with python trigger")

    project_ref = get_root_component(sbom)
    ref2comp = {c.get("bom-ref"): c for c in components if c.get("bom-ref")}
    ref2deps = {d.get("ref"): list(d.get("dependsOn") or []) for d in dependencies if d.get("ref")}

    # Find OS component dependency entry
    os_ref = next((c.get("bom-ref") for c in components if c.get("type") == "operating-system"), None)
    os_dep_refs = (ref2deps.get(os_ref) or []) if os_ref else []

    # Find OS deps starting with given prefix
    selected = []
    for ref in os_dep_refs:
        comp = ref2comp.get(ref)
        name = (comp or {}).get("name", "")
        if name.lower().startswith(prefix):
            selected.append(ref)

    # Replace main component entries with those found from OS entry
    dependencies = [d for d in dependencies if d.get("ref") != project_ref]
    dependencies.insert(0, {"ref": project_ref, "dependsOn": sorted(set(selected))})
    sbom["dependencies"] = dependencies


def main():
    ap = argparse.ArgumentParser(
        description="Update the dependencies of the root component to contain all dependencies of the OS component starting with a given prefix"
    )
    ap.add_argument("--in", dest="sbom_in", required=True, help="Input CycloneDX JSON")
    ap.add_argument("--out", dest="sbom_out", required=True, help="Output CycloneDX JSON")
    ap.add_argument(
        "--prefix", dest="prefix", required=True, help="Keep only OS deps with names starting with this prefix"
    )
    args = ap.parse_args()

    with open(args.sbom_in, "r", encoding="utf-8") as f:
        sbom = json.load(f)

    replace_root_dependencies(sbom, args.prefix.lower())

    with open(args.sbom_out, "w", encoding="utf-8") as f:
        json.dump(sbom, f, indent=2)
        f.write("\n")


if __name__ == "__main__":
    main()

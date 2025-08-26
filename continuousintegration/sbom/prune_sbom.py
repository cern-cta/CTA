#!/usr/bin/env python3
import json
import sys
import argparse
from collections import deque
from typing import Any

def get_root_component(sbom) -> str:
    meta_comp = (sbom.get("metadata") or {}).get("component") or {}
    project_ref = meta_comp.get("bom-ref")
    if not project_ref:
        print("ERROR: metadata.component.bom-ref not found", file=sys.stderr)
        sys.exit(1)
    return project_ref

def find_reachable_components(root_component: str, ref2deps: dict[str, list[str]]) -> set[str]:
    reachable_refs: set[str] = set([root_component])
    q = deque([root_component])
    while q:
        ref = q.popleft()
        for dep in ref2deps.get(ref, []):
            if dep not in reachable_refs:
                reachable_refs.add(dep)
                q.append(dep)
    return reachable_refs

def prune_unreachable(sbom: dict[str, Any]):
    components = sbom.get("components") or []
    dependencies = sbom.get("dependencies") or []

    project_ref: str = get_root_component(sbom)
    ref2deps: dict[str, list[str]] = {d.get("ref"): list(d.get("dependsOn") or []) for d in dependencies if d.get("ref")}

    # Find all reachable components
    reachable_refs: set[str] = find_reachable_components(project_ref, ref2deps)

    # Remove all non-reachable dependencies
    pruned_deps = []
    for ref, deps_list in ref2deps.items():
        if ref in reachable_refs:
            pruned_deps.append({"ref": ref, "dependsOn": [d for d in deps_list if d in reachable_refs]})

    # Remove all non-reachable components
    pruned_components = [c for c in components if c.get("bom-ref") in reachable_refs]

    # write back
    sbom["components"] = pruned_components
    sbom["dependencies"] = pruned_deps


def main():
    ap = argparse.ArgumentParser(description="Prune any components and dependencies unreachable from the main component")
    ap.add_argument("--in", dest="sbom_in", required=True, help="Input CycloneDX JSON")
    ap.add_argument("--out", dest="sbom_out", required=True, help="Output CycloneDX JSON")
    args = ap.parse_args()

    with open(args.sbom_in, "r", encoding="utf-8") as f:
        sbom = json.load(f)

    prune_unreachable(sbom)

    with open(args.sbom_out, "w", encoding="utf-8") as f:
        json.dump(sbom, f, indent=2)
        f.write("\n")

if __name__ == "__main__":
    main()

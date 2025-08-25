#!/usr/bin/env python3
import json
import sys
import argparse
from collections import deque

def main():
    ap = argparse.ArgumentParser(description="Retarget project deps to subset of OS deps and prune orphans")
    ap.add_argument("--in", dest="sbom_in", required=True, help="Input CycloneDX JSON")
    ap.add_argument("--out", dest="sbom_out", required=True, help="Output CycloneDX JSON")
    ap.add_argument("--prefix", dest="prefix", required=True, help="Keep only OS deps with names starting with this prefix")
    args = ap.parse_args()

    with open(args.sbom_in, "r", encoding="utf-8") as f:
        sbom = json.load(f)

    components = sbom.get("components") or []
    dependencies = sbom.get("dependencies") or []

    # 1) find main/root component (from metadata)
    meta_comp = (sbom.get("metadata") or {}).get("component") or {}
    project_ref = meta_comp.get("bom-ref")
    if not project_ref:
        print("ERROR: metadata.component.bom-ref not found", file=sys.stderr)
        sys.exit(1)

    # quick indexes
    ref2comp = {c.get("bom-ref"): c for c in components if c.get("bom-ref")}
    ref2deps = {d.get("ref"): list(d.get("dependsOn") or []) for d in dependencies if d.get("ref")}

    # 2) find OS component dependency node
    os_ref = next((c.get("bom-ref") for c in components if c.get("type") == "operating-system"), None)
    os_dep_refs = (ref2deps.get(os_ref) or []) if os_ref else []

    # 3) subset: OS deps whose component name starts with prefix
    pref = args.prefix.lower()
    selected = []
    for ref in os_dep_refs:
        comp = ref2comp.get(ref)
        name = (comp or {}).get("name", "")
        if name.lower().startswith(pref):
            selected.append(ref)

    # 4) replace/insert project's dependency entry
    dependencies = [d for d in dependencies if d.get("ref") != project_ref]
    dependencies.insert(0, {"ref": project_ref, "dependsOn": sorted(set(selected))})

    # rebuild ref->deps after modification
    ref2deps = {d.get("ref"): list(d.get("dependsOn") or []) for d in dependencies if d.get("ref")}

    # 5) compute transitive closure reachable from project_ref
    reachable = set([project_ref])
    q = deque([project_ref])
    while q:
        ref = q.popleft()
        for dep in ref2deps.get(ref, []):
            if dep not in reachable:
                reachable.add(dep)
                q.append(dep)

    # 6) prune dependencies to reachable set; also filter their dependsOn to reachable
    pruned_deps = []
    for ref, deps_list in ref2deps.items():
        if ref in reachable:
            pruned_deps.append({"ref": ref, "dependsOn": [d for d in deps_list if d in reachable]})
    # Ensure project node exists even if it has empty dependsOn
    if project_ref not in {d["ref"] for d in pruned_deps}:
        pruned_deps.insert(0, {"ref": project_ref, "dependsOn": []})

    # 7) prune components to reachable set only
    pruned_components = [c for c in components if c.get("bom-ref") in reachable or c.get("type") == "operating-system" and os_ref in reachable]

    # write back
    sbom["components"] = pruned_components
    sbom["dependencies"] = pruned_deps

    with open(args.sbom_out, "w", encoding="utf-8") as f:
        json.dump(sbom, f, indent=2)
        f.write("\n")

if __name__ == "__main__":
    main()

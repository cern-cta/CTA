#!/usr/bin/env python3
import json
import sys
import argparse

def main():
    ap = argparse.ArgumentParser(description="Retarget project deps to a subset of OS deps")
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

    # quick index: bom-ref -> component
    ref2comp = {c.get("bom-ref"): c for c in components if c.get("bom-ref")}

    # 2) find the OS component's dependency node
    os_ref = next((c.get("bom-ref") for c in components if c.get("type") == "operating-system"), None)
    os_dep = next((d for d in dependencies if os_ref and d.get("ref") == os_ref), None)
    os_dep_refs = os_dep.get("dependsOn", []) if os_dep else []

    # 3) build filtered list: OS deps whose component name starts with prefix
    pref = args.prefix.lower()
    selected = []
    for ref in os_dep_refs:
        comp = ref2comp.get(ref)
        name = (comp or {}).get("name", "")
        if name.lower().startswith(pref):
            selected.append(ref)

    # replace (or insert) project's dependency entry
    dependencies = [d for d in dependencies if d.get("ref") != project_ref]
    dependencies.insert(0, {"ref": project_ref, "dependsOn": sorted(set(selected))})

    sbom["dependencies"] = dependencies

    with open(args.sbom_out, "w", encoding="utf-8") as f:
        json.dump(sbom, f, indent=2)
        f.write("\n")

if __name__ == "__main__":
    main()

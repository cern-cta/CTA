# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import argparse
import json
from typing import Any


def augment_metadata(
    sbom: dict[str, Any],
    *,
    component_name: str,
    component_version: str,
    author_name: str,
    author_email: str,
    project_url: str,
) -> None:
    metadata = sbom.setdefault("metadata", {})

    author = {"name": author_name, "email": author_email}
    authors = metadata.setdefault("authors", [])
    if author not in authors:
        authors.append(author)

    supplier = {"name": "CERN"}
    metadata["supplier"] = supplier

    lifecycles = metadata.setdefault("lifecycles", [])
    if not any(lifecycle.get("phase") == "post-build" for lifecycle in lifecycles):
        lifecycles.append({"phase": "post-build"})

    component = metadata.setdefault("component", {})
    component["type"] = "container"
    component["name"] = component_name
    component["version"] = component_version
    component["supplier"] = supplier

    external_reference = {"type": "vcs", "url": project_url}
    external_references = component.setdefault("externalReferences", [])
    if external_reference not in external_references:
        external_references.append(external_reference)


def main() -> None:
    def non_empty(value: str) -> str:
        if not value:
            raise argparse.ArgumentTypeError("value must not be empty")
        return value

    parser = argparse.ArgumentParser(description="Add image metadata to a CycloneDX SBOM")
    parser.add_argument("--in", dest="sbom_in", required=True, type=non_empty, help="Input CycloneDX JSON")
    parser.add_argument("--out", dest="sbom_out", required=True, type=non_empty, help="Output CycloneDX JSON")
    parser.add_argument("--component-name", required=True, type=non_empty)
    parser.add_argument("--component-version", required=True, type=non_empty)
    parser.add_argument("--author-name", required=True, type=non_empty)
    parser.add_argument("--author-email", required=True, type=non_empty)
    parser.add_argument("--project-url", required=True, type=non_empty)
    args = parser.parse_args()

    with open(args.sbom_in, encoding="utf-8") as input_file:
        sbom = json.load(input_file)

    augment_metadata(
        sbom,
        component_name=args.component_name,
        component_version=args.component_version,
        author_name=args.author_name,
        author_email=args.author_email,
        project_url=args.project_url,
    )

    with open(args.sbom_out, "w", encoding="utf-8") as output_file:
        json.dump(sbom, output_file, indent=2)
        output_file.write("\n")


if __name__ == "__main__":
    main()

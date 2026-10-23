#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Download SonarCloud issues using only the Python standard library."""

import argparse
import json
from pathlib import Path, PurePosixPath
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


REPOSITORY = Path(__file__).resolve().parent.parent
API_URL = "https://sonarcloud.io/api/issues/search"
QUALITY_GATE_URL = "https://sonarcloud.io/api/qualitygates/project_status"
PAGE_SIZE = 500
SEARCH_LIMIT = 10000


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Download CTA SonarCloud issues as JSON (unresolved issues on the main branch by default).",
        epilog="Uses anonymous access to public SonarCloud projects.",
    )
    parser.add_argument("--project", default="cern-cta_CTA", help="SonarCloud project key (default: cern-cta_CTA)")
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--branch", help="Analyzed branch name (default: SonarCloud main branch)")
    selection.add_argument("--pull-request", help="Analyzed pull request ID")
    parser.add_argument("--dir", help="Repository-relative directory, including descendants (filtered locally)")
    parser.add_argument("--language", help="Comma-separated Sonar language keys, e.g. cpp,c,py (API filter)")
    parser.add_argument("--file-type", help="Comma-separated file extensions, e.g. hpp,cpp or .py (filtered locally)")
    parser.add_argument(
        "--quality-gate-only",
        action="store_true",
        help="Only unresolved issues associated with failed quality-gate conditions",
    )
    parser.add_argument("--include-resolved", action="store_true", help="Also download resolved issues")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=REPOSITORY / "tmp/sonarcloud-issues",
        help="Output directory (default: <repository>/tmp/sonarcloud-issues)",
    )
    args = parser.parse_args()

    if args.quality_gate_only and args.include_resolved:
        parser.error("--quality-gate-only cannot be combined with --include-resolved")

    if args.dir is not None:
        directory = PurePosixPath(args.dir)
        if directory.is_absolute() or ".." in directory.parts or "\\" in args.dir:
            parser.error("--dir must be a repository-relative path without .., using / separators")
        args.dir = directory.as_posix()

    args.extensions = set()
    if args.file_type is not None:
        for value in args.file_type.split(","):
            extension = value.strip().lstrip(".")
            if not extension or any(character in extension for character in "/\\*?"):
                parser.error("--file-type requires extensions such as hpp,cpp (no globs)")
            args.extensions.add("." + extension)
    return args


def request_page(params, url=API_URL):
    # Only the fixed HTTPS API endpoints are supported.
    if url not in (API_URL, QUALITY_GATE_URL):
        raise ValueError("Unsupported SonarCloud API endpoint")
    headers = {"Accept": "application/json"}
    request = Request(url + "?" + urlencode(params), headers=headers)  # noqa: S310 - endpoint allowlisted above
    try:
        with urlopen(request, timeout=60) as response:  # noqa: S310 - endpoint allowlisted above
            return json.load(response)
    except HTTPError as error:
        hints = {
            401: "This script requires a publicly accessible SonarCloud project.",
            403: "Anonymous access was denied; check that the project is public.",
            429: "SonarCloud rate limit reached; retry in a few minutes.",
        }
        raise RuntimeError(
            f"SonarCloud returned HTTP {error.code}. " + hints.get(error.code, "Check the project and filters.")
        ) from error


def download_issues(params):
    issues = []
    components = {}
    rules = {}
    page = 1
    while True:
        result = request_page(dict(params, p=page, ps=PAGE_SIZE, additionalFields="rules"))
        total = result["paging"]["total"]
        # Fail rather than silently exporting only SonarCloud's first 10,000 results.
        if total > SEARCH_LIMIT:
            raise RuntimeError(
                f"SonarCloud matched {total} issues, exceeding its {SEARCH_LIMIT}-result search limit. "
                "Narrow the query with --language or omit --include-resolved. "
                "--dir and --file-type are local filters and do not reduce this limit."
            )
        batch = result["issues"]
        issues.extend(batch)
        components.update((item["key"], item) for item in result.get("components", []))
        rules.update((item["key"], item) for item in result.get("rules", []))
        print(f"Downloaded {len(issues)}/{total} issues...", file=sys.stderr)
        if len(issues) >= total:
            break
        if not batch:
            raise RuntimeError("SonarCloud returned an empty page before all issues were downloaded; retry.")
        page += 1
    return issues, components, rules


def quality_gate_query(condition, pull_request):
    """Map supported aggregate metrics to contributing issue categories, not individual blame."""
    metric = condition["metricKey"]
    new_code = metric.startswith("new_")
    metric = metric[4:] if new_code else metric
    types = {
        "reliability_rating": "BUG",
        "bugs": "BUG",
        "security_rating": "VULNERABILITY",
        "vulnerabilities": "VULNERABILITY",
        "maintainability_rating": "CODE_SMELL",
        "sqale_rating": "CODE_SMELL",
        "sqale_index": "CODE_SMELL",
        "sqale_debt_ratio": "CODE_SMELL",
        "code_smells": "CODE_SMELL",
    }
    qualities = {
        "software_quality_reliability_rating": "RELIABILITY",
        "software_quality_security_rating": "SECURITY",
        "software_quality_maintainability_rating": "MAINTAINABILITY",
    }
    if metric in types:
        query = {"types": types[metric]}
    elif metric in qualities:
        query = {"impactSoftwareQualities": qualities[metric]}
    elif metric in ("violations", "issues"):
        query = {}
    else:
        return None

    # Pull request searches already select changes relative to the target branch.
    if new_code and not pull_request:
        query["sinceLeakPeriod"] = "true"
    return query


def download_gate_issues(args, params):
    gate_params = {"projectKey": args.project}
    for key in ("branch", "pullRequest"):
        if key in params:
            gate_params[key] = params[key]
    gate = request_page(gate_params, QUALITY_GATE_URL)["projectStatus"]
    if gate["status"] not in ("OK", "ERROR"):
        raise RuntimeError("Quality gate has not been computed for this selection.")

    issues, components, rules = {}, {}, {}
    unsupported = []
    queries = set()
    for condition in gate.get("conditions", []):
        if condition["status"] != "ERROR":
            continue
        query = quality_gate_query(condition, args.pull_request)
        if query is None:
            unsupported.append(condition["metricKey"])
            continue
        # Overlapping failed conditions should not duplicate requests or exported issues.
        signature = tuple(sorted(query.items()))
        if signature in queries:
            continue
        queries.add(signature)
        batch, batch_components, batch_rules = download_issues(dict(params, **query))
        issues.update((issue["key"], issue) for issue in batch)
        components.update(batch_components)
        rules.update(batch_rules)

    if unsupported:
        print("Warning: failed conditions without an issue mapping: " + ", ".join(unsupported), file=sys.stderr)
    metadata = {
        "projectStatus": gate,
        "unmappedFailedConditions": unsupported,
        "selection": "Unresolved issues associated with failed metric categories; not individual gate verdicts.",
    }
    return list(issues.values()), components, rules, metadata


def matches_path(path, directory, extensions):
    if directory and directory != "." and (not path or not path.startswith(directory + "/")):
        return False
    return not extensions or bool(path and any(path.endswith(extension) for extension in extensions))


def confirm_replacement(existing):
    """Ask before replacing an existing file, keeping cancellation as the default."""
    if existing.is_dir():
        raise RuntimeError(f"Expected a file, but found a directory: {existing}")
    try:
        answer = input(f"Remove existing file {existing.absolute()} and replace it? [y/N] ")
    except EOFError:
        answer = ""
    return answer.strip().lower() in ("y", "yes")


def main():
    args = parse_arguments()
    destination = args.output / "issues.json"
    existing = destination if args.output.is_dir() else args.output
    replace_existing = existing.exists() or existing.is_symlink()
    if replace_existing and not confirm_replacement(existing):
        print("Cancelled; existing output was kept.")
        return

    params = {"componentKeys": args.project}
    if args.branch:
        params["branch"] = args.branch
    if args.pull_request:
        params["pullRequest"] = args.pull_request
    if args.language:
        params["languages"] = args.language
    if not args.include_resolved:
        params["resolved"] = "false"

    gate = None
    if args.quality_gate_only:
        issues, components, rules, gate = download_gate_issues(args, params)
    else:
        issues, components, rules = download_issues(params)
    selected = []
    for issue in issues:
        path = components.get(issue["component"], {}).get("path")
        if matches_path(path, args.dir, args.extensions):
            selected.append(issue)

    # Preserve raw issues and their component/rule metadata for downstream tooling.
    component_keys = {issue["component"] for issue in selected}
    rule_keys = {issue["rule"] for issue in selected}
    payload = {
        "project": args.project,
        "branch": args.branch,
        "pullRequest": args.pull_request,
        "filters": {
            "directory": args.dir,
            "language": args.language,
            "fileTypes": sorted(args.extensions),
            "includeResolved": args.include_resolved,
            "qualityGateOnly": args.quality_gate_only,
        },
        "downloadedTotal": len(issues),
        "total": len(selected),
        "issues": selected,
        "components": [value for key, value in components.items() if key in component_keys],
        "rules": [value for key, value in rules.items() if key in rule_keys],
    }
    if gate is not None:
        payload["qualityGate"] = gate

    # Keep existing output until the download and serialization have succeeded.
    content = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    if replace_existing:
        existing.unlink()
    args.output.mkdir(parents=True, exist_ok=True)
    destination.write_text(content, encoding="utf-8")
    print(f"Saved {len(selected)} issues to {destination}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, URLError, ValueError, KeyError, RuntimeError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)

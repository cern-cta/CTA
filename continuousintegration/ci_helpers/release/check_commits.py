#!/bin/python3

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

import sys
from datetime import datetime
import argparse
from gitlabapi import GitLabAPI, Commit
import re


class ColorCodes:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    ERROR = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


changelog_cats: list[str] = ["feature", "bug", "maintenance", "documentation", "other"]

# ------------------------------------------------------------------------------


def changelog_preview(
    api: GitLabAPI, from_commit_sha: str, to_commit_sha: str | None, release_version: str, markdown: bool = False
) -> str:
    if to_commit_sha is not None:
        to_commit_sha = "HEAD"
    params: dict = {
        "from": from_commit_sha,
        "to": to_commit_sha,
        "version": release_version,
    }
    changelog_update = api.get("repository/changelog", params)

    res = header("CHANGELOG UPDATE PREVIEW", markdown)
    res += changelog_update["notes"]
    return res


def get_commits_in_range(api: GitLabAPI, since_sha: str, until_sha: str | None) -> list[Commit]:
    commit_range: str = since_sha
    if until_sha is not None:
        commit_range += ".." + until_sha
    # The GitLab API only allows for the retrieval of max 100 entries per page
    per_page: int = 100
    params: dict = {
        "ref_name": commit_range,
        "trailers": True,
        "per_page": per_page,
    }
    res = api.get("repository/commits", params=params)
    if res is not None and len(res) >= per_page:
        print("WARNING: you have retrieved the maximum commits per page. Not all commits might be checked.")
    return res


def get_commit(api: GitLabAPI, commit_sha: str) -> Commit | None:
    return api.get(f"repository/commits/{commit_sha}")


# ------------------------------------------------------------------------------


def is_valid_issue_ref(issue: str) -> bool:
    # Accepts e.g. #123, CTA#123, cta/CTA#123
    return re.match(r"^((?:\w+/)?\w+)?#\d+", issue) is not None


def generate_report(commits: list[Commit], verbose: bool) -> dict[str, list[str]]:
    report: dict[str, list[str]] = {
        "errors": [],
        "warnings": [],
        "success": [],
    }
    for commit in commits:
        trailers = commit["trailers"]
        commit_id = commit["short_id"]
        if not trailers:
            err_string: str = f"({commit_id}) Commit does not have any trailers"
            if verbose:
                err_string += f"\n{commit_summary(commit)}"
            report["errors"].append(err_string)
            continue
        if "Changelog" not in trailers:
            err_string: str = f"({commit_id}) Commit does not have the Changelog trailer"
            if verbose:
                err_string += f"\n{commit_summary(commit)}"
            report["errors"].append(err_string)
            continue
        changelog_type = trailers["Changelog"]
        if changelog_type not in changelog_cats:
            warn_string: str = f"({commit_id}) Commit has an unsupported Changelog trailer type"
            if verbose:
                warn_string += f"\n{commit_summary(commit)}"
            report["warnings"].append(warn_string)
            continue
        if "Issue" not in trailers:
            warn_string: str = f"({commit_id}) Commit does not have the Issue trailer"
            if verbose:
                warn_string += f"\n{commit_summary(commit)}"
            report["warnings"].append(warn_string)
            continue
        issue = trailers["Issue"]
        if not is_valid_issue_ref(issue):
            warn_string: str = f"({commit_id}) Commit does not have a correct issue reference"
            if verbose:
                warn_string += f"\n{commit_summary(commit)}"
            report["warnings"].append(warn_string)
            continue
        report["success"].append(f"({commit_id}) {commit["title"]}")
    return report


# ------------------------------------------------------------------------------
def divider() -> str:
    return f"\n{'-'*42}\n"


def header(title: str, markdown: bool) -> str:
    if markdown:
        return f"# {title}\n\n"
    res = divider()
    res += f"{ColorCodes.BOLD}{title}{ColorCodes.ENDC}\n"
    res += divider()
    return res


def commit_summary(commit: Commit) -> str:
    return f"\t- Title: {commit["title"]}\n\t- Trailers: {commit["trailers"]}"


def report_summary(report: dict[str, list[str]], markdown: bool = False) -> str:
    num_errors: int = len(report["errors"])
    num_warnings: int = len(report["warnings"])
    num_success: int = len(report["success"])
    total_commits = num_errors + num_warnings + num_success

    res = header("REPORT", markdown)

    if markdown:
        res += f"**ERRORS ({num_errors})**\n"
    else:
        res += f"{ColorCodes.ERROR}Errors ({num_errors}){ColorCodes.ENDC}\n"
    res += "\n".join(["- " + error for error in report["errors"]])
    res += "\n\n"

    if markdown:
        res += f"**WARNINGS ({num_warnings})**\n"
    else:
        res += f"{ColorCodes.WARNING}WARNINGS ({num_warnings}){ColorCodes.ENDC}"
    res += "\n".join(["- " + warning for warning in report["warnings"]])
    res += "\n\n"

    if markdown:
        res += f"**OK ({num_success})**\n"
    else:
        res += f"{ColorCodes.OKGREEN}OK ({num_success}){ColorCodes.ENDC}"
    res += "\n".join(["- " + success for success in report["success"]])
    res += "\n\n"

    res += header("SUMMARY", markdown)
    if markdown:
        res += f"- Errors: {num_errors}\n"
        res += f"- Warnings: {num_warnings}\n"
        res += f"- Ok: {num_success}\n"
        ok_commits: int = num_success + num_warnings
        res += f"\n**{ok_commits}/{total_commits} commits will be added to the changelog**\n\n"
    else:
        res += f"- {ColorCodes.ERROR}Errors:{ColorCodes.ENDC} {num_errors}\n"
        res += f"- {ColorCodes.WARNING}Warnings:{ColorCodes.ENDC} {num_warnings}\n"
        res += f"- {ColorCodes.OKGREEN}Ok:{ColorCodes.ENDC} {num_success}\n"
        ok_commits: int = num_success + num_warnings
        res += f"\n{ColorCodes.BOLD} \
                {ok_commits}/{total_commits} commits will be added to the changelog \
                {ColorCodes.ENDC}\n\n"
    return res


def commit_range_summary(api: GitLabAPI, from_commit_sha: str, to_commit_sha: str) -> str:

    from_commit: Commit | None = get_commit(api, from_commit_sha)
    to_commit: Commit | None = get_commit(api, to_commit_sha)
    if from_commit is None:
        print(f"Failure: {from_commit_sha} is not a valid commit")
        sys.exit(1)
    if to_commit is None:
        print(f"Failure: {to_commit_sha} is not a valid commit")
        sys.exit(1)
    if datetime.fromisoformat(from_commit["authored_date"]) > datetime.fromisoformat(to_commit["authored_date"]):
        print(f"Failure: to-commit cannot be before from-commit")
        sys.exit(1)
    res = f"Start commit (exclusive): ({from_commit["short_id"]}) {from_commit["title"]}\n"
    res += f"End commit   (inclusive): ({to_commit["short_id"]}) {to_commit["title"]}\n"
    return res


def commit_list_summary(commits: list[Commit]) -> str:
    res: str = f"\nFound {len(commits)} commits:\n"
    for commit in commits:
        res += f"- ({commit["short_id"]}) {commit["title"]}\n"
    return res


# ------------------------------------------------------------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Tool that checks which commits will end up in the changelog")
    # The reason for not including this commit in the changelog is consistency with the gitlab api:
    # https://docs.gitlab.com/ee/api/repositories.html
    parser.add_argument(
        "--from",
        dest="from_commit",
        required=True,
        help="The commit or tag to start generating the changelog from. This commit is NOT included in the changelog",
    )
    parser.add_argument(
        "--to",
        dest="to_commit",
        default="HEAD",
        help="The commit or tag (inclusive) up to which to generate the changelog. \
              If not provided, will use the latest commit. This commit is included in the changelog",
    )
    parser.add_argument(
        "-p",
        "--preview",
        action="store_true",
        help="Outputs a preview of the changelog",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Outputs verbose output",
    )
    parser.add_argument(
        "--markdown-output",
        required=True,
        help="Outputs everything in markdown format to the provided file",
    )
    parser.add_argument(
        "--api-url",
        required=True,
        help='The GitLab API url. E.g. "https://gitlab.cern.ch"',
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="The GitLab project id of the repository in question",
    )
    parser.add_argument(
        "--api-token",
        required=True,
        help="The api token needed to execute API requests",
    )
    parser.add_argument(
        "--release-version",
        required=True,
        help="The version or tag number of the new release. \
              The tag/release does not need to exist yet; this is purely cosmetic. \
              Must follow semantic versioning",
    )

    args = parser.parse_args()

    release_version: str = args.release_version
    if release_version.startswith("v"):
        print("Removing 'v' prefix from release version...")
        release_version = release_version[1:]

    api: GitLabAPI = GitLabAPI(args.api_url, args.project_id, args.api_token)

    print(commit_range_summary(api, args.from_commit, args.to_commit))

    commits: list[Commit] = get_commits_in_range(api, args.from_commit, args.to_commit)
    if args.verbose:
        print(commit_list_summary(commits))

    report = generate_report(commits, args.verbose)
    print(report_summary(report))

    if args.preview:
        print(changelog_preview(api, args.from_commit, args.to_commit, release_version))

    if args.markdown_output:
        md: str = commit_range_summary(api, args.from_commit, args.to_commit)
        md += commit_list_summary(commits)
        md += report_summary(report, markdown=True)
        md += changelog_preview(api, args.from_commit, args.to_commit, release_version, markdown=True)

        with open(args.markdown_output, "w") as file:
            file.write(md)

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

from datetime import datetime
import sys
from typing import Any, Union
import argparse
import base64
from gitlabapi import GitLabAPI
import textwrap
import time

# ------------------------------------------------------------------------------


# https://docs.gitlab.com/ee/api/branches.html#create-repository-branch
def create_new_branch(api: GitLabAPI, branch: str, source_branch: str) -> bool:
    print(f"Creating new branch: {branch} from source {args.source_branch}...")
    params: dict = {
        "branch": branch,
        "ref": source_branch,
    }
    result: str | None = api.post("repository/branches", params=params)
    if result is not None:
        print("Branch created successfully")
        print(f"\t To view the created branch, visit: {result["web_url"]}")
        return True
    else:
        print(f"Failed to create branch {branch} from source {args.source_branch}")
        return False


# https://docs.gitlab.com/ee/api/repositories.html#add-changelog-data-to-a-changelog-file
def update_changelog(api: GitLabAPI, release_version: str, from_commit: str, to_commit: str, branch: str) -> bool:
    print(f"Pushing CHANGELOG.MD updates to branch: {branch_name}...")
    print(f"\tRelease version: {release_version}")
    print(f"\tStart commit (exclusive): {from_commit}")
    print(f"\tEnd commit   (inclusive): {to_commit}")
    params: dict = {
        "branch": branch,
        "version": release_version,
        "from": from_commit,
        "to": to_commit,
    }
    result: str | None = api.post("repository/changelog", params=params)
    if result is not None:
        print("Changelog update pushed successfully")
        return True
    else:
        print(f"Failed to update changelog")
        return False


# https://docs.gitlab.com/ee/api/repository_files.html#get-file-from-repository
def get_file(api: GitLabAPI, path: str, ref: str) -> bool:
    params: dict = {
        "ref": ref,
    }
    return api.get(f"repository/files/{path}", params)


# https://docs.gitlab.com/ee/api/repository_files.html#update-existing-file-in-repository
def update_rpm_spec(api: GitLabAPI, spec_path: str, branch: str, content: str, commit_msg: str) -> bool:
    data: dict = {
        "branch": branch,
        "content": content,
        "commit_message": commit_msg,
    }
    result: str | None = api.put(f"repository/files/{spec_path}", json=data)
    if result is not None:
        print(f"Update of {spec_path} pushed successfully")
        return True
    else:
        print(f"Failed to update {spec_path}")
        return False


# https://docs.gitlab.com/ee/api/merge_requests.html#create-mr
# returns the id of the MR
def create_merge_request(
    api: GitLabAPI,
    source_branch: str,
    target_branch: str,
    title: str,
    description: str,
    reviewers: list[str],
    labels: list[str],
) -> Union[int, None]:
    print(f"Creating Merge Request from branch {source_branch} into branch: {target_branch}")
    print(f"\tTitle: {title}")
    data: dict = {
        "source_branch": source_branch,
        "target_branch": target_branch,
        "title": title,
        "description": description,
        "reviewer_ids": reviewers,
        "labels": labels,
        "allow_collaboration": True,
        "remove_source_branch": True,
        "squash": True,
    }
    result: str | None = api.post("merge_requests", json=data)
    if result is not None:
        print("Merge request created successfully")
        print(f"Merge request is ready for review. Please visit: {result["web_url"]}")
        return result["iid"]
    else:
        print(f"Failed to create merge request")
        return None


# https://docs.gitlab.com/ee/api/discussions.html#create-a-new-thread-in-the-merge-request-diff
def add_mr_review_comment(
    api: GitLabAPI,
    mr_id: str,
    file_path: str,
    line_number: str,
    comment: str,
) -> bool:
    print(f"Getting version for Merge Request with id: {mr_id}")
    tries: int = 0
    max_tries: int = 20
    versions: list[dict[str, Any]] | None = []
    # There seems to be a bit of a delay between the creation of the MR and this api request succeeding
    while versions == []:
        versions: dict | None = api.get(f"merge_requests/{mr_id}/versions")
        if versions is None:
            print("Failed to retrieve versions")
            return False
        if max_tries > 20:
            print("Failed to retrieve versions after {tries} tries.")
            return False
        tries += 1
        time.sleep(1)
    print(f"Adding review comment for {file_path} at line {line_number}:")
    print(f"{textwrap.indent(comment, "\t")}")
    data: dict = {
        "position[position_type]": "text",
        "position[base_sha]": versions[0]["base_commit_sha"],
        "position[head_sha]": versions[0]["head_commit_sha"],
        "position[start_sha]": versions[0]["start_commit_sha"],
        "position[new_path]": file_path,
        "position[old_path]": file_path,
        "position[new_line]": line_number,
        "body": comment,
    }
    result: str | None = api.post(f"merge_requests/{mr_id}/discussions", json=data)
    if result is not None:
        print("Comment added successfully")
        return True
    else:
        print(f"Failed to add review comments")
        return False


# ------------------------------------------------------------------------------


def find_line_number(content: str, section: str):
    # Find the position of the section
    section_index = content.find(section)
    if section_index == -1:
        raise ValueError(f"The section '{section}' was not found in the file.")

    # Count the number of newlines up to the section index to find the line number
    line_number = content.count("\n", 0, section_index) + 1
    return line_number


def insert_rpm_changelog_entry(
    api: GitLabAPI, branch: str, release_version: str, spec_path: str, changelog_entry: str
) -> int:
    file_info: dict[str, Any] | None = get_file(api, spec_path, branch)
    if file_info is None:
        sys.exit(1)
    content = base64.b64decode(file_info["content"]).decode("utf-8")

    # Find the position of the %changelog section
    changelog_section = "%changelog"
    changelog_index = content.find(changelog_section)

    if changelog_index == -1:
        raise ValueError(f"The section '{changelog_section}' was not found in the file.")

    # Insert the changelog entry after the %changelog section
    insertion_index = changelog_index + len(changelog_section) + 1
    updated_content = content[:insertion_index] + "\n" + changelog_entry + content[insertion_index:]

    print(f"Pushing {spec_path} changelog updates to branch: {branch_name}...")
    print(f"{textwrap.indent(changelog_entry, "\t")}")

    commit_msg: str = f"Added changelog entry for release: {release_version}"
    update_rpm_spec(api, spec_path, branch, updated_content, commit_msg)

    return find_line_number(updated_content, changelog_entry)


# ------------------------------------------------------------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Tool that checks which commits will end up in the changelog")
    # The reason for not including the "from" commit in the changelog is consistency with the gitlab api:
    # https://docs.gitlab.com/ee/api/repositories.html
    parser.add_argument(
        "--from",
        dest="from_commit",
        required=True,
        help="The commit or tag to start generating the changelog from. \
              This commit is NOT included in the changelog",
    )
    parser.add_argument(
        "--to",
        dest="to_commit",
        required=True,
        help="The commit or tag (inclusive) up to which to generate the changelog. \
              If not provided, will use the latest commit. This commit is included in the changelog",
    )
    parser.add_argument(
        "--release-version",
        required=True,
        help="The version or tag number of the new release. The tag/release does not need to exist yet. \
              Must follow semantic versioning",
    )
    parser.add_argument(
        "--source-branch",
        required=True,
        help="Branch from which to create the changelog branch. \
              This branch will also be used as the target branch for the changelog merge request",
    )
    parser.add_argument(
        "--reviewer",
        required=True,
        help="Id of the GitLab user that will be reviewing the merge request.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Outputs verbose output",
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
        "--description-file",
        required=True,
        help="A path to a file containing the description for the merge request.",
    )
    parser.add_argument(
        "--user-name",
        required=True,
        help="The full name of the person that should end up in the spec.in changelog.",
    )
    parser.add_argument(
        "--user-email",
        required=True,
        help="The email of the person that should end up in the spec.in changelog.",
    )

    args = parser.parse_args()

    # Set up
    api: GitLabAPI = GitLabAPI(args.api_url, args.project_id, args.api_token)

    release_version: str = args.release_version
    if release_version.startswith("v"):
        print("Removing 'v' prefix from release version...")
        release_version = release_version[1:]

    branch_name: str = args.release_version + "-changelog-update"
    with open(args.description_file, "r") as file:
        mr_description = file.read()
    if mr_description is None:
        print(f"Failed to read description file: {args.description_file}")
        sys.exit(1)

    # Create new branch
    success: bool = create_new_branch(api, branch=branch_name, source_branch=args.source_branch)
    if not success:
        sys.exit(1)

    # Update changelog
    success = update_changelog(
        api, release_version=release_version, from_commit=args.from_commit, to_commit=args.to_commit, branch=branch_name
    )
    if not success:
        sys.exit(1)

    # Update spec.in
    spec_path: str = "cta.spec.in"
    changelog_entry = (
        f"* {datetime.now().strftime("%a %b %d %Y")} {args.user_name} <{args.user_email}> - {release_version}\n"
    )
    changelog_entry += f"- See CHANGELOG.md for details\n"
    line_number: int = insert_rpm_changelog_entry(api, branch_name, release_version, spec_path, changelog_entry)

    # Create Merge Request
    mr_title: str = f"Changelog update for release {release_version}"
    reviewers: list[str] = [args.reviewer]
    labels: list[str] = ["workflow::in review"]
    mr_id: int | None = create_merge_request(
        api,
        source_branch=branch_name,
        target_branch=args.source_branch,
        title=mr_title,
        description=mr_description,
        reviewers=reviewers,
        labels=labels,
    )
    if mr_id is None:
        sys.exit(1)

    # Add a review comment to the Merge Request
    success = add_mr_review_comment(
        api, mr_id, spec_path, line_number + 1, "Please add any specific notes for this release here."
    )
    if not success:
        sys.exit(1)
    print(f"Script completed successfully.")

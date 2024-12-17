#!/usr/bin/env python3

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
# @license      This program is free software, distributed under the terms of
#               the GNU General Public Licence version 3 (GPL Version 3),
#               copied verbatim in the file "LICENSE".
#               You can redistribute it and/or modify it under the terms of the
#               GPL Version 3, or (at your option) any later version.
#
#               This program is distributed in the hope that it will be useful,
#               but WITHOUT ANY WARRANTY; without even the implied warranty of
#               MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#               See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and
#               immunities granted to it by virtue of its status as an
#               Intergovernmental Organization or submit itself to any
#               jurisdiction.

import re
import subprocess
import sys
import os
import json

# Dictionary containing the supported config of the different variables.
SUPPORTED = {
    "PIPELINE_TYPE": ["COMMIT",
                      "EOS_REGR_AGAINST_CTA_MAIN",
                      "EOS_REGR_AGAINST_CTA_TAG",
                      "CTAGENERIC_IMAGE"],
    "CUSTOM_XRD_TAG_REGEX": [ re.compile("^\d+:\d+\.\d+\.\d+(-\d+)*$") ],
    "CUSTOM_EOS_TAG_REGEX": [ re.compile("^\d+\.\d+\.\d+$") ],
    "CUSTOM_CTA_TAG_REGEX": [ re.compile("^v\d+\.\d+\.\d+\.\d+-\d+$"),
                       re.compile("^v\d+\.\d+\.\d+\.\d+-\d+\.el9$") # EL9 Transition format
                     ]
    # "SCHED_TYPE": ["objecstore",
    #                "pgsched"]
    #  ...
}

DEFINED_INPUT_VARS = [
    "BUILD_GENERATOR", "CMAKE_BUILD_TYPE", "CTA_VERSION",
    "SCHED_TYPE", "ORACLE_SUPPORT", "VALGRIND_DB_UNIT_TESTS",
    "SYSTEMTESTS_ONLY", "SYSTEMTESTS_IMAGETAG",
    "BASH_LOGGING_ENABLED", "PIPELINE_TYPE", "CUSTOM_XRD_TAG",
    "CUSTOM_EOS_TAG", "CUSTOM_CTA_TAG", "CTA_BUILD_ID",
    "CTA_PIPELINE_NAME", "GIT_STRATEGY", "GIT_SUBMODULE_STRATEGY",
    "GIT_DEPTH", "GIT_SUBMODULE_DEPTH", "IMAGE_EL9", "IMAGE_PYTHON3",
    "IMAGE_DOCKER_IMAGE_BUILDER", "IMAGE_CPPCHECK",
    "IMAGE_GITLAB_RELEASE_CLI"
]

def run_cmd(cmd):
    """
    Run a command in console. The function checks that the commands succeds.
    If the command fails the error will be printed and the script will exit.
    :param cmd: String representing the command to execute.
    :return: Stripped stdout of the command execution.
    """
    try:
        cmd_call = subprocess.run(cmd,
                                  shell = True,
                                  stdout = subprocess.PIPE,
                                  stderr = subprocess.PIPE,
                                  text = True,
                                  check = True,
                                  timeout = 120)
    except subprocess.TimeoutExpired as e:
        sys.exit(f"ERROR: Timeout reached for command: {cmd}")
    except subprocess.CalledProcessError as e:
        sys.exit(f"ERROR: Command failed: {e.stderr}")

    return cmd_call.stdout.strip()


def _check_remote_rpm(ci_input_vars, ci_var_tag_name, version_regex, remote_rpm_repo, rpm_name, rpm_ver):
    """"
    Internal function to check that the version of an RPM we are looking for is
    avaiable in the specified repo. It checks the needed variables are set and
    that the version matches a regex. If the command fails the program will exit.
    :param ci_input_vars: CI input variables dictionary received by the program.
    :param ci_var_tag_name: the key that contains the desired tag in `ci_input_vars`
    :param version_regex: the key that contains the desired tag in `ci_input_vars`
    :param remote_rpm_repo: list containing a representative name of the repository and the URL to the repository.
    :param rpm_name: RPM name to look for
    :param rpm_ver: RPM version to look for
    """
    # Check tag is supported
    if ci_var_tag_name not in ci_input_vars.keys():
        sys.exit(f"ERROR: tag version variable ${ci_var_tag_name} is not set")

    # Check specified RPM version matches any of the possible regexes.
    regex_matched = False
    for regex in SUPPORTED[version_regex]:
        if regex.match(ci_input_vars[ci_var_tag_name]):
            regex_matched = True
            break

    if not regex_matched:
        sys.exit(f"ERROR: specified tag {ci_input_vars[ci_var_tag_name]} "
                 f"does not match regex {SUPPORTED['CUSTOM_XRD_TAG_REGEX'].pattern}")

    # Check tag is available
    print(run_cmd(
        f"dnf list --showduplicates "
        f"--repofrompath {remote_rpm_repo[0]},{remote_rpm_repo[1]} {rpm_name}"
        f"| grep \"{rpm_ver}\""))
    tag_available = run_cmd(
        f"dnf list --showduplicates "
        f"--repofrompath {remote_rpm_repo[0]},{remote_rpm_repo[1]} {rpm_name}"
        f"| grep \"{rpm_ver}\" | wc -l"
    )

    if int(tag_available) < 1:
        sys.exit(f"ERROR: Could not find {rpm_name} version ")


def validate_default(ci_input_vars):
    """
    Validation for the default pipeline type.
    To be implemented by cta#962
    """
    pass


def validate_eos_regr_against_cta_main(ci_input_vars):
    """
    Validation for the pipeline type `EOS_REGR_AGAINST_CTA_MAIN`. Checks the XRootD and EOS
    RPM packages.
      - XRootD is fetched from the stable repo.
      - EOS version should be available in the testing repo as it comes from
        their latest tag.
   
    :param ci_input_vars: CI input variables dictionary received by the program.
    """
    print("Validating XRootD...")
    _check_remote_rpm(ci_input_vars,
                      "CUSTOM_XRD_TAG",
                      "CUSTOM_XRD_TAG_REGEX",
                      ["xrd", "https://xrootd.web.cern.ch/repo/stable/el9/x86_64"],
                      "xrootd-server",
                      ci_input_vars["CUSTOM_XRD_TAG"])
    print("XRootD validation passed")

    print("Validating EOS...")
    _check_remote_rpm(ci_input_vars,
                      "CUSTOM_EOS_TAG",
                      "CUSTOM_EOS_TAG_REGEX",
                      ["eos", "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/el-9/x86_64/"],
                      "eos-server",
                      ci_input_vars["CUSTOM_EOS_TAG"])
    print("EOS validation passed")


def validate_eos_regr_against_cta_tag(ci_input_vars):
    """
    Validation for the pipeline type `EOS_REGR_AGAINST_CTA_TAG`. Checks the XRootD, EOS
    and CTA RPM packages.
      - XRootD is fetched from the stable repo.
      - EOS version should be available in the testing repo as it comes from
        their latest tag.
      - CTA should be available in the testing public repository as the latest
        available version in there should match the latest version in production.

    :param ci_input_vars: CI input variables dictionary received by the program.
    """
    print("Validating XRootD...")
    _check_remote_rpm(ci_input_vars,
                      "CUSTOM_XRD_TAG",
                      "CUSTOM_XRD_TAG_REGEX",
                      ["xrd", "https://xrootd.web.cern.ch/repo/stable/el9/x86_64"],
                      "xrootd-server",
                      ci_input_vars["CUSTOM_XRD_TAG"])
    print("XRootD validation passed")

    print("Validating EOS...")
    _check_remote_rpm(ci_input_vars,
                      "CUSTOM_EOS_TAG",
                      "CUSTOM_EOS_TAG_REGEX",
                      ["eos", "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/el-9/x86_64/"],
                      "eos-server",
                      ci_input_vars["CUSTOM_EOS_TAG"])
    print("EOS validation passed")

    print("Validating CTA...")
    _check_remote_rpm(ci_input_vars,
                      "CUSTOM_CTA_TAG",
                      "CUSTOM_CTA_TAG_REGEX",
                      ["cta", "https://cta-public-repo.web.cern.ch/testing/cta-5/el9/cta/x86_64/"],
                      "cta-frontend",
                      ci_input_vars["CUSTOM_CTA_TAG"][1:])

    # Check that the XRootD version specified in the variables matches the
    # XRootD version that the RPMs where compiled against.
    # In the spec.in file we state that the version must match which causes a conflict
    # when installing the RPMs, we could do something similar to what EOS project does
    # `Requires: xrootd >= %{some_ver}` instead of `Requires: xrootd = %{some_ver}`
    # This way we could avoid this conflict. For now fail the pipeline if the versions
    # do not match.
    print("Checking CTA and EOS were compiled against the same XRootD version...")
    cta_xrd_ver = run_cmd(f"git checkout tags/{ci_input_vars['CUSTOM_CTA_TAG']} -- cta.spec.in && "
                         "grep 'xrootdVersion 1:5' cta.spec.in | awk '{print $3}' | "
                         "awk -F'-' '{print $1}'")

    if cta_xrd_ver != ci_input_vars["CUSTOM_XRD_TAG"]:
        sys.exit(f"ERROR: CTA was compiled for {cta_xrd_ver} while EOS requires {ci_input_vars['CUSTOM_XRD_TAG']}")
    print("CTA validation passed")


def validate_ctageneric_image(ci_input_vars):
    """
    Validation for the pipeline type `CTAGENERIC_IMAGE`. Checks that the desired
    XRootD, EOS and CTA RPM packages are available.
    """
    print("Validating XRootD...")
    _check_remote_rpm(ci_input_vars,
                      "CUSTOM_XRD_TAG",
                      "CUSTOM_XRD_TAG_REGEX",
                      ["xrd", "https://xrootd.web.cern.ch/repo/stable/el9/x86_64"],
                      "xrootd-server",
                      ci_input_vars["CUSTOM_XRD_TAG"])
    print("XRootD validation passed")

    print("Validating EOS...")
    _check_remote_rpm(ci_input_vars,
                      "CUSTOM_EOS_TAG",
                      "CUSTOM_EOS_TAG_REGEX",
                      ["eos", "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/el-9/x86_64/"],
                      "eos-server",
                      ci_input_vars["CUSTOM_EOS_TAG"])
    print("EOS validation passed")

    print("Validating CTA...")
    _check_remote_rpm(ci_input_vars,
                      "CUSTOM_CTA_TAG",
                      "CUSTOM_CTA_TAG_REGEX",
                      ["cta", "https://cta-public-repo.web.cern.ch/testing/cta-5/el9/cta/x86_64/"],
                      "cta-frontend",
                      ci_input_vars["CUSTOM_CTA_TAG"][1:])



def main():
    """
    Validate the varaibles received by the GitLab CI pipeline
    """

    # Get project defined CI variables from environment
    ci_input_vars = {}
    for var in DEFINED_INPUT_VARS:
        ci_input_vars[var] = os.environ[var]

    print(ci_input_vars)

    if ci_input_vars["PIPELINE_TYPE"] not in SUPPORTED["PIPELINE_TYPE"]:
        sys.exit(f"ERROR: Pipeline type {ci_input_vars['PIPELINE_TYPE']} not supported")

    # Execute the corresponding validation function passing as input
    # all other variables to check.
    print(f"Validating input for {ci_input_vars['PIPELINE_TYPE']} pipeline type")
    validate_func_name = "validate_" + ci_input_vars['PIPELINE_TYPE'].lower()
    globals()[validate_func_name](ci_input_vars)

    print("Validation was successful")


if __name__ == '__main__':
    main()

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

from pathlib import Path
import subprocess
import sys
import os
import json
import re

# Dictionary containing the supported config of the different variables.
SUPPORTED = {
    "PIPELINE_TYPE": ["DEFAULT",
                      "REGR_AGAINST_CTA_MAIN",
                      "REGR_AGAINST_CTA_VERSION",
                      "IMAGE_FROM_CTA_VERSION",
                      "SYSTEM_TEST_ONLY",
                      "CUSTOM_SYSTEM_TEST_IMAGE_TAG"]
}

SUPPORTED_ENV_VARS = [
    "PIPELINE_TYPE", "CUSTOM_XROOTD_VERSION",
    "CUSTOM_EOS_VERSION", "CUSTOM_CTA_VERSION", "PLATFORM"
]

def env_var_defined(name: str, ci_input_vars):
    return name in ci_input_vars and len(ci_input_vars[name]) > 0

def exit_if_defined(env_var_name, ci_input_vars):
    if env_var_defined(env_var_name, ci_input_vars):
        sys.exit(f"ERROR: using {env_var_name} is not allowed when running a {ci_input_vars['PIPELINE_TYPE']} pipeline.")

def exit_if_not_defined(env_var_name, ci_input_vars):
    if not env_var_defined(env_var_name, ci_input_vars):
        sys.exit(f"ERROR: {env_var_name} must be provided when running a {ci_input_vars['PIPELINE_TYPE']} pipeline.")

def run_cmd(cmd: str):
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


def check_rpm_available(package_name: str, package_version: str):
    """
    Checks that the package of a given version is available.
    Note that this assumes the current machine has all the required .repo files correctly configured.
    """
    version_regex = r"\d[\w.+~]*-\d+"
    if not re.fullmatch(version_regex, package_version):
        sys.exit(f"ERROR: package version {package_version} does not satisfy regex {version_regex}. Please double-check that the version (including the epoch) is correct.")

    tag_available = run_cmd(f"dnf list --showduplicates {package_name} | grep \"{package_version}\" | wc -l")

    if int(tag_available) < 1:
        sys.exit(f"ERROR: Could not find {package_name} version {package_version}")
    if int(tag_available) > 1:
        sys.exit(f"ERROR: Multiple packages found for {package_name} version {package_version}")

def check_image_tag_available(image_tag: str, repository: str):
    """
    Checks that the image tag is available in the given repository.
    """
    full_image = f"{repository}:{image_tag}"
    try:
        run_cmd(f"podman manifest inspect {full_image}")
    except SystemExit:
        sys.exit(f"ERROR: Image tag '{image_tag}' not found in repository '{repository}'")

def validate_default(ci_input_vars):
    """
    Validation for the DEFAULT pipeline type.
    """
    exit_if_defined("CUSTOM_CTA_VERSION", ci_input_vars)
    exit_if_defined("CUSTOM_EOS_VERSION", ci_input_vars)
    exit_if_defined("CUSTOM_XROOTD_VERSION", ci_input_vars)
    exit_if_defined("CUSTOM_SYSTEM_TEST_IMAGE_TAG", ci_input_vars)


def validate_regr_against_cta_main(ci_input_vars):
    """
    Validation for the pipeline type `REGR_AGAINST_CTA_MAIN`.
    """
    exit_if_defined("CUSTOM_CTA_VERSION", ci_input_vars)
    exit_if_defined("CUSTOM_SYSTEM_TEST_IMAGE_TAG", ci_input_vars)
    if not env_var_defined("CUSTOM_EOS_VERSION", ci_input_vars) and not env_var_defined("CUSTOM_XROOTD_VERSION", ci_input_vars):
        sys.exit(f"ERROR: at least one of [CUSTOM_EOS_VERSION, CUSTOM_XROOTD_VERSION] must be provided when running a regression test.")


def validate_regr_against_cta_version(ci_input_vars):
    """
    Validation for the pipeline type `EOS_REGR_AGAINST_CTA_VERSION`.
    """
    exit_if_not_defined("CUSTOM_CTA_VERSION", ci_input_vars)
    exit_if_defined("CUSTOM_SYSTEM_TEST_IMAGE_TAG", ci_input_vars)
    if not env_var_defined("CUSTOM_EOS_VERSION", ci_input_vars) and not env_var_defined("CUSTOM_XROOTD_VERSION", ci_input_vars):
        sys.exit(f"ERROR: at least one of [CUSTOM_EOS_VERSION, CUSTOM_XROOTD_VERSION] must be provided when running a regression test.")


def validate_image_from_cta_version(ci_input_vars):
    """
    Validation for the pipeline type `IMAGE_FROM_CTA_VERSION`.
    """
    exit_if_not_defined("CUSTOM_CTA_VERSION", ci_input_vars)
    exit_if_defined("CUSTOM_EOS_VERSION", ci_input_vars)
    exit_if_defined("CUSTOM_XROOTD_VERSION", ci_input_vars)
    exit_if_defined("CUSTOM_SYSTEM_TEST_IMAGE_TAG", ci_input_vars)

def validate_system_test_only(ci_input_vars):
    """
    Validation for the pipeline type `SYSTEM_TEST_ONLY`.
    """
    exit_if_defined("CUSTOM_CTA_VERSION", ci_input_vars)
    exit_if_defined("CUSTOM_EOS_VERSION", ci_input_vars)
    exit_if_defined("CUSTOM_XROOTD_VERSION", ci_input_vars)


def main():
    """
    Validate the varaibles received by the GitLab CI pipeline
    """
    project_json_path = Path(__file__).resolve().parents[2] / "project.json"
    with open(project_json_path, "r") as f:
        project_json = json.load(f)

    # Get project defined CI variables from environment
    ci_input_vars = {}
    for var in SUPPORTED_ENV_VARS:
        if var not in os.environ:
            sys.exit(f"ERROR: input variable {var} was not found as an environment variable.")
        ci_input_vars[var] = os.environ[var]

    print(ci_input_vars)

    if ci_input_vars["PIPELINE_TYPE"] not in SUPPORTED["PIPELINE_TYPE"]:
        sys.exit(f"ERROR: Pipeline type {ci_input_vars['PIPELINE_TYPE']} not supported")

    # Invoke for each pipeline type its corresponding function to do some basic constraint checks
    print(f"Validating input for {ci_input_vars['PIPELINE_TYPE']} pipeline type")
    validate_func_name = "validate_" + ci_input_vars['PIPELINE_TYPE'].lower()
    globals()[validate_func_name](ci_input_vars)

    # Ensure availability of any custom provided versions
    if env_var_defined("CUSTOM_CTA_VERSION", ci_input_vars):
        check_rpm_available("cta-frontend", ci_input_vars["CUSTOM_CTA_VERSION"])
    if env_var_defined("CUSTOM_XROOTD_VERSION", ci_input_vars):
        check_rpm_available("xrootd-server", ci_input_vars["CUSTOM_XROOTD_VERSION"])
    if env_var_defined("CUSTOM_EOS_VERSION", ci_input_vars):
        check_rpm_available("eos-server", ci_input_vars["CUSTOM_EOS_VERSION"])
        eos_image_tag=f"{ci_input_vars['CUSTOM_EOS_VERSION']}.{ci_input_vars['PLATFORM']}"
        check_image_tag_available(eos_image_tag, project_json["dev"]["defaultEosImageRepository"])
    if env_var_defined("CUSTOM_SYSTEM_TEST_IMAGE_TAG", ci_input_vars):
        check_image_tag_available(eos_image_tag, project_json["dev"]["defaultEosImageRepository"])

    print("Validation was successful")


if __name__ == '__main__':
    main()

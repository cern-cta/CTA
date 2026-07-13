#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import os
import sys
from pathlib import Path

# Dictionary containing the supported config of the different variables.
SUPPORTED = {
    "PIPELINE_TYPE": [
        "DEFAULT",
        "REGR_AGAINST_CTA_BRANCH",
        "REGR_AGAINST_CTA_VERSION",
    ]
}

SUPPORTED_ENV_VARS = [
    "PIPELINE_TYPE",
    "CUSTOM_XROOTD_VERSION",
    "CUSTOM_CTA_IMAGE_TAG",
    "CUSTOM_EOS_IMAGE_TAG",
    "PLATFORM",
]


def env_var_defined(name: str, ci_input_vars):
    return name in ci_input_vars and len(ci_input_vars[name]) > 0


def exit_if_defined(env_var_name, ci_input_vars):
    if env_var_defined(env_var_name, ci_input_vars):
        sys.exit(
            f"ERROR: using {env_var_name} is not allowed when running a {ci_input_vars['PIPELINE_TYPE']} pipeline."
        )


def exit_if_not_defined(env_var_name, ci_input_vars):
    if not env_var_defined(env_var_name, ci_input_vars):
        sys.exit(f"ERROR: {env_var_name} must be provided when running a {ci_input_vars['PIPELINE_TYPE']} pipeline.")


def validate_default(ci_input_vars):
    """
    Validation for the DEFAULT pipeline type.
    """
    exit_if_defined("CUSTOM_CTA_IMAGE_TAG", ci_input_vars)
    exit_if_defined("CUSTOM_EOS_IMAGE_TAG", ci_input_vars)
    exit_if_defined("CUSTOM_XROOTD_VERSION", ci_input_vars)


def validate_regr_against_cta_branch(ci_input_vars):
    """
    Validation for the pipeline type `REGR_AGAINST_CTA_BRANCH`.
    """
    exit_if_defined("CUSTOM_CTA_IMAGE_TAG", ci_input_vars)


def validate_regr_against_cta_version(ci_input_vars):
    """
    Validation for the pipeline type `EOS_REGR_AGAINST_CTA_VERSION`.
    """
    del ci_input_vars  # We don't need to check anything here


def main():
    """
    Validate the variables received by the GitLab CI pipeline
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
    validate_func_name = "validate_" + ci_input_vars["PIPELINE_TYPE"].lower()
    globals()[validate_func_name](ci_input_vars)

    if env_var_defined("CUSTOM_XROOTD_VERSION", ci_input_vars):
        xrootd_version = ci_input_vars["CUSTOM_XROOTD_VERSION"]
        project_xrootd_version = project_json["platforms"][ci_input_vars["PLATFORM"]]["versionlock"]["group-xrootd"]
        # Check that at this point the project.json contains the same version
        if xrootd_version != project_xrootd_version:
            sys.exit(
                f"ERROR: CUSTOM_XROOTD_VERSION must be equal to value in project.json ({xrootd_version} != {project_xrootd_version}). Please verify the logic in the modify-project-json job."
            )

    project_eos_image_tag = project_json["dev"]["eosImageTag"]
    if env_var_defined("CUSTOM_EOS_IMAGE_TAG", ci_input_vars):
        eos_image_tag = ci_input_vars["CUSTOM_EOS_IMAGE_TAG"]
        # Check that at this point the project.json contains the same version
        if eos_image_tag != project_eos_image_tag:
            sys.exit(
                f"ERROR: CUSTOM_EOS_IMAGE_TAG must be equal to value in project.json ({eos_image_tag} != {project_eos_image_tag}). Please verify the logic in the modify-project-json job."
            )
    print("Validation was successful")


if __name__ == "__main__":
    main()

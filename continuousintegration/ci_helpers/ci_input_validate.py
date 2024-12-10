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

"""

"""

import re
import subprocess
import sys
import os
import json

# Dictionary containing the supported config of the different variables.
SUPPORTED = {
    "PIPELINE_TYPE": ["DEFAULT",
                      "EOSREG_CTAMAIN",
                      "EOSREG_CTATAG"],
    "XRD_TAG_REGEX": re.compile("^\d+:\d+\.\d+\.\d+-\d+$"),
    "EOS_TAG_REGEX": re.compile("^\d+\.\d+\.\d+$"),
    "CTA_TAG_REGEX": re.compile("^v\d+\.\d+\.\d+\.\d+-\d+$")
    # "SCHED_TYPE": ["objecstore",
    #                "pgsched"]
    #  ...
}

def run_cmd(cmd):
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

def help():
    """
    """
    pass

def _check_remote_rpm(ci_input_vars, ci_var_tag_name, version_regex, remote_rpm_repo, rpm_name, rpm_ver):
    """"

    """
    # Check tag is supported
    if ci_var_tag_name not in ci_input_vars.keys():
        sys.exit(f"ERROR: tag version variable ${ci_var_tag_name} is not set")

    # Check specified RPM version is valid with what we expect.
    if not SUPPORTED[version_regex].match(ci_input_vars[ci_var_tag_name]):
        sys.exit(f"ERROR: specified XRootD tag {ci_input_vars[ci_var_tag_name]} "
              f"does not match regex {SUPPORTED['XRD_TAG_REGEX'].pattern}")


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
    """
    pass


def validate_eosreg_ctamain(ci_input_vars):
    """

    """
    print("Validating XRootD...")
    _check_remote_rpm(ci_input_vars,
                      "P_TYPE_EOSREG_XRD_TAG",
                      "XRD_TAG_REGEX",
                      ["xrd", "https://xrootd.web.cern.ch/repo/stable/el9/x86_64"],
                      "xrootd-server",
                      ci_input_vars["P_TYPE_EOSREG_XRD_TAG"])
    print("XRootD validation passed")


    print("Validating EOS...")
    _check_remote_rpm(ci_input_vars,
                      "P_TYPE_EOSREG_EOS_TAG",
                      "EOS_TAG_REGEX",
                      ["eos", "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/el-9/x86_64/"],
                      "eos-server",
                      ci_input_vars["P_TYPE_EOSREG_EOS_TAG"])
    print("EOS validation passed")



def validate_eosreg_ctatag(ci_input_vars):
    """

    """
    print("Validating XRootD...")
    _check_remote_rpm(ci_input_vars,
                      "P_TYPE_EOSREG_XRD_TAG",
                      "XRD_TAG_REGEX",
                      ["xrd", "https://xrootd.web.cern.ch/repo/stable/el9/x86_64"],
                      "xrootd-server",
                      ci_input_vars["P_TYPE_EOSREG_XRD_TAG"])
    print("XRootD validation passed")


    print("Validating EOS...")
    _check_remote_rpm(ci_input_vars,
                      "P_TYPE_EOSREG_EOS_TAG",
                      "EOS_TAG_REGEX",
                      ["eos", "https://storage-ci.web.cern.ch/storage-ci/eos/diopside/tag/testing/el-9/x86_64/"],
                      "eos-server",
                      ci_input_vars["P_TYPE_EOSREG_EOS_TAG"])
    print("EOS validation passed")


    print("Validating CTA...")
    _check_remote_rpm(ci_input_vars,
                      "P_TYPE_EOSREG_CTA_TAG",
                      "CTA_TAG_REGEX",
                      ["cta", "https://cta-public-repo.web.cern.ch/testing/cta-5/el9/cta/x86_64/"],
                      "cta-frontend",
                      ci_input_vars["P_TYPE_EOSREG_CTA_TAG"][1:])
    print("CTA validation passed")


    # Check that the XRootD version specified in the variables matches the
    # XRootD version that the RPMs where compiled against.
    # In the spec.in file we state that the version must match which causes a conflict
    # when installing the RPMs, we could do something similar to what EOS project does
    # `Requires: xrootd >= %{some_ver}` instead of `Requires: xrootd = %{some_ver}`
    # This way we could avoid this conflict. For now fail the pipeline if the versions
    # do not match.
    run_cmd(f"git fetch")
    cta_xrd_ver =run_cmd(f"git checkout tags/{ci_input_vars['P_TYPE_EOSREG_CTA_TAG']} -- cta.spec.in && "
                         "grep 'xrootdVersion 1:5' cta.spec.in | awk '{print $3}' | "
                         "awk -F'-' '{print $1}'")

    if cta_xrd_ver != ci_input_vars["P_TYPE_EOSREG_XRD_TAG"]:
        sys.exit(f"ERROR: CTA was compiled for {cta_xrd_ver} while EOS requires {ci_input_vars['P_TYPE_EOSREG_XRD_TAG']}")


def main():
    """
    Execute the input validation logic

    $1: env variable containing the input dictionary
    """
    # Check specified pipeline type is supported
    if sys.argv[1] not in os.environ.keys():
        sys.exit("ERROR: json input variable not found in environment")

    ci_input_vars = json.loads(os.environ[sys.argv[1]])

    if ci_input_vars["PIPELINE_TYPE"] not in SUPPORTED["PIPELINE_TYPE"]:
        sys.exit(f"ERROR: Pipeline type {ci_input_vars['PIEPLINE_TYPE']} not supported")

    # Execute the corresponding validation function passing as input
    # all other variables to check.
    print(f"Validating input for {ci_input_vars['PIPELINE_TYPE']} pipeline type")
    validate_func_name = "validate_" + ci_input_vars['PIPELINE_TYPE'].lower()
    globals()[validate_func_name](ci_input_vars)

    print("Validation was successful")


if __name__ == '__main__':
    main()

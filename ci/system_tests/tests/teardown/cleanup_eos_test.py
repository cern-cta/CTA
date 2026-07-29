# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


#####################################################################################################################
# EOS Teardown
#####################################################################################################################


from system_tests.helpers.test_env import TestEnv


def test_cleanup_eos_directories(env: TestEnv) -> None:
    env.eos_mgm[0].force_remove_directory(env.eos_mgm[0].base_dir_path / "cta")
    env.eos_mgm[0].force_remove_directory(env.eos_mgm[0].base_dir_path / "repack")

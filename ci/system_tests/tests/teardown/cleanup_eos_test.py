# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


# =========================================================================
#  EOS Teardown
# =========================================================================


from system_tests.helpers.test_env import TestEnv


def test_cleanup_eos_directories(env: TestEnv) -> None:
    for eos_mgm in env.eos_mgm:
        eos_mgm.force_remove_directory(eos_mgm.base_dir_path / "cta")
        eos_mgm.force_remove_directory(eos_mgm.base_dir_path / "repack")

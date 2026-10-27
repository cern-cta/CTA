/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "AdminCmdMode.hpp"

#include <gtest/gtest.h>

namespace unitTests {

namespace {

cta::admin::AdminCmd makeCmd(cta::admin::AdminCmd::Cmd cmd, cta::admin::AdminCmd::SubCmd subcmd) {
  cta::admin::AdminCmd adminCmd;
  adminCmd.set_cmd(cmd);
  adminCmd.set_subcmd(subcmd);
  return adminCmd;
}

}  // namespace

TEST(AdminCmdMode, NoRepackRejectsAllTheRepackCommands) {
  using cta::admin::AdminCmd;
  using cta::admin_api::AdminCmdMode;
  using cta::admin_api::isCommandDisabled;

  for (const auto subcmd : {AdminCmd::SUBCMD_ADD, AdminCmd::SUBCMD_RM, AdminCmd::SUBCMD_ERR, AdminCmd::SUBCMD_LS}) {
    EXPECT_TRUE(isCommandDisabled(AdminCmdMode::NO_REPACK, makeCmd(AdminCmd::CMD_REPACK, subcmd))) << subcmd;
  }
}

TEST(AdminCmdMode, NoRepackAcceptsTheOtherCommands) {
  using cta::admin::AdminCmd;
  using cta::admin_api::AdminCmdMode;
  using cta::admin_api::isCommandDisabled;

  EXPECT_FALSE(isCommandDisabled(AdminCmdMode::NO_REPACK, makeCmd(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_LS)));
  EXPECT_FALSE(isCommandDisabled(AdminCmdMode::NO_REPACK, makeCmd(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_CH)));
  EXPECT_FALSE(isCommandDisabled(AdminCmdMode::NO_REPACK, makeCmd(AdminCmd::CMD_VERSION, AdminCmd::SUBCMD_NONE)));
}

TEST(AdminCmdMode, OtherModesRejectNothing) {
  using cta::admin::AdminCmd;
  using cta::admin_api::AdminCmdMode;
  using cta::admin_api::isCommandDisabled;

  for (const auto mode : {AdminCmdMode::ALL, AdminCmdMode::REPACK}) {
    EXPECT_FALSE(isCommandDisabled(mode, makeCmd(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_LS)));
    EXPECT_FALSE(isCommandDisabled(mode, makeCmd(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_ADD)));
    EXPECT_FALSE(isCommandDisabled(mode, makeCmd(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_LS)));
  }
}

TEST(AdminCmdMode, DisabledAdminCmdIsAUserErrorNamingTheCommand) {
  using cta::admin::AdminCmd;
  using cta::admin_api::DisabledAdminCmd;

  try {
    throw DisabledAdminCmd(makeCmd(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_ADD));
  } catch (const cta::exception::UserError& ex) {
    EXPECT_EQ(ex.getMessageValue(), "Admin command pair <CMD_REPACK, SUBCMD_ADD> is disabled on this frontend.");
  }
}

}  // namespace unitTests

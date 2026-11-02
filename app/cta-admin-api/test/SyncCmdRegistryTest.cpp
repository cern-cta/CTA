/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/sync/SyncCmdRegistry.hpp"

#include "commands/sync/handlers/Handlers.hpp"

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

TEST(SyncCmdRegistry, FindsTheHandlerOfACommand) {
  using cta::admin::AdminCmd;
  namespace handlers = cta::admin_api::commands::sync::handlers;
  using cta::admin_api::commands::sync::findHandler;

  EXPECT_EQ(findHandler(makeCmd(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_ADD)), &handlers::addTape);
  EXPECT_EQ(findHandler(makeCmd(AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_UP)), &handlers::setDriveUp);
  EXPECT_EQ(findHandler(makeCmd(AdminCmd::CMD_ARCHIVEFILE, AdminCmd::SUBCMD_CH)), &handlers::changeArchiveFile);
  EXPECT_EQ(findHandler(makeCmd(AdminCmd::CMD_RECYCLETAPEFILE, AdminCmd::SUBCMD_RESTORE)),
            &handlers::restoreRecycleTapeFile);
}

TEST(SyncCmdRegistry, StreamedAndUnknownCommandsHaveNoHandler) {
  using cta::admin::AdminCmd;
  using cta::admin_api::commands::sync::findHandler;

  // Served by the streams instead
  EXPECT_EQ(findHandler(makeCmd(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_LS)), nullptr);
  EXPECT_EQ(findHandler(makeCmd(AdminCmd::CMD_SHOWQUEUES, AdminCmd::SUBCMD_NONE)), nullptr);
  // Not a valid pair
  EXPECT_EQ(findHandler(makeCmd(AdminCmd::CMD_VERSION, AdminCmd::SUBCMD_RM)), nullptr);
}

}  // namespace unitTests

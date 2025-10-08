/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <gtest/gtest.h>

#include "ProcessManager.hpp"
#include "TestSubprocessHandlers.hpp"
#include "common/log/StringLogger.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

TEST(cta_Daemon, ProcessManager) {
  cta::log::StringLogger dlog("dummy","unitTest", cta::log::DEBUG);
  cta::log::LogContext lc(dlog);
  cta::tape::daemon::ProcessManager pm(lc);
  {
    std::unique_ptr<EchoSubprocess> es(new EchoSubprocess("Echo subprocess", pm));
    pm.addHandler(std::move(es));
    pm.run();
  }
  EchoSubprocess & es = dynamic_cast<EchoSubprocess&>(pm.at("Echo subprocess"));
  ASSERT_TRUE(es.echoReceived());
}

// Bigger layouts are tested in specific handler's unit tests (like SignalHandler)

} //namespace unitTests

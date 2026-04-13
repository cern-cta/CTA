/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ProcessManager.hpp"
#include "SignalHandler.hpp"
#include "TestSubprocessHandlers.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StringLogger.hpp"

#include <gtest/gtest.h>

namespace unitTests {
using cta::tape::daemon::SignalHandler;
using cta::tape::daemon::SubprocessHandler;

TEST(cta_Daemon, SignalHandlerShutdown) {
  cta::log::StringLogger dlog("dummy", "unitTest", cta::log::DEBUG);
  cta::log::LogContext lc(dlog);
  cta::tape::daemon::ProcessManager pm(lc);
  {
    // Add the signal handler to the manager
    std::unique_ptr<SignalHandler> sh(new SignalHandler(pm));
    pm.addHandler(std::move(sh));
    // Add the probe handler to the manager
    std::unique_ptr<ProbeSubprocess> ps(new ProbeSubprocess());
    pm.addHandler(std::move(ps));
    // This signal will be queued for the signal handler.
    ::kill(::getpid(), SIGTERM);
  }
  pm.run();
  ProbeSubprocess& ps = dynamic_cast<ProbeSubprocess&>(pm.at("ProbeProcessHandler"));
  ASSERT_TRUE(ps.sawShutdown());
  ASSERT_FALSE(ps.sawKill());
}

TEST(cta_Daemon, SignalHandlerKill) {
  cta::log::StringLogger dlog("dummy", "unitTest", cta::log::DEBUG);
  cta::log::LogContext lc(dlog);
  cta::tape::daemon::ProcessManager pm(lc);
  {
    // Add the signal handler to the manager
    std::unique_ptr<SignalHandler> sh(new SignalHandler(pm));
    // Set the timeout
    sh->setTimeout(std::chrono::milliseconds(10));
    pm.addHandler(std::move(sh));
    // Add the probe handler to the manager
    std::unique_ptr<ProbeSubprocess> ps(new ProbeSubprocess());
    ps->setHonorShutdown(false);
    pm.addHandler(std::move(ps));
    // This signal will be queued for the signal handler.
    ::kill(::getpid(), SIGTERM);
  }
  pm.run();
  ProbeSubprocess& ps = dynamic_cast<ProbeSubprocess&>(pm.at("ProbeProcessHandler"));
  ASSERT_TRUE(ps.sawShutdown());
  ASSERT_TRUE(ps.sawKill());
}

TEST(cta_Daemon, SignalHandlerSigChild) {
  cta::log::StringLogger dlog("dummy", "unitTest", cta::log::DEBUG);
  cta::log::LogContext lc(dlog);
  cta::tape::daemon::ProcessManager pm(lc);
  {
    // Add the signal handler to the manager
    std::unique_ptr<SignalHandler> sh(new SignalHandler(pm));
    // Set the timeout
    sh->setTimeout(std::chrono::milliseconds(10));
    pm.addHandler(std::move(sh));
    // Add the probe handler to the manager
    std::unique_ptr<ProbeSubprocess> ps(new ProbeSubprocess());
    ps->shutdown();
    pm.addHandler(std::move(ps));
    // This signal will be queued for the signal handler.
    // Add the EchoSubprocess whose child will exit.
    std::unique_ptr<EchoSubprocess> es(new EchoSubprocess("Echo", pm));
    es->setCrashingShild(true);
    pm.addHandler(std::move(es));
  }
  pm.run();
  ProbeSubprocess& ps = dynamic_cast<ProbeSubprocess&>(pm.at("ProbeProcessHandler"));
  ASSERT_TRUE(ps.sawShutdown());
  ASSERT_FALSE(ps.sawKill());
  ASSERT_TRUE(ps.sawSigChild());
  EchoSubprocess& es = dynamic_cast<EchoSubprocess&>(pm.at("Echo"));
  ASSERT_FALSE(es.echoReceived());
}
}  // namespace unitTests

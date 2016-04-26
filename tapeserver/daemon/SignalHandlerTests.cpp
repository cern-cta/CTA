/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>
#include "ProcessManager.hpp"
#include "SignalHandler.hpp"
#include "TestSubprocessHandlers.hpp"

namespace unitTests {
using cta::tape::daemon::SignalHandler;
using cta::tape::daemon::SubprocessHandler;

TEST(cta_Daemon, SignalHandlerShutdown) {
  cta::tape::daemon::ProcessManager pm;
  {
    // Add the signal handler to the manager
    std::unique_ptr<SignalHandler> sh(new SignalHandler(pm));
    // downcast pointer
    std::unique_ptr<SubprocessHandler> shSph = std::move(sh);
    pm.addHandler(std::move(shSph));
    // Add the probe handler to the manager
    std::unique_ptr<ProbeSubprocess> ps(new ProbeSubprocess());
    // downcast pointer
    std::unique_ptr<SubprocessHandler> shPs = std::move(ps);
    pm.addHandler(std::move(shPs));
    // This signal will be queued for the signal handler.
    ::kill(::getpid(), SIGTERM);
  }
  pm.run();
  ProbeSubprocess &ps=dynamic_cast<ProbeSubprocess&>(pm.at("ProbeProcessHandler"));
  ASSERT_TRUE(ps.sawShutdown());
  ASSERT_FALSE(ps.sawKill());
}

TEST(cta_Daemon, SignalHandlerKill) {
  cta::tape::daemon::ProcessManager pm;
  {
    // Add the signal handler to the manager
    std::unique_ptr<SignalHandler> sh(new SignalHandler(pm));
    // Set the timeout
    sh->setTimeout(std::chrono::milliseconds(10));
    // downcast pointer
    std::unique_ptr<SubprocessHandler> shSph = std::move(sh);
    pm.addHandler(std::move(shSph));
    // Add the probe handler to the manager
    std::unique_ptr<ProbeSubprocess> ps(new ProbeSubprocess());
    ps->setHonorShutdown(false);
    // downcast pointer
    std::unique_ptr<SubprocessHandler> shPs = std::move(ps);
    pm.addHandler(std::move(shPs));
    // This signal will be queued for the signal handler.
    ::kill(::getpid(), SIGTERM);
  }
  pm.run();
  ProbeSubprocess &ps=dynamic_cast<ProbeSubprocess&>(pm.at("ProbeProcessHandler"));
  ASSERT_TRUE(ps.sawShutdown());
  ASSERT_TRUE(ps.sawKill());
}

TEST(cta_Daemon, SignalHandlerSigChild) {
  cta::tape::daemon::ProcessManager pm;
  {
    // Add the signal handler to the manager
    std::unique_ptr<SignalHandler> sh(new SignalHandler(pm));
    // Set the timeout
    sh->setTimeout(std::chrono::milliseconds(10));
    // downcast pointer
    std::unique_ptr<SubprocessHandler> shSph = std::move(sh);
    pm.addHandler(std::move(shSph));
    // Add the probe handler to the manager
    std::unique_ptr<ProbeSubprocess> ps(new ProbeSubprocess());
    ps->shutdown();
    // downcast pointer
    std::unique_ptr<SubprocessHandler> shPs = std::move(ps);
    pm.addHandler(std::move(shPs));
    // This signal will be queued for the signal handler.
    // Add the EchoSubprocess whose child will exit.
    std::unique_ptr<EchoSubprocess> es(new EchoSubprocess("Echo", pm));
    es->setCrashingShild(true);
    // downcast pointer
    std::unique_ptr<SubprocessHandler> shEs = std::move(es);
    pm.addHandler(std::move(shEs));
  }
  pm.run();
  ProbeSubprocess &ps = dynamic_cast<ProbeSubprocess&>(pm.at("ProbeProcessHandler"));
  ASSERT_TRUE(ps.sawShutdown());
  ASSERT_FALSE(ps.sawKill());
  ASSERT_TRUE(ps.sawSigChild());
  EchoSubprocess &es = dynamic_cast<EchoSubprocess&>(pm.at("Echo"));
  ASSERT_FALSE(es.echoReceived());
}
}

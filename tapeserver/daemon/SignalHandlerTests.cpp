/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <gtest/gtest.h>
#include "DriveHandler.hpp"
#include "DummyDriveHandler.hpp"
#include "ProcessManager.hpp"
#include "SignalHandler.hpp"
#include "TestSubprocessHandlers.hpp"
#include "common/log/StringLogger.hpp"
#include "common/log/LogContext.hpp"
#include "tests/TempFile.hpp"
#include "tapeserver/daemon/TapedConfiguration.hpp"

namespace unitTests {
using cta::tape::daemon::SignalHandler;
using cta::tape::daemon::SubprocessHandler;
using cta::tape::daemon::DriveHandler;
using cta::tape::daemon::tests::DummyDriveHandler;

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
  ProbeSubprocess &ps=dynamic_cast<ProbeSubprocess&>(pm.at("ProbeProcessHandler"));
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
  ProbeSubprocess &ps=dynamic_cast<ProbeSubprocess&>(pm.at("ProbeProcessHandler"));
  ASSERT_TRUE(ps.sawShutdown());
  ASSERT_TRUE(ps.sawKill());
}

TEST(cta_Daemon, SignalHandlerKillDualDrive) {
  cta::log::StringLogger dlog("dummy", "unitTest", cta::log::DEBUG);
  cta::log::LogContext lc(dlog);
  cta::tape::daemon::ProcessManager pm(lc);
  {
    // Add the signal handler to the manager
    std::unique_ptr<SignalHandler> sh(new SignalHandler(pm));
    // Set the timeout
    sh->setTimeout(std::chrono::milliseconds(10));
    pm.addHandler(std::move(sh));
    // Create taped config file.
    TempFile tapedConfigFile;
    tapedConfigFile.stringFill(
      "ObjectStore BackendPath vfsObjectStore:///tmp/dir\n"
      "taped CatalogueConfigFile /etc/cta/catalog.conf\n"
      "taped ArchiveFetchBytesFiles 1, 2\n"
      "taped ArchiveFlushBytesFiles 3, 4\n"
      "taped RetrieveFetchBytesFiles 5, 6\n"
      "taped BufferCount 1\n"
      "taped TpConfigPath ");

    // Create drive config file.
    TempFile driveConfigFile;
    driveConfigFile.stringFill("drive0 lib0 /dev/tape0 smc0\n"
      "drive1 lib0 /dev/tape1 smc1\n"
      "drive2 lib0 /dev/tape2 smc2");

    tapedConfigFile.stringAppend(driveConfigFile.path());

    auto tapedConfig = cta::tape::daemon::TapedConfiguration::createFromCtaConf(tapedConfigFile.path());

    // Add two drive handlers to the manager.
    for(auto & drive : tapedConfig.driveConfigs) {
      std::unique_ptr<DummyDriveHandler> dh(new DummyDriveHandler(tapedConfig,
                                                        drive.second.value(), pm));
      pm.addHandler(std::move(dh));
    }

    // This signal will be queued for the signal handler.
    ::kill(::getpid(), SIGTERM);
  }
  pm.run();
  DummyDriveHandler& dhToShutdown = dynamic_cast<DummyDriveHandler&>(pm.at("DriveNameHere"));
  dhToShutdown.requestShutdown();

  //Some asserts here.
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
  ProbeSubprocess &ps = dynamic_cast<ProbeSubprocess&>(pm.at("ProbeProcessHandler"));
  ASSERT_TRUE(ps.sawShutdown());
  ASSERT_FALSE(ps.sawKill());
  ASSERT_TRUE(ps.sawSigChild());
  EchoSubprocess &es = dynamic_cast<EchoSubprocess&>(pm.at("Echo"));
  ASSERT_FALSE(es.echoReceived());
}
}

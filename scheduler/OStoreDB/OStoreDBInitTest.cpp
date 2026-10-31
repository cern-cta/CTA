/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/OStoreDB/OStoreDBInit.hpp"

#include "common/log/DummyLogger.hpp"
#include "objectstore/RootEntry.hpp"

#include <gtest/gtest.h>

namespace unitTests {

TEST(OStoreDBInit, ReleasesHeartbeatOnDestruction) {
  cta::log::DummyLogger logger("dummy", "unitTest");
  cta::objectstore::BackendVFS backend;
  cta::objectstore::RootEntry root(backend);
  root.initialize();
  root.insert();

  std::string agentAddress;
  {
    cta::OStoreDBInit init("unitTest", backend.getParams()->toURL(), logger);
    agentAddress = init.getAgentReference().getAgentAddress();
    EXPECT_TRUE(backend.exists(agentAddress));
  }

  // LSan also checks that the stopped heartbeat and its promise were released.
  EXPECT_FALSE(backend.exists(agentAddress));
}

}  // namespace unitTests

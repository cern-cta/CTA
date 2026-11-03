/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MigrationMemoryManager.hpp"

#include "common/log/StringLogger.hpp"
#include "taped/session/pipeline/DataPipeline.hpp"

#include <gtest/gtest.h>

namespace unitTests {

TEST(MigrationMemoryManager, UsageExcludesPrefetchedBlocks) {
  using namespace cta::tape::daemon;
  cta::log::StringLogger log("dummy", "MigrationMemoryManagerTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  MigrationMemoryManager manager(2, 1024, lc);
  DataPipeline pipeline(2);

  EXPECT_EQ(2048, manager.getTotalMemoryAllocated());
  EXPECT_EQ(0, manager.getTotalMemoryUsed());

  // Joining the provider guarantees both empty blocks have reached the pipeline.
  manager.addClient(&pipeline);
  manager.finish();
  manager.startThreads();
  manager.waitThreads();
  EXPECT_FALSE(manager.areBlocksAllBack());
  EXPECT_EQ(0, manager.getTotalMemoryUsed());

  auto* first = manager.acquireBlock(pipeline);
  EXPECT_EQ(1024, manager.getTotalMemoryUsed());
  // Resetting metadata must not release an acquired block's capacity.
  first->reset();
  EXPECT_EQ(1024, manager.getTotalMemoryUsed());
  pipeline.pushDataBlock(first);
  EXPECT_EQ(1024, manager.getTotalMemoryUsed());
  first = pipeline.popDataBlock();
  EXPECT_EQ(1024, manager.getTotalMemoryUsed());

  auto* second = manager.acquireBlock(pipeline);
  EXPECT_EQ(2048, manager.getTotalMemoryUsed());
  manager.releaseBlock(first);
  EXPECT_EQ(1024, manager.getTotalMemoryUsed());

  // Failure markers still occupy a block until the consumer releases it.
  second->markAsCancelled();
  pipeline.pushDataBlock(second);
  EXPECT_EQ(1024, manager.getTotalMemoryUsed());
  manager.releaseBlock(pipeline.popDataBlock());
  EXPECT_EQ(0, manager.getTotalMemoryUsed());
  EXPECT_TRUE(manager.areBlocksAllBack());
}

}  // namespace unitTests

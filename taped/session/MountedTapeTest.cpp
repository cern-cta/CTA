/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MountedTape.hpp"

#include "TapeSessionTracker.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/log/StringLogger.hpp"
#include "mediachanger/LibrarySlotParser.hpp"
#include "mediachanger/RmcProxy.hpp"
#include "taped/drive/FakeDrive.hpp"

#include <gtest/gtest.h>
#include <stdexcept>
#include <type_traits>

namespace cta::tape::daemon {
namespace {

static_assert(!std::is_copy_constructible_v<MountedTape>);
static_assert(!std::is_move_constructible_v<MountedTape>);
static_assert(std::is_nothrow_destructible_v<MountedTape>);

TEST(MountedTapeTest, UsesBorrowedCleanerAndDrive) {
  log::StringLogger logger("host", "MountedTapeTest", log::DEBUG);
  catalogue::DummyCatalogue catalogue;
  mediachanger::RmcProxy proxy;
  mediachanger::MediaChangerFacade changer(proxy, logger);
  TapeSessionTracker tracker;
  common::dataStructures::DriveInfo driveInfo;
  drive::FakeDrive drive(5000, drive::FakeDrive::OnFlush);
  drive.setTapeInPlace(false);
  DriveCleaner cleaner(changer, logger, driveInfo, "", false, 0, catalogue, tracker);
  for (const auto mode : {MountedTape::AccessMode::ReadOnly, MountedTape::AccessMode::ReadWrite}) {
    logger.clearLog();
    drive.enableCRC32CLogicalBlockProtectionReadWrite();
    log::StringLogger cleanupLogger("host", "MountedTapeTest", log::DEBUG);
    log::LogContext lc(cleanupLogger);
    MountedTape::Outcome outcome;
    {
      MountedTape tape(
        changer,
        "TAPE01",
        mediachanger::LibrarySlotParser::parse("dummy"),
        mode,
        cleaner,
        drive,
        [](auto) {},
        outcome,
        lc);
      EXPECT_NE(std::string::npos,
                logger.getLog().find(mode == MountedTape::AccessMode::ReadOnly ? "Dummy mount for read-only access" :
                                                                                 "Dummy mount for read/write access"));
      EXPECT_NE(std::string::npos, logger.getLog().find("TAPE01"));
    }
    EXPECT_TRUE(outcome.driveReusable());
    EXPECT_EQ(drive::lbpToUse::disabled, drive.getLbpToUse());
  }
}

TEST(MountedTapeTest, CleansOnScopeExit) {
  log::StringLogger cleanupLogger("host", "MountedTapeTest", log::DEBUG);
  log::LogContext lc(cleanupLogger);
  MountedTape::Outcome outcome;
  size_t calls = 0;
  {
    MountedTape tape([] {},
                     [&] {
                       ++calls;
                       return DriveCleaner::CleanupResult {};
                     },
                     outcome,
                     lc);
    EXPECT_FALSE(outcome.driveReusable());
    EXPECT_EQ(0, calls);
  }
  EXPECT_EQ(1, calls);
  EXPECT_TRUE(outcome.driveReusable());
}

TEST(MountedTapeTest, ExplicitCleanupIsNotRepeatedByDestructor) {
  log::StringLogger cleanupLogger("host", "MountedTapeTest", log::DEBUG);
  log::LogContext lc(cleanupLogger);
  MountedTape::Outcome outcome;
  size_t calls = 0;
  {
    MountedTape tape([] {},
                     [&] {
                       ++calls;
                       return DriveCleaner::CleanupResult {};
                     },
                     outcome,
                     lc);
    EXPECT_TRUE(tape.cleanup().driveReusable());
    EXPECT_TRUE(tape.cleanup().driveReusable());
  }
  EXPECT_EQ(1, calls);
}

TEST(MountedTapeTest, RecordsAndLogsCleanupFailureWithoutRetry) {
  log::StringLogger cleanupLogger("host", "MountedTapeTest", log::DEBUG);
  log::LogContext lc(cleanupLogger);
  MountedTape::Outcome outcome;
  size_t calls = 0;
  {
    MountedTape tape([] {},
                     [&] {
                       if (++calls == 1) {
                         return DriveCleaner::CleanupResult {.ejectFailed = true, .errorMessage = "stuck"};
                       }
                       return DriveCleaner::CleanupResult {};
                     },
                     outcome,
                     lc);
  }
  EXPECT_EQ(1, calls);
  EXPECT_FALSE(outcome.driveReusable());
  EXPECT_NE(std::string::npos, cleanupLogger.getLog().find("stuck"));
  EXPECT_EQ(nullptr, outcome.exception);
  ASSERT_TRUE(outcome.result);
  EXPECT_EQ("stuck", outcome.result->errorMessage);
}

TEST(MountedTapeTest, FailedCleanupIsNotRepeatedByDestructor) {
  size_t calls = 0;
  log::StringLogger cleanupLogger("host", "MountedTapeTest", log::DEBUG);
  log::LogContext lc(cleanupLogger);
  MountedTape::Outcome outcome;
  {
    MountedTape tape([] {},
                     [&] {
                       ++calls;
                       return DriveCleaner::CleanupResult {.configurationResetFailed = true,
                                                           .errorMessage = "reset failed"};
                     },
                     outcome,
                     lc);
    EXPECT_FALSE(tape.cleanup().driveReusable());
    tape.cleanup();
  }
  EXPECT_EQ(1, calls);
  ASSERT_TRUE(outcome.result);
  EXPECT_EQ("reset failed", outcome.result->errorMessage);
  EXPECT_FALSE(outcome.driveReusable());
}

TEST(MountedTapeTest, RecordsAndLogsStandardExceptionWithoutRetry) {
  log::StringLogger cleanupLogger("host", "MountedTapeTest", log::DEBUG);
  log::LogContext lc(cleanupLogger);
  MountedTape::Outcome outcome;
  size_t calls = 0;
  {
    MountedTape tape([] {},
                     [&] {
                       if (++calls == 1) {
                         throw std::runtime_error("cleanup failed");
                       }
                       return DriveCleaner::CleanupResult {};
                     },
                     outcome,
                     lc);
  }
  EXPECT_FALSE(outcome.driveReusable());
  EXPECT_EQ(1, calls);
  ASSERT_NE(nullptr, outcome.exception);
  EXPECT_THROW(std::rethrow_exception(outcome.exception), std::runtime_error);
  EXPECT_NE(std::string::npos, cleanupLogger.getLog().find("cleanup failed"));
}

TEST(MountedTapeTest, ContainsNonstandardExceptionsDuringUnwinding) {
  log::StringLogger cleanupLogger("host", "MountedTapeTest", log::DEBUG);
  log::LogContext lc(cleanupLogger);
  MountedTape::Outcome outcome;
  try {
    MountedTape tape([] {}, []() -> DriveCleaner::CleanupResult { throw 42; }, outcome, lc);
    throw std::runtime_error("session failed");
  } catch (const std::runtime_error& ex) {
    EXPECT_STREQ("session failed", ex.what());
  }
  EXPECT_FALSE(outcome.driveReusable());
  ASSERT_NE(nullptr, outcome.exception);
  EXPECT_THROW(std::rethrow_exception(outcome.exception), int);
  EXPECT_NE(std::string::npos, cleanupLogger.getLog().find("Non-standard exception"));
}

TEST(MountedTapeTest, MountsBeforeReturningFromConstructor) {
  log::StringLogger cleanupLogger("host", "MountedTapeTest", log::DEBUG);
  log::LogContext lc(cleanupLogger);
  MountedTape::Outcome outcome;
  bool mounted = false;
  {
    MountedTape tape([&] { mounted = true; },
                     [&] {
                       EXPECT_TRUE(mounted);
                       mounted = false;
                       return DriveCleaner::CleanupResult {};
                     },
                     outcome,
                     lc);
    EXPECT_TRUE(mounted);
  }
  EXPECT_FALSE(mounted);
}

TEST(MountedTapeTest, CleansPartialMountAndRethrowsOriginalFailure) {
  log::StringLogger cleanupLogger("host", "MountedTapeTest", log::DEBUG);
  log::LogContext lc(cleanupLogger);
  MountedTape::Outcome outcome;
  size_t calls = 0;
  try {
    MountedTape tape([] { throw std::runtime_error("mount failed"); },
                     [&] {
                       ++calls;
                       return DriveCleaner::CleanupResult {};
                     },
                     outcome,
                     lc);
    FAIL() << "Mount failure must escape construction";
  } catch (const std::runtime_error& ex) {
    EXPECT_STREQ("mount failed", ex.what());
  }
  EXPECT_EQ(1, calls);
  EXPECT_TRUE(outcome.driveReusable());
}

TEST(MountedTapeTest, CleanupExceptionsDoNotReplaceMountFailure) {
  log::StringLogger cleanupLogger("host", "MountedTapeTest", log::DEBUG);
  log::LogContext lc(cleanupLogger);
  MountedTape::Outcome outcome;
  try {
    MountedTape tape([] { throw 42; },
                     []() -> DriveCleaner::CleanupResult { throw std::runtime_error("cleanup failed"); },
                     outcome,
                     lc);
    FAIL() << "Mount failure must escape construction";
  } catch (int failure) {
    EXPECT_EQ(42, failure);
  }
  EXPECT_FALSE(outcome.driveReusable());
  ASSERT_NE(nullptr, outcome.exception);
  EXPECT_THROW(std::rethrow_exception(outcome.exception), std::runtime_error);
}

TEST(MountedTapeTest, RejectsEmptyMountWithoutCleaning) {
  log::StringLogger cleanupLogger("host", "MountedTapeTest", log::DEBUG);
  log::LogContext lc(cleanupLogger);
  MountedTape::Outcome outcome;
  size_t calls = 0;
  EXPECT_THROW((MountedTape {MountedTape::Mount {},
                             [&] {
                               ++calls;
                               return DriveCleaner::CleanupResult {};
                             },
                             outcome,
                             lc}),
               std::invalid_argument);
  EXPECT_EQ(0, calls);
}

TEST(MountedTapeTest, RejectsEmptyCleanup) {
  log::StringLogger cleanupLogger("host", "MountedTapeTest", log::DEBUG);
  log::LogContext lc(cleanupLogger);
  MountedTape::Outcome outcome;
  EXPECT_THROW((MountedTape {[] {}, MountedTape::Cleanup {}, outcome, lc}), std::invalid_argument);
}

}  // namespace
}  // namespace cta::tape::daemon

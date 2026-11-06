/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "taped/TapedConfig.hpp"

#include "runtime/config/ConfigLoader.hpp"
#include "scheduler/MountSelectionMode.hpp"

#include <gtest/gtest.h>

namespace unitTests {

TEST(TapedMountsConfig, DefaultsToAll) {
  const auto config = cta::runtime::loadFromTomlString<cta::tape::daemon::MountsConfig>("", "test", true);
  EXPECT_EQ("all", config.mode);
}

TEST(TapedMountsConfig, ParsesSupportedModes) {
  for (const auto* mode : {"all", "archive_only", "retrieve_only"}) {
    const auto config =
      cta::runtime::loadFromTomlString<cta::tape::daemon::MountsConfig>(std::string("mode = \"") + mode + "\"",
                                                                        "test",
                                                                        true);
    EXPECT_EQ(mode, config.mode);
    EXPECT_NO_THROW(cta::mountSelectionModeFromString(config.mode));
  }
}

TEST(TapedMountsConfig, RejectsUnknownMode) {
  EXPECT_THROW(
    cta::runtime::loadFromTomlString<cta::tape::daemon::MountsConfig>("mode = \"retrive_only\"", "test", true),
    cta::exception::UserError);
  EXPECT_THROW(cta::mountSelectionModeFromString("retrive_only"), std::invalid_argument);
}

TEST(TapedMountsConfig, FiltersMixedCandidatesIncludingRepack) {
  using cta::MountSelectionMode;
  using cta::common::dataStructures::MountType;

  for (const auto type : {MountType::ArchiveForUser, MountType::ArchiveForRepack, MountType::Retrieve}) {
    EXPECT_TRUE(cta::allowsMount(MountSelectionMode::All, type));
    EXPECT_EQ(type != MountType::Retrieve, cta::allowsMount(MountSelectionMode::ArchiveOnly, type));
    EXPECT_EQ(type == MountType::Retrieve, cta::allowsMount(MountSelectionMode::RetrieveOnly, type));
  }
}

}  // namespace unitTests

/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/LibrarySlotParser.hpp"

#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_mediachanger_LibrarySlotParserTest : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(cta_mediachanger_LibrarySlotParserTest, dummy) {
  using namespace cta::mediachanger;

  std::unique_ptr<LibrarySlot> slot;
  ASSERT_NO_THROW(slot = LibrarySlotParser::parse("dummy"));
  ASSERT_NE((LibrarySlot*) 0, slot.get());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_DUMMY, slot->getLibraryType());
}

TEST_F(cta_mediachanger_LibrarySlotParserTest, scsi) {
  using namespace cta::mediachanger;

  std::unique_ptr<LibrarySlot> slot;
  ASSERT_NO_THROW(slot = LibrarySlotParser::parse("smc1"));
  ASSERT_NE((LibrarySlot*) 0, slot.get());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, slot->getLibraryType());
}

TEST_F(cta_mediachanger_LibrarySlotParserTest, nonsense) {
  using namespace cta::mediachanger;

  std::unique_ptr<LibrarySlot> slot;
  ASSERT_THROW(slot = LibrarySlotParser::parse("nonsense"), cta::exception::Exception);
}

}  // namespace unitTests

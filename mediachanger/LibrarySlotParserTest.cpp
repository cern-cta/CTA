/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/exception/Exception.hpp"
#include "mediachanger/LibrarySlotParser.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_mediachanger_LibrarySlotParserTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_mediachanger_LibrarySlotParserTest, manual) {
  using namespace cta::mediachanger;

  std::unique_ptr<LibrarySlot> slot;
  ASSERT_NO_THROW(slot.reset(LibrarySlotParser::parse("manual")));
  ASSERT_NE((LibrarySlot*)0, slot.get());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_MANUAL, slot->getLibraryType());
}

TEST_F(cta_mediachanger_LibrarySlotParserTest, scsi) {
  using namespace cta::mediachanger;

  std::unique_ptr<LibrarySlot> slot;
  ASSERT_NO_THROW(slot.reset(LibrarySlotParser::parse("smc1")));
  ASSERT_NE((LibrarySlot*)0, slot.get());
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, slot->getLibraryType());
}

TEST_F(cta_mediachanger_LibrarySlotParserTest, nonsense) {
  using namespace cta::mediachanger;

  std::unique_ptr<LibrarySlot> slot;
  ASSERT_THROW(slot.reset(LibrarySlotParser::parse("nonsense")),
    cta::exception::Exception);
}

} // namespace unitTests

/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/exception/Exception.hpp"
#include "mediachanger/ManualLibrarySlot.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_mediachanger_ManualLibrarySlotTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_mediachanger_ManualLibrarySlotTest, manual1234) {
  using namespace cta::mediachanger;

  std::unique_ptr<ManualLibrarySlot> slot;
  ASSERT_NO_THROW(slot.reset(new ManualLibrarySlot("manual1234")));

  ASSERT_EQ(TAPE_LIBRARY_TYPE_MANUAL, slot->getLibraryType());
  ASSERT_EQ("manual1234", slot->str());
}

TEST_F(cta_mediachanger_ManualLibrarySlotTest, notmanual) {
  using namespace cta::mediachanger;

  std::unique_ptr<ManualLibrarySlot> slot;
  ASSERT_THROW(slot.reset(new ManualLibrarySlot("notmanual")),
    cta::exception::Exception);
}

TEST_F(cta_mediachanger_ManualLibrarySlotTest, clone) {
  using namespace cta::mediachanger;

  std::unique_ptr<ManualLibrarySlot> slot1;
  ASSERT_NO_THROW(slot1.reset(new ManualLibrarySlot("manual1234")));

  ASSERT_EQ(TAPE_LIBRARY_TYPE_MANUAL, slot1->getLibraryType());
  ASSERT_EQ("manual1234", slot1->str());

  std::unique_ptr<ManualLibrarySlot> slot2;
  ASSERT_NO_THROW(slot2.reset((ManualLibrarySlot*)slot1->clone()));

  ASSERT_EQ(TAPE_LIBRARY_TYPE_MANUAL, slot2->getLibraryType());
  ASSERT_EQ("manual1234", slot2->str());
}

} // namespace unitTests

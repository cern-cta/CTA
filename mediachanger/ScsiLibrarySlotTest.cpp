/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/ScsiLibrarySlot.hpp"

#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_mediachanger_ScsiLibrarySlotTest : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(cta_mediachanger_ScsiLibrarySlotTest, goodDay) {
  using namespace cta::mediachanger;

  ScsiLibrarySlot slot(2);
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, slot.getLibraryType());
  ASSERT_EQ(std::string("smc2"), slot.str());
  ASSERT_EQ((uint16_t) 2, slot.getDrvOrd());
}

TEST_F(cta_mediachanger_ScsiLibrarySlotTest, clone) {
  using namespace cta::mediachanger;

  std::unique_ptr<ScsiLibrarySlot> slot1;
  ASSERT_NO_THROW(slot1.reset(new ScsiLibrarySlot(2)));
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, slot1->getLibraryType());
  ASSERT_EQ(std::string("smc2"), slot1->str());

  std::unique_ptr<ScsiLibrarySlot> slot2;
  ASSERT_NO_THROW(slot2.reset((ScsiLibrarySlot*) slot1->clone()));
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, slot2->getLibraryType());
  ASSERT_EQ(std::string("smc2"), slot2->str());
}

}  // namespace unitTests

/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/mediachanger/AcsLibrarySlot.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class castor_mediachanger_AcsLibrarySlotTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_mediachanger_AcsLibrarySlotTest, goodDay) {
  using namespace castor::mediachanger;
  AcsLibrarySlot slot(11, 22, 33, 44);
  ASSERT_EQ(TAPE_LIBRARY_TYPE_ACS, slot.getLibraryType());
  ASSERT_EQ(std::string("acs11,22,33,44"), slot.str());
  ASSERT_EQ((uint32_t)11, slot.getAcs());
  ASSERT_EQ((uint32_t)22, slot.getLsm());
  ASSERT_EQ((uint32_t)33, slot.getPanel());
  ASSERT_EQ((uint32_t)44, slot.getDrive());
}

TEST_F(castor_mediachanger_AcsLibrarySlotTest, clone) {
  using namespace castor::mediachanger;

  std::auto_ptr<AcsLibrarySlot> slot1;
  ASSERT_NO_THROW(slot1.reset(new AcsLibrarySlot(11, 22, 33, 44)));
  ASSERT_EQ(TAPE_LIBRARY_TYPE_ACS, slot1->getLibraryType());
  ASSERT_EQ(std::string("acs11,22,33,44"), slot1->str());
  ASSERT_EQ((uint32_t)11, slot1->getAcs());
  ASSERT_EQ((uint32_t)22, slot1->getLsm());
  ASSERT_EQ((uint32_t)33, slot1->getPanel());
  ASSERT_EQ((uint32_t)44, slot1->getDrive());

  std::auto_ptr<AcsLibrarySlot> slot2;
  ASSERT_NO_THROW(slot2.reset((AcsLibrarySlot*)slot1->clone()));
  ASSERT_EQ(TAPE_LIBRARY_TYPE_ACS, slot2->getLibraryType());
  ASSERT_EQ(std::string("acs11,22,33,44"), slot2->str());
  ASSERT_EQ((uint32_t)11, slot2->getAcs());
  ASSERT_EQ((uint32_t)22, slot2->getLsm());
  ASSERT_EQ((uint32_t)33, slot2->getPanel());
  ASSERT_EQ((uint32_t)44, slot2->getDrive());
}

} // namespace unitTests

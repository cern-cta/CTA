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

#include "castor/exception/Exception.hpp"
#include "castor/mediachanger/ScsiLibrarySlot.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class castor_mediachanger_ScsiLibrarySlotTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_mediachanger_ScsiLibrarySlotTest, goodDay) {
  using namespace castor::mediachanger;

  ScsiLibrarySlot slot(2);
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, slot.getLibraryType());
  ASSERT_EQ(std::string("smc2"), slot.str());
  ASSERT_EQ((uint16_t)2, slot.getDrvOrd());
}

TEST_F(castor_mediachanger_ScsiLibrarySlotTest, clone) {
  using namespace castor::mediachanger;

  std::unique_ptr<ScsiLibrarySlot> slot1;
  ASSERT_NO_THROW(slot1.reset(new ScsiLibrarySlot(2)));
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, slot1->getLibraryType());
  ASSERT_EQ(std::string("smc2"), slot1->str());

  std::unique_ptr<ScsiLibrarySlot> slot2;
  ASSERT_NO_THROW(slot2.reset((ScsiLibrarySlot*)slot1->clone()));
  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, slot2->getLibraryType());
  ASSERT_EQ(std::string("smc2"), slot2->str());
}

} // namespace unitTests

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
#include "castor/mediachanger/TapeLibrarySlot.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class castor_mediachanger_TapeLibrarySlotTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_mediachanger_TapeLibrarySlotTest, getLibraryTypeNone) {
  using namespace castor::mediachanger;

  TapeLibrarySlot slot;

  ASSERT_EQ(TAPE_LIBRARY_TYPE_NONE, slot.getLibraryType());
}

TEST_F(castor_mediachanger_TapeLibrarySlotTest, getLibraryTypeAcs) {
  using namespace castor::mediachanger;

  TapeLibrarySlot slot("acs@rmc_host,1,2,3,4");

  ASSERT_EQ(TAPE_LIBRARY_TYPE_ACS, slot.getLibraryType());
}

TEST_F(castor_mediachanger_TapeLibrarySlotTest, getLibraryTypeManual) {
  using namespace castor::mediachanger;

  TapeLibrarySlot slot("manual@opaque_drive_id");

  ASSERT_EQ(TAPE_LIBRARY_TYPE_MANUAL, slot.getLibraryType());
}

TEST_F(castor_mediachanger_TapeLibrarySlotTest, getLibraryTypeScsi) {
  using namespace castor::mediachanger;

  TapeLibrarySlot slot("smc@rmc_host,1");

  ASSERT_EQ(TAPE_LIBRARY_TYPE_SCSI, slot.getLibraryType());
}

TEST_F(castor_mediachanger_TapeLibrarySlotTest, getLibraryTypeNonsense) {
  using namespace castor::mediachanger;

  std::auto_ptr<TapeLibrarySlot> slot;

  ASSERT_THROW(slot.reset(new TapeLibrarySlot("nonsense")),
    castor::exception::Exception);
}

} // namespace unitTests

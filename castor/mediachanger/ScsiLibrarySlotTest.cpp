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

#include "castor/mediachanger/ScsiLibrarySlot.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_mediachanger_ScsiLibrarySlotTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_mediachanger_ScsiLibrarySlotTest, goodDayParsing) {
  using namespace castor::mediachanger;
 
  ScsiLibrarySlot slot;
  ASSERT_EQ(std::string(""), slot.getRmcHostName());
  ASSERT_EQ((uint16_t)0, slot.getDrvOrd());

  const std::string str = "smc@rmc_host,4";
  ASSERT_NO_THROW(slot = ScsiLibrarySlot(str));
  ASSERT_EQ(std::string("rmc_host"), slot.getRmcHostName());
  ASSERT_EQ((uint16_t)4, slot.getDrvOrd());
}

TEST_F(castor_mediachanger_ScsiLibrarySlotTest, badDayParsing) {
  using namespace castor::mediachanger;

  const std::string str = "nonsense";
  ASSERT_THROW(ScsiLibrarySlot slot(str), castor::exception::Exception);
}

} // namespace unitTests

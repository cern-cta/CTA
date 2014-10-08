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

namespace unitTests {

class castor_mediachanger_AcsLibrarySlotTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_mediachanger_AcsLibrarySlotTest, goodDayParsing) {
  using namespace castor::mediachanger;
 
  AcsLibrarySlot slot;
  ASSERT_EQ(std::string("acs=0 lsm=0 panel=0 drive=0"), slot.str());
  ASSERT_EQ((uint32_t)0, slot.getAcs());
  ASSERT_EQ((uint32_t)0, slot.getLsm());
  ASSERT_EQ((uint32_t)0, slot.getPanel());
  ASSERT_EQ((uint32_t)0, slot.getDrive());

  const std::string str = "acs11,22,33,44";
  ASSERT_NO_THROW(slot = AcsLibrarySlot(str));
  ASSERT_EQ(std::string("acs=11 lsm=22 panel=33 drive=44"), slot.str());
  ASSERT_EQ((uint32_t)11, slot.getAcs());
  ASSERT_EQ((uint32_t)22, slot.getLsm());
  ASSERT_EQ((uint32_t)33, slot.getPanel());
  ASSERT_EQ((uint32_t)44, slot.getDrive());
}

TEST_F(castor_mediachanger_AcsLibrarySlotTest, badDayParsing) {
  using namespace castor::mediachanger;

  const std::string str = "nonsense";
  ASSERT_THROW(AcsLibrarySlot slot(str), castor::exception::InvalidArgument);
  
  const std::string strAcs = "asc0,1,2,3";
  ASSERT_THROW(AcsLibrarySlot slot(strAcs), castor::exception::InvalidArgument);
  
  const std::string strACS_NUMBER = "acssd1,1,2,3";
  ASSERT_THROW(AcsLibrarySlot slot(strACS_NUMBER), castor::exception::InvalidArgument);

  const std::string strLsm = "acs0,1111,2,3";
  ASSERT_THROW(AcsLibrarySlot slot(strLsm), castor::exception::InvalidArgument);

  const std::string strPanel = "acs0,111,ABC,3";
  ASSERT_THROW(AcsLibrarySlot slot(strPanel), castor::exception::InvalidArgument);

  const std::string strDrive = "acs0,111,222,3 ";
  ASSERT_THROW(AcsLibrarySlot slot(strDrive), castor::exception::InvalidArgument);
}

} // namespace unitTests

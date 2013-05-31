// ----------------------------------------------------------------------
// File: SCSI/DeviceTest.cc
// Author: Eric Cano - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include <gtest/gtest.h>
#include <gmock/gmock-cardinalities.h>
#include "Device.hh"
#include "../system/Wrapper.hh"

using ::testing::AtLeast;
using ::testing::Return;
using ::testing::_;

TEST(DeviceList, TriesToFind) {
  Tape::System::mockWrapper sysWrapper;
  
  /* Give minimal service output from mock system calls:
   * at least pretend there is a directory to scan */
  /* _ means anything goes */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(1);
  EXPECT_CALL(sysWrapper, readdir(sysWrapper.m_DIR)).Times(1);
  EXPECT_CALL(sysWrapper, closedir(sysWrapper.m_DIR)).Times(1);
  
  SCSI::DeviceList<Tape::System::mockWrapper> dl(sysWrapper);
}

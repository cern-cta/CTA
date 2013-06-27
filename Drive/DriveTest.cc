// ----------------------------------------------------------------------
// File: SCSI/DriveTest.cc
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
#include "../SCSI/Device.hh"
#include "../System/Wrapper.hh"
#include "Drive.hh"

using ::testing::AtLeast;
using ::testing::Return;
using ::testing::_;

TEST(TapeDrive, OpensCorrectly) {
  /* Prepare the test harness */
  Tape::System::mockWrapper sysWrapper;
  sysWrapper.fake.setupSLC5();
  sysWrapper.delegateToFake();
  
  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(3);
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(3);
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(3);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(14);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(20);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,_)).Times(2);
  EXPECT_CALL(sysWrapper, close(_)).Times(14);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(3);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(7);
  
  /* Test: detect devices, then open the device files */
  SCSI::DeviceVector<Tape::System::mockWrapper> dl(sysWrapper);
  for (std::vector<SCSI::DeviceInfo>::iterator i = dl.begin();
      i != dl.end(); i++) {
    if (SCSI::Types::tape == i->type) {
      Tape::Drive<Tape::System::mockWrapper> drive(*i, sysWrapper);
    }
  }
}

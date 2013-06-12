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

Tape::System::mockWrapper sysWrapper;

TEST(DeviceList, TriesToFind) {
  /* Give minimal service output from mock system calls:
   * at least pretend there is a directory to scan */
  /* _ means anything goes */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(1);
  EXPECT_CALL(sysWrapper, readdir(sysWrapper.m_DIR)).Times(1);
  EXPECT_CALL(sysWrapper, closedir(sysWrapper.m_DIR)).Times(1);

  SCSI::DeviceVector<Tape::System::virtualWrapper> dl(sysWrapper);
}

TEST(DeviceList, ScansCorrectly) {
  /* Configure the mock to use fake */
  sysWrapper.delegateToFake();
  /* Populate the test harness */
  sysWrapper.fake.setupSLC5();

  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(3);
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(1);
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(3);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(3);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(6);
  EXPECT_CALL(sysWrapper, close(_)).Times(3);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(3);

  /* Everything should have been found correctly */

  SCSI::DeviceVector<Tape::System::virtualWrapper> dl(sysWrapper);

  ASSERT_EQ(dl.size(), 3);
  ASSERT_EQ(dl[0].type, SCSI::Types::mediumChanger);
  ASSERT_EQ(dl[1].type, SCSI::Types::tape);
  ASSERT_EQ(dl[2].type, SCSI::Types::tape);
  ASSERT_EQ(dl[0].sg_dev, "/dev/sg2");
  ASSERT_EQ(dl[1].sg_dev, "/dev/sg0");
  ASSERT_EQ(dl[2].sg_dev, "/dev/sg1");
  ASSERT_EQ(dl[0].st_dev, "");
  ASSERT_EQ(dl[1].st_dev, "/dev/st0");
  ASSERT_EQ(dl[2].st_dev, "/dev/st1");
  ASSERT_EQ(dl[0].nst_dev, "");
  ASSERT_EQ(dl[1].nst_dev, "/dev/nst0");
  ASSERT_EQ(dl[2].nst_dev, "/dev/nst1");
  ASSERT_EQ(dl[0].sysfs_entry, "/sys/devices/pseudo_0/adapter0/host3/target3:0:0/3:0:0:0");
  ASSERT_EQ(dl[1].sysfs_entry, "/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0");
  ASSERT_EQ(dl[2].sysfs_entry, "/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0");
  ASSERT_EQ(dl[0].sg.major, 21);
  ASSERT_EQ(dl[0].sg.minor, 2);
  ASSERT_EQ(dl[1].sg.major, 21);
  ASSERT_EQ(dl[1].sg.minor, 0);
  ASSERT_EQ(dl[2].sg.major, 21);
  ASSERT_EQ(dl[2].sg.minor, 1);
  ASSERT_EQ(dl[0].st.major, -1);
  ASSERT_EQ(dl[0].st.minor, -1);
  ASSERT_EQ(dl[1].st.major, 9);
  ASSERT_EQ(dl[1].st.minor, 0);
  ASSERT_EQ(dl[2].st.major, 9);
  ASSERT_EQ(dl[2].st.minor, 1);
  ASSERT_EQ(dl[0].nst.major, -1);
  ASSERT_EQ(dl[0].nst.minor, -1);
  ASSERT_EQ(dl[1].nst.major, 9);
  ASSERT_EQ(dl[1].nst.minor, 128);
  ASSERT_EQ(dl[2].nst.major, 9);
  ASSERT_EQ(dl[2].nst.minor, 129);
}

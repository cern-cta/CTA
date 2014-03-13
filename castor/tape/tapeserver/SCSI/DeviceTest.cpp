/******************************************************************************
 *                      DeviceTest.cpp
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

#include <gtest/gtest.h>
#include <gmock/gmock-cardinalities.h>
#include "Device.hpp"
#include "../system/Wrapper.hpp"

using ::testing::AtLeast;
using ::testing::Return;
using ::testing::_;

namespace unitTests {
castor::tape::System::mockWrapper sysWrapper;

TEST(castor_tape_SCSI_DeviceList, TriesToFind) {
  /* Give minimal service output from mock system calls:
   * at least pretend there is a directory to scan */
  /* _ means anything goes */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(1);
  EXPECT_CALL(sysWrapper, readdir(sysWrapper.m_DIR)).Times(1);
  EXPECT_CALL(sysWrapper, closedir(sysWrapper.m_DIR)).Times(1);

  castor::tape::SCSI::DeviceVector dl(sysWrapper);
}

TEST(castor_tape_SCSI_DeviceList, ScansCorrectly) {
  /* Configure the mock to use fake */
  sysWrapper.delegateToFake();
  /* Populate the test harness */
  sysWrapper.fake.setupSLC5();

  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(3);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(19);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(38);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(19);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(3);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(7);

  /* Everything should have been found correctly */

  castor::tape::SCSI::DeviceVector dl(sysWrapper);

  ASSERT_EQ(3U, dl.size());
  ASSERT_EQ(castor::tape::SCSI::Types::mediumChanger, dl[0].type);
  ASSERT_EQ(castor::tape::SCSI::Types::tape,          dl[1].type);
  ASSERT_EQ(castor::tape::SCSI::Types::tape,          dl[2].type);
  ASSERT_EQ( "/dev/sg2", dl[0].sg_dev);
  ASSERT_EQ( "/dev/sg0", dl[1].sg_dev);
  ASSERT_EQ( "/dev/sg1", dl[2].sg_dev);
  ASSERT_EQ(         "", dl[0].st_dev);
  ASSERT_EQ( "/dev/st0", dl[1].st_dev);
  ASSERT_EQ( "/dev/st1", dl[2].st_dev);
  ASSERT_EQ(         "", dl[0].nst_dev);
  ASSERT_EQ("/dev/nst0", dl[1].nst_dev);
  ASSERT_EQ("/dev/nst1", dl[2].nst_dev);
  ASSERT_EQ("/sys/devices/pseudo_0/adapter0/host3/target3:0:0/3:0:0:0", dl[0].sysfs_entry);
  ASSERT_EQ("/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0", dl[1].sysfs_entry);
  ASSERT_EQ("/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0", dl[2].sysfs_entry);
  ASSERT_EQ( 21U, dl[0].sg.major);
  ASSERT_EQ(  2U, dl[0].sg.minor);
  ASSERT_EQ( 21U, dl[1].sg.major);
  ASSERT_EQ(  0U, dl[1].sg.minor);
  ASSERT_EQ( 21U, dl[2].sg.major);
  ASSERT_EQ(  1U, dl[2].sg.minor);
  ASSERT_EQ(  0U, dl[0].st.major);
  ASSERT_EQ(  0U, dl[0].st.minor);
  ASSERT_EQ(  9U, dl[1].st.major);
  ASSERT_EQ(  0U, dl[1].st.minor);
  ASSERT_EQ(  9U, dl[2].st.major);
  ASSERT_EQ(  1U, dl[2].st.minor);
  ASSERT_EQ(  0U, dl[0].nst.major);
  ASSERT_EQ(  0U, dl[0].nst.minor);
  ASSERT_EQ(  9U, dl[1].nst.major);
  ASSERT_EQ(128U, dl[1].nst.minor);
  ASSERT_EQ(  9U, dl[2].nst.major);
  ASSERT_EQ(129U, dl[2].nst.minor);
  ASSERT_EQ("STK", dl[0].vendor);
  ASSERT_EQ("STK", dl[1].vendor);
  ASSERT_EQ("STK", dl[2].vendor);
  ASSERT_EQ("VL32STK1", dl[0].product);
  ASSERT_EQ("T10000B", dl[1].product);
  ASSERT_EQ("T10000B", dl[2].product);
  ASSERT_EQ("0104", dl[0].productRevisionLevel);
  ASSERT_EQ("0104", dl[1].productRevisionLevel);
  ASSERT_EQ("0104", dl[2].productRevisionLevel);
}

}

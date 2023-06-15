/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <gtest/gtest.h>
#include <gmock/gmock-cardinalities.h>
#include "Device.hpp"
#include "../system/Wrapper.hpp"
#include "common/exception/Errnum.hpp"

using ::testing::_;
using ::testing::AtLeast;
using ::testing::Return;

namespace unitTests {

TEST(castor_tape_SCSI_DeviceList, TriesToFind) {
  /* Give minimal service output from mock system calls:
   * at least pretend there is a directory to scan */
  /* _ means anything goes */
  castor::tape::System::mockWrapper sysWrapper;
  EXPECT_CALL(sysWrapper, opendir(_)).Times(1);
  EXPECT_CALL(sysWrapper, readdir(sysWrapper.m_DIR)).Times(1);
  EXPECT_CALL(sysWrapper, closedir(sysWrapper.m_DIR)).Times(1);

  castor::tape::SCSI::DeviceVector dl(sysWrapper);
}

TEST(castor_tape_SCSI_DeviceList, ScansCorrectly) {
  castor::tape::System::mockWrapper sysWrapper;
  /* Configure the mock to use fake */
  sysWrapper.delegateToFake();
  /* Populate the test harness */
  sysWrapper.fake.setupSLC5();
  // TODO: Test against SLC6 setup

  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(6);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(38);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(76);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(38);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(6);
  EXPECT_CALL(sysWrapper, stat(_, _)).Times(14);

  /* Everything should have been found correctly */

  castor::tape::SCSI::DeviceVector dl(sysWrapper);

  ASSERT_EQ(6U, dl.size());
  ASSERT_EQ(castor::tape::SCSI::Types::mediumChanger, dl[0].type);
  ASSERT_EQ(castor::tape::SCSI::Types::tape, dl[1].type);
  ASSERT_EQ(castor::tape::SCSI::Types::tape, dl[2].type);
  ASSERT_EQ(castor::tape::SCSI::Types::mediumChanger, dl[3].type);
  ASSERT_EQ(castor::tape::SCSI::Types::tape, dl[4].type);
  ASSERT_EQ(castor::tape::SCSI::Types::tape, dl[5].type);

  ASSERT_EQ("/dev/sg2", dl[0].sg_dev);
  ASSERT_EQ("/dev/sg0", dl[1].sg_dev);
  ASSERT_EQ("/dev/sg1", dl[2].sg_dev);
  ASSERT_EQ("/dev/sg5", dl[3].sg_dev);
  ASSERT_EQ("/dev/sg3", dl[4].sg_dev);
  ASSERT_EQ("/dev/sg4", dl[5].sg_dev);

  ASSERT_EQ("", dl[0].st_dev);
  ASSERT_EQ("/dev/st0", dl[1].st_dev);
  ASSERT_EQ("/dev/st1", dl[2].st_dev);
  ASSERT_EQ("", dl[3].st_dev);
  ASSERT_EQ("/dev/st2", dl[4].st_dev);
  ASSERT_EQ("/dev/st3", dl[5].st_dev);

  ASSERT_EQ("", dl[0].nst_dev);
  ASSERT_EQ("/dev/nst0", dl[1].nst_dev);
  ASSERT_EQ("/dev/nst1", dl[2].nst_dev);
  ASSERT_EQ("", dl[3].nst_dev);
  ASSERT_EQ("/dev/nst2", dl[4].nst_dev);
  ASSERT_EQ("/dev/nst3", dl[5].nst_dev);

  ASSERT_EQ("/sys/devices/pseudo_0/adapter0/host3/target3:0:0/3:0:0:0", dl[0].sysfs_entry);
  ASSERT_EQ("/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0", dl[1].sysfs_entry);
  ASSERT_EQ("/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0", dl[2].sysfs_entry);
  ASSERT_EQ("/sys/devices/pseudo_0/adapter0/host2/target2:0:0/2:0:0:0", dl[3].sysfs_entry);
  ASSERT_EQ("/sys/devices/pseudo_0/adapter0/host2/target2:0:1/2:0:1:0", dl[4].sysfs_entry);
  ASSERT_EQ("/sys/devices/pseudo_0/adapter0/host2/target2:0:2/2:0:2:0", dl[5].sysfs_entry);

  ASSERT_EQ(21U, dl[0].sg.major);
  ASSERT_EQ(2U, dl[0].sg.minor);
  ASSERT_EQ(21U, dl[1].sg.major);
  ASSERT_EQ(0U, dl[1].sg.minor);
  ASSERT_EQ(21U, dl[2].sg.major);
  ASSERT_EQ(1U, dl[2].sg.minor);
  ASSERT_EQ(21U, dl[3].sg.major);
  ASSERT_EQ(5U, dl[3].sg.minor);
  ASSERT_EQ(21U, dl[4].sg.major);
  ASSERT_EQ(3U, dl[4].sg.minor);
  ASSERT_EQ(21U, dl[5].sg.major);
  ASSERT_EQ(4U, dl[5].sg.minor);
  ASSERT_EQ(0U, dl[0].st.major);
  ASSERT_EQ(0U, dl[0].st.minor);
  ASSERT_EQ(9U, dl[1].st.major);
  ASSERT_EQ(0U, dl[1].st.minor);
  ASSERT_EQ(9U, dl[2].st.major);
  ASSERT_EQ(1U, dl[2].st.minor);
  ASSERT_EQ(0U, dl[3].st.major);
  ASSERT_EQ(0U, dl[3].st.minor);
  ASSERT_EQ(9U, dl[4].st.major);
  ASSERT_EQ(2U, dl[4].st.minor);
  ASSERT_EQ(9U, dl[5].st.major);
  ASSERT_EQ(3U, dl[5].st.minor);
  ASSERT_EQ(0U, dl[0].nst.major);
  ASSERT_EQ(0U, dl[0].nst.minor);
  ASSERT_EQ(9U, dl[1].nst.major);
  ASSERT_EQ(128U, dl[1].nst.minor);
  ASSERT_EQ(9U, dl[2].nst.major);
  ASSERT_EQ(129U, dl[2].nst.minor);
  ASSERT_EQ(0U, dl[3].nst.major);
  ASSERT_EQ(0U, dl[3].nst.minor);
  ASSERT_EQ(9U, dl[4].nst.major);
  ASSERT_EQ(130U, dl[4].nst.minor);
  ASSERT_EQ(9U, dl[5].nst.major);
  ASSERT_EQ(131U, dl[5].nst.minor);
  ASSERT_EQ("STK", dl[0].vendor);
  ASSERT_EQ("STK", dl[1].vendor);
  ASSERT_EQ("STK", dl[2].vendor);
  ASSERT_EQ("VL32STK1", dl[0].product);
  ASSERT_EQ("T10000B", dl[1].product);
  ASSERT_EQ("T10000B", dl[2].product);
  ASSERT_EQ("0104", dl[0].productRevisionLevel);
  ASSERT_EQ("0104", dl[1].productRevisionLevel);
  ASSERT_EQ("0104", dl[2].productRevisionLevel);
  ASSERT_EQ("IBM", dl[3].vendor);
  ASSERT_EQ("IBM", dl[4].vendor);
  ASSERT_EQ("IBM", dl[5].vendor);
  ASSERT_EQ("03584L22", dl[3].product);
  ASSERT_EQ("03592E08", dl[4].product);
  ASSERT_EQ("03592E08", dl[5].product);
  ASSERT_EQ("F030", dl[3].productRevisionLevel);
  ASSERT_EQ("460E", dl[4].productRevisionLevel);
  ASSERT_EQ("460E", dl[5].productRevisionLevel);
}

TEST(castor_tape_SCSI_DeviceList, FindBySymlink) {
  castor::tape::System::mockWrapper sysWrapper;
  sysWrapper.delegateToFake();
  sysWrapper.fake.setupForVirtualDriveSLC6();

  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, open(_, _)).Times(AtLeast(20));
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(AtLeast(38));
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(AtLeast(19));
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, stat(_, _)).Times(AtLeast(7));

  castor::tape::SCSI::DeviceVector dl(sysWrapper);
  ASSERT_NO_THROW(dl.findBySymlink("/dev/tape_T10D6116"));
  ASSERT_THROW(dl.findBySymlink("NoSuchPath"), cta::exception::Errnum);
  ASSERT_THROW(dl.findBySymlink("/dev/noSuchTape"), castor::tape::SCSI::DeviceVector::NotFound);
  castor::tape::SCSI::DeviceInfo& di = dl.findBySymlink("/dev/tape_T10D6116");
  // The symlink is supposed to point to nst0 which is 9,128 (maj,min)
  ASSERT_EQ(9U, di.nst.major);
  ASSERT_EQ(128U, di.nst.minor);
  // We expect to get a module type of "VIRTUAL" here. This will be used
  // in other tests
  ASSERT_EQ("VIRTUAL", di.product);
}

}  // namespace unitTests

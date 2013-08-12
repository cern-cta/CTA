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
using ::testing::An;

namespace UnitTests {

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
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(2);
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

TEST(TapeDrive, getPositionInfoAndPositionToLogicalObject) {
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
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(2);
  EXPECT_CALL(sysWrapper, close(_)).Times(14);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(3);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(7);
  
  /* Test: detect devices, then open the device files */
  SCSI::DeviceVector<Tape::System::mockWrapper> dl(sysWrapper);
  for (std::vector<SCSI::DeviceInfo>::iterator i = dl.begin();
      i != dl.end(); i++) {
    if (SCSI::Types::tape == i->type) {
      Tape::Drive<Tape::System::mockWrapper> drive(*i, sysWrapper);
      Tape::positionInfo posInfo;
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(1);      
      posInfo = drive.getPositionInfo();

      ASSERT_EQ(0xABCDEF12U,posInfo.currentPosition);
      ASSERT_EQ(0x12EFCDABU,posInfo.oldestDirtyObject);
      ASSERT_EQ(0xABCDEFU,posInfo.dirtyObjectsCount);
      ASSERT_EQ(0x12EFCDABU,posInfo.dirtyBytesCount);
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(1);      
      drive.positionToLogicalObject(0xABCDEF0);
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(1);
      posInfo = drive.getPositionInfo();
      
      ASSERT_EQ(0xABCDEF0U,posInfo.currentPosition);
      ASSERT_EQ(0xABCDEF0U,posInfo.oldestDirtyObject);
      ASSERT_EQ(0x0U,posInfo.dirtyObjectsCount);
      ASSERT_EQ(0x0U,posInfo.dirtyBytesCount);
    }
  }
} 
TEST(TapeDrive, setDensityAndCompression) {
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
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(2);
  EXPECT_CALL(sysWrapper, close(_)).Times(14);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(3);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(7);
  
  /* Test: detect devices, then open the device files */
  SCSI::DeviceVector<Tape::System::mockWrapper> dl(sysWrapper);
  for (std::vector<SCSI::DeviceInfo>::iterator i = dl.begin();
      i != dl.end(); i++) {
    if (SCSI::Types::tape == i->type) {
      Tape::Drive<Tape::System::mockWrapper> drive(*i, sysWrapper);

      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(2);      
      drive.setDensityAndCompression();
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(2);      
      drive.setDensityAndCompression(true);
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(2);      
      drive.setDensityAndCompression(false);

      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(2);      
      drive.setDensityAndCompression(0x42,true);
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(2);      
      drive.setDensityAndCompression(0x46,false);
    }
  }
}

TEST(TapeDrive, setStDriverOptions) {
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
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(2);
  EXPECT_CALL(sysWrapper, close(_)).Times(14);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(3);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(7);
  
  /* Test: detect devices, then open the device files */
  SCSI::DeviceVector<Tape::System::mockWrapper> dl(sysWrapper);
  for (std::vector<SCSI::DeviceInfo>::iterator i = dl.begin(); i != dl.end(); i++) {
    if (SCSI::Types::tape == i->type) {
      Tape::Drive<Tape::System::mockWrapper> drive(*i, sysWrapper);
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<struct mtop *>())).Times(1);
      drive.setSTBufferWrite(true);
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<struct mtop *>())).Times(1);
      drive.setSTBufferWrite(false);
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<struct mtop *>())).Times(1);
      drive.setSTFastMTEOM(true);
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<struct mtop *>())).Times(1);
      drive.setSTFastMTEOM(false);
    }
  }
}

TEST(TapeDrive, getCompressionAndClearCompressionStats) {
  /* Prepare the test harness */
  Tape::System::mockWrapper sysWrapper;
  sysWrapper.fake.setupSLC5();
  sysWrapper.delegateToFake();
  
  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(3);
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(3);
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(3);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(22);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(20);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(6);
  EXPECT_CALL(sysWrapper, close(_)).Times(22);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(3);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(7);
  
  /* Test: detect devices, then open the device files */
  SCSI::DeviceVector<Tape::System::mockWrapper> dl(sysWrapper);
  for (std::vector<SCSI::DeviceInfo>::iterator i = dl.begin();
      i != dl.end(); i++) {
    if (SCSI::Types::tape == i->type) {
      Tape::Drive<Tape::System::mockWrapper> *drive;
      Tape::compressionStats comp;
      
        {
          drive = new Tape::DriveT10000<Tape::System::mockWrapper>(*i, sysWrapper);

          EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t*>())).Times(1);
          comp = drive->getCompression();
          ASSERT_EQ(0xABCDEF1122334455U, comp.fromHost);
          ASSERT_EQ(0x2233445566778899U, comp.toHost);
          ASSERT_EQ(0x99AABBCCDDEEFF11U, comp.fromTape);
          ASSERT_EQ(0x1122334455667788U, comp.toTape);

          EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t*>())).Times(1);
          drive->clearCompressionStats();

          EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t*>())).Times(1);
          comp = drive->getCompression();
          ASSERT_EQ(0U, comp.fromHost);
          ASSERT_EQ(0U, comp.toHost);
          ASSERT_EQ(0U, comp.fromTape);
          ASSERT_EQ(0U, comp.toTape);

          delete drive;
        }
        {
          drive = new Tape::DriveIBM3592<Tape::System::mockWrapper>(*i, sysWrapper);

          EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t*>())).Times(1);
          comp = drive->getCompression();
          ASSERT_EQ(0x4488CD1000U, comp.fromHost);
          ASSERT_EQ(0x15599DE2000U, comp.toHost);
          ASSERT_EQ(0x377BBFC4400U, comp.fromTape);
          ASSERT_EQ(0x266AAEF3000U, comp.toTape);

          EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t*>())).Times(1);
          drive->clearCompressionStats();

          EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t*>())).Times(1);
          comp = drive->getCompression();
          ASSERT_EQ(0U, comp.fromHost);
          ASSERT_EQ(0U, comp.toHost);
          ASSERT_EQ(0U, comp.fromTape);
          ASSERT_EQ(0U, comp.toTape);

          delete drive;
        }
        {
          drive = new Tape::DriveLTO<Tape::System::mockWrapper>(*i, sysWrapper);

          EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t*>())).Times(1);
          comp = drive->getCompression();
          ASSERT_EQ(0x209DB2BE187D9U, comp.fromHost);
          ASSERT_EQ(0x105707026D088U, comp.toHost);
          ASSERT_EQ(0x928C54DFC8A11U, comp.fromTape);
          ASSERT_EQ(0xA2D3009B64262U, comp.toTape);

          EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t*>())).Times(1);
          drive->clearCompressionStats();

          EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t*>())).Times(1);
          comp = drive->getCompression();
          ASSERT_EQ(0U, comp.fromHost);
          ASSERT_EQ(0U, comp.toHost);
          ASSERT_EQ(0U, comp.fromTape);
          ASSERT_EQ(0U, comp.toTape);

          delete drive;
        }
    }
  }
} 

}

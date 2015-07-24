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

#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/drive/FakeDrive.hpp"
#include "castor/tape/tapeserver/drive/DriveGeneric.hpp"
#include <gtest/gtest.h>
#include <gmock/gmock-cardinalities.h>
#include <typeinfo>
#include <memory>

using ::testing::AtLeast;
using ::testing::Return;
using ::testing::_;
using ::testing::An;

namespace unitTests {

TEST(castor_tape_drive_Drive, OpensCorrectly) {
  /* Prepare the test harness */
  castor::tape::System::mockWrapper sysWrapper;
  sysWrapper.fake.setupSLC5();
  sysWrapper.delegateToFake();
  
  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(3);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(21);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(38);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(21);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(3);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(7);
  
  /* Test: detect devices, then open the device files */
  castor::tape::SCSI::DeviceVector dl(sysWrapper);
  /* Check we detected things properly */
  ASSERT_EQ("VL32STK1", dl[0].product);
  ASSERT_EQ("STK", dl[0].vendor);
  ASSERT_EQ("0104", dl[0].productRevisionLevel);
  ASSERT_EQ("T10000B", dl[1].product);
  ASSERT_EQ("STK", dl[1].vendor);
  ASSERT_EQ("0104", dl[1].productRevisionLevel);
  for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
      i != dl.end(); i++) {
    if (castor::tape::SCSI::Types::tape == i->type) {
      std::string expected_classid (typeid(castor::tape::tapeserver::drive::DriveT10000).name());
      std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface>drive(
        castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
      std::string found_classid (typeid(*drive).name());
      ASSERT_EQ(expected_classid, found_classid);
    }
  }
}

TEST(castor_tape_drive_Drive, getPositionInfoAndPositionToLogicalObject) {
  /* Prepare the test harness */
  castor::tape::System::mockWrapper sysWrapper;
  sysWrapper.fake.setupSLC5();
  sysWrapper.delegateToFake();
  
  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(3);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(21);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(38);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(21);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(3);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(7);
  
  /* Test: detect devices, then open the device files */
  castor::tape::SCSI::DeviceVector dl(sysWrapper);
  for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
      i != dl.end(); i++) {
    if (castor::tape::SCSI::Types::tape == i->type) {
      std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive (
        castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
      castor::tape::tapeserver::drive::positionInfo posInfo;
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(1);      
      posInfo = drive->getPositionInfo();

      ASSERT_EQ(0xABCDEF12U,posInfo.currentPosition);
      ASSERT_EQ(0x12EFCDABU,posInfo.oldestDirtyObject);
      ASSERT_EQ(0xABCDEFU,posInfo.dirtyObjectsCount);
      ASSERT_EQ(0x12EFCDABU,posInfo.dirtyBytesCount);
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(1);      
      drive->positionToLogicalObject(0xABCDEF0);
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(1);
      posInfo = drive->getPositionInfo();
      
      ASSERT_EQ(0xABCDEF0U,posInfo.currentPosition);
      ASSERT_EQ(0xABCDEF0U,posInfo.oldestDirtyObject);
      ASSERT_EQ(0x0U,posInfo.dirtyObjectsCount);
      ASSERT_EQ(0x0U,posInfo.dirtyBytesCount);
    }
  }
} 
TEST(castor_tape_drive_Drive, setDensityAndCompression) {
  /* Prepare the test harness */
  castor::tape::System::mockWrapper sysWrapper;
  sysWrapper.fake.setupSLC5();
  sysWrapper.delegateToFake();
  
  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(3);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(21);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(38);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(21);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(3);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(7);
  
  /* Test: detect devices, then open the device files */
  castor::tape::SCSI::DeviceVector dl(sysWrapper);
  for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
      i != dl.end(); i++) {
    if (castor::tape::SCSI::Types::tape == i->type) {
      std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive (
        castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));

      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(2);      
      drive->setDensityAndCompression();
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(2);      
      drive->setDensityAndCompression(true);
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(2);      
      drive->setDensityAndCompression(false);

      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(2);      
      drive->setDensityAndCompression(0x42,true);
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(2);      
      drive->setDensityAndCompression(0x46,false);
    }
  }
}

TEST(castor_tape_drive_Drive, setStDriverOptions) {
  /* Prepare the test harness */
  castor::tape::System::mockWrapper sysWrapper;
  sysWrapper.fake.setupSLC5();
  sysWrapper.delegateToFake();
  
  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(3);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(21);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(38);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(21);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(3);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(7);
  
  /* Test: detect devices, then open the device files */
  castor::tape::SCSI::DeviceVector dl(sysWrapper);
  for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin(); i != dl.end(); i++) {
    if (castor::tape::SCSI::Types::tape == i->type) {
      std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive (
        castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<struct mtop *>())).Times(1);
      drive->setSTBufferWrite(true);
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<struct mtop *>())).Times(1);
      drive->setSTBufferWrite(false);
    }
  }
}

TEST(castor_tape_drive_Drive, getDeviceInfo) {
  /* Prepare the test harness */
  castor::tape::System::mockWrapper sysWrapper;
  sysWrapper.fake.setupSLC5();
  sysWrapper.delegateToFake();
  
  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(3);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(21);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(38);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(21);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(3);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(7);
  
  /* Test: detect devices, then open the device files */
  castor::tape::SCSI::DeviceVector dl(sysWrapper);
  for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
      i != dl.end(); i++) {
    if (castor::tape::SCSI::Types::tape == i->type) {
      std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive (
        castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
      castor::tape::tapeserver::drive::deviceInfo devInfo;
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(2);      
      devInfo = drive->getDeviceInfo();

      ASSERT_EQ("STK     ",devInfo.vendor);
      ASSERT_EQ("T10000B         ",devInfo.product);
      ASSERT_EQ("0104",devInfo.productRevisionLevel );
      ASSERT_EQ("XYZZY_A2  ",devInfo.serialNumber );
    }
  }
}

TEST(castor_tape_drive_Drive, getCompressionAndClearCompressionStats) {
  /* Prepare the test harness */
  castor::tape::System::mockWrapper sysWrapper;
  sysWrapper.fake.setupSLC5();
  sysWrapper.delegateToFake();
  
  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(3);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(25);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(38);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(25);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(3);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(7);
  
  /* Test: detect devices, then open the device files */
  castor::tape::SCSI::DeviceVector dl(sysWrapper);
  for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
      i != dl.end(); i++) {
    if (castor::tape::SCSI::Types::tape == i->type) {
      castor::tape::tapeserver::drive::DriveGeneric *drive;
      castor::tape::tapeserver::drive::compressionStats comp;
      
        {
          drive = new castor::tape::tapeserver::drive::DriveT10000(*i, sysWrapper);

          EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t*>())).Times(1);
          comp = drive->getCompression();
          ASSERT_EQ(0xABCDEF1122334455ULL, comp.fromHost);
          ASSERT_EQ(0x2233445566778899ULL, comp.toHost);
          ASSERT_EQ(0x99AABBCCDDEEFF11ULL, comp.fromTape);
          ASSERT_EQ(0x1122334455667788ULL, comp.toTape);

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
          drive = new castor::tape::tapeserver::drive::DriveIBM3592(*i, sysWrapper);

          EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t*>())).Times(1);
          comp = drive->getCompression();
          ASSERT_EQ(0x4488CD1000ULL, comp.fromHost);
          ASSERT_EQ(0x15599DE2000ULL, comp.toHost);
          ASSERT_EQ(0x377BBFC4400ULL, comp.fromTape);
          ASSERT_EQ(0x266AAEF3000ULL, comp.toTape);

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
          drive = new castor::tape::tapeserver::drive::DriveLTO(*i, sysWrapper);

          EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t*>())).Times(1);
          comp = drive->getCompression();
          ASSERT_EQ(0x209DB2BE187D9ULL, comp.fromHost);
          ASSERT_EQ(0x105707026D088ULL, comp.toHost);
          ASSERT_EQ(0x928C54DFC8A11ULL, comp.fromTape);
          ASSERT_EQ(0xA2D3009B64262ULL, comp.toTape);

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

TEST(castor_tape_drive_Drive, getTapeAlerts) {

  /**
   * "Local function" allowing the test to be run twice (for SLC5 and then for 
   * SLC6).
   */
  struct {
    void operator() (castor::tape::System::mockWrapper & sysWrapper) {
      /* We expect the following calls: */
      EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(3));
      EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
      EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(3));
      EXPECT_CALL(sysWrapper, realpath(_, _)).Times(AtLeast(3));
      EXPECT_CALL(sysWrapper, open(_, _)).Times(AtLeast(21));
      EXPECT_CALL(sysWrapper, read(_, _, _)).Times(AtLeast(38));
      EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
      EXPECT_CALL(sysWrapper, ioctl(_, _, An<mtget*>())).Times(0);
      EXPECT_CALL(sysWrapper, close(_)).Times(AtLeast(21));
      EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(AtLeast(3));
      EXPECT_CALL(sysWrapper, stat(_, _)).Times(AtLeast(7));

      /* Test: detect devices, then open the device files */
      castor::tape::SCSI::DeviceVector dl(sysWrapper);
      for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
          i != dl.end(); i++) {
        if (castor::tape::SCSI::Types::tape == i->type) {
          std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive (
            castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
          EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t*>())).Times(1);
          std::vector<std::string> alerts = drive->getTapeAlerts(drive->getTapeAlertCodes());
          ASSERT_EQ(3U, alerts.size());
          ASSERT_FALSE(alerts.end() == 
              find(alerts.begin(), alerts.end(), 
              std::string("Unexpected tapeAlert code: 0x41")));
          ASSERT_FALSE(alerts.end() == find(alerts.begin(), alerts.end(), 
              std::string("Obsolete tapeAlert code: 0x28")));
          ASSERT_FALSE(alerts.end() == find(alerts.begin(), alerts.end(), 
              std::string("Forced eject")));
        }
      }
    }
  } test_functor;


  /* Prepare the test harness */
  castor::tape::System::mockWrapper sysWrapperSLC5;
  castor::tape::System::mockWrapper sysWrapperSLC6;
  sysWrapperSLC5.fake.setupSLC5();
  sysWrapperSLC6.fake.setupSLC6();
  sysWrapperSLC5.delegateToFake();
  sysWrapperSLC6.delegateToFake();
  test_functor(sysWrapperSLC5);
  test_functor(sysWrapperSLC6);
}

}

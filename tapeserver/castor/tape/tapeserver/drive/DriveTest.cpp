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
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(4));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(4));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(6);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(42);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(76);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(42);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(6);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(14);

  
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
      std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface>drive(
        castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
      std::string expected_classid;
      if (dynamic_cast<castor::tape::tapeserver::drive::DriveIBM3592 *>(drive.get())) {
        expected_classid = std::string(typeid(castor::tape::tapeserver::drive::DriveIBM3592).name());
      }
      else if (dynamic_cast<castor::tape::tapeserver::drive::DriveT10000 *>(drive.get())) {
        expected_classid = std::string(typeid(castor::tape::tapeserver::drive::DriveT10000).name());
      }
      else {/* Fill in other vendors in the future here. */}
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
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(4));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(4));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(6);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(42);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(76);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(42);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(6);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(14);
  
  /* Test: detect devices, then open the device files */
  castor::tape::SCSI::DeviceVector dl(sysWrapper);
  for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
      i != dl.end(); i++) {
    if (castor::tape::SCSI::Types::tape == i->type) {
      std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive (
        castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
      castor::tape::tapeserver::drive::positionInfo posInfo;
      castor::tape::tapeserver::drive::physicalPositionInfo physicalPosInfo;

      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(1);      
      posInfo = drive->getPositionInfo();

      ASSERT_EQ(0xABCDEF12U,posInfo.currentPosition);
      ASSERT_EQ(0x12EFCDABU,posInfo.oldestDirtyObject);
      ASSERT_EQ(0xABCDEFU,posInfo.dirtyObjectsCount);
      ASSERT_EQ(0x12EFCDABU,posInfo.dirtyBytesCount);

      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(1);      
      physicalPosInfo = drive->getPhysicalPositionInfo();
      ASSERT_EQ(0x0, physicalPosInfo.wrap);
      ASSERT_EQ(0x0, physicalPosInfo.lpos);
      ASSERT_EQ(castor::tape::tapeserver::drive::physicalPositionInfo::FORWARD, physicalPosInfo.direction());

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
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(4));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(4));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(6);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(42);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(76);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(42);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(6);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(14);
  
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
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(4));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(4));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(6);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(42);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(76);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(42);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(6);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(14);
  
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
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(4));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(4));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(6);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(42);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(76);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(42);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(6);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(14);
  
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

      if (dynamic_cast<castor::tape::tapeserver::drive::DriveIBM3592 *>(drive.get())) {
        ASSERT_EQ("IBM     ",devInfo.vendor);
        ASSERT_EQ("03592E08        ",devInfo.product);
        ASSERT_EQ("460E",devInfo.productRevisionLevel );
        ASSERT_EQ("XYZZY_A2  ",devInfo.serialNumber );
      }
      else if (dynamic_cast<castor::tape::tapeserver::drive::DriveIBM3592 *>(drive.get())) {
        ASSERT_EQ("STK     ",devInfo.vendor);
        ASSERT_EQ("T10000B         ",devInfo.product);
        ASSERT_EQ("0104",devInfo.productRevisionLevel );
        ASSERT_EQ("XYZZY_A2  ",devInfo.serialNumber );
      }
      else { /* Fill in other vendors in the future here. */ }
    }
  }
}

TEST(castor_tape_drive_Drive, getCompressionAndClearCompressionStats) {
  /* Prepare the test harness */
  castor::tape::System::mockWrapper sysWrapper;
  sysWrapper.fake.setupSLC5();
  sysWrapper.delegateToFake();
  
  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(4));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(6);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(44);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(76);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(44);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(6);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(14);
  
  /* Test: detect devices, then open the device files */
  castor::tape::SCSI::DeviceVector dl(sysWrapper);
  for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
    i != dl.end(); i++) {
    /*
     * The second condition relates to the fact that LTO drives and IBM 3592 have the same
     * logSense page code for different metrics.
     * The current test is written in such a way that does not really use vfs files. So, there
     * is no need take into consideration the IBM tapes at all.
     */
    if (castor::tape::SCSI::Types::tape == i->type && i->product != "03592E08") {
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

TEST(castor_tape_drive_Drive, getLBPInfo) {
  /* Prepare the test harness */
  castor::tape::System::mockWrapper sysWrapper;
  sysWrapper.fake.setupSLC5();
  sysWrapper.delegateToFake();
  
  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(6);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(42);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(76);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(42);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(6);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(14);
  
  /* Test: detect devices, then open the device files */
  castor::tape::SCSI::DeviceVector dl(sysWrapper);
  for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
      i != dl.end(); i++) {
    if (castor::tape::SCSI::Types::tape == i->type) {
      std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive (
      castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
      castor::tape::tapeserver::drive::LBPInfo LBPdata;
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(1);         
      LBPdata = drive->getLBPInfo();

      ASSERT_EQ(0xFA,LBPdata.method);
      ASSERT_EQ(0x3C,LBPdata.methodLength);
      ASSERT_TRUE(LBPdata.enableLBPforRead);
      ASSERT_FALSE(LBPdata.enableLBPforWrite);
    }
  }
}

TEST(castor_tape_drive_Drive, setLogicalBlockProtection) {
  /* Prepare the test harness */
  castor::tape::System::mockWrapper sysWrapper;
  sysWrapper.fake.setupSLC5();
  sysWrapper.delegateToFake();
  
  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(6);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(42);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(76);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(42);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(6);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(14);
  
  /* Test: detect devices, then open the device files */
  castor::tape::SCSI::DeviceVector dl(sysWrapper);
  for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
      i != dl.end(); i++) {
    if (castor::tape::SCSI::Types::tape == i->type) {
      std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive (
      castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
      castor::tape::tapeserver::drive::LBPInfo LBPdata;
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(3);   
      drive->setLogicalBlockProtection(
        castor::tape::SCSI::logicBlockProtectionMethod::CRC32C,
        castor::tape::SCSI::logicBlockProtectionMethod::CRC32CLength, 
        true, true);      
      LBPdata = drive->getLBPInfo();

      ASSERT_EQ(castor::tape::SCSI::logicBlockProtectionMethod::CRC32C,
        LBPdata.method);
      ASSERT_EQ(castor::tape::SCSI::logicBlockProtectionMethod::CRC32CLength,
        LBPdata.methodLength);
      ASSERT_TRUE(LBPdata.enableLBPforRead);
      ASSERT_TRUE(LBPdata.enableLBPforWrite);
    }
  }
}

TEST(castor_tape_drive_Drive, disableLogicalBlockProtection) {
  /* Prepare the test harness */
  castor::tape::System::mockWrapper sysWrapper;
  sysWrapper.fake.setupSLC5();
  sysWrapper.delegateToFake();
  
  /* We expect the following calls: */
  EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(30));
  EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(3));
  EXPECT_CALL(sysWrapper, realpath(_, _)).Times(6);
  EXPECT_CALL(sysWrapper, open(_, _)).Times(42);
  EXPECT_CALL(sysWrapper, read(_, _, _)).Times(76);
  EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
  EXPECT_CALL(sysWrapper, ioctl(_,_,An<mtget*>())).Times(0);
  EXPECT_CALL(sysWrapper, close(_)).Times(42);
  EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(6);
  EXPECT_CALL(sysWrapper, stat(_,_)).Times(14);
  
  /* Test: detect devices, then open the device files */
  castor::tape::SCSI::DeviceVector dl(sysWrapper);
  for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
      i != dl.end(); i++) {
    if (castor::tape::SCSI::Types::tape == i->type) {
      std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive (
      castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
      castor::tape::tapeserver::drive::LBPInfo LBPdata;
      
      EXPECT_CALL(sysWrapper, ioctl(_,_,An<sg_io_hdr_t*>())).Times(5);   
      drive->setLogicalBlockProtection(
        castor::tape::SCSI::logicBlockProtectionMethod::CRC32C,
        castor::tape::SCSI::logicBlockProtectionMethod::CRC32CLength, 
        true, true);   
      drive->disableLogicalBlockProtection();
      LBPdata = drive->getLBPInfo();
      
      ASSERT_EQ(castor::tape::SCSI::logicBlockProtectionMethod::DoNotUse,
        LBPdata.method);
      ASSERT_EQ(0, LBPdata.methodLength);
      ASSERT_FALSE(LBPdata.enableLBPforRead);
      ASSERT_FALSE(LBPdata.enableLBPforWrite);    
    }
  }
}

TEST(castor_tape_drive_Drive, getReadErrors) {
  struct {
    void operator()(castor::tape::System::mockWrapper &sysWrapper) {
      EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(5));
      EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(68));
      EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(5));
      EXPECT_CALL(sysWrapper, realpath(_, _)).Times(AtLeast(6));
      EXPECT_CALL(sysWrapper, open(_, _)).Times(AtLeast(42));
      EXPECT_CALL(sysWrapper, read(_, _, _)).Times(AtLeast(76));
      EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
      EXPECT_CALL(sysWrapper, ioctl(_, _, An<mtget*>())).Times(0);
      EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t *>())).Times(4);
      EXPECT_CALL(sysWrapper, close(_)).Times(AtLeast(42));
      EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(AtLeast(6));
      EXPECT_CALL(sysWrapper, stat(_, _)).Times(AtLeast(14));

      /* Test: detect devices, then open the device files */
      castor::tape::SCSI::DeviceVector dl(sysWrapper);
      for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
           i != dl.end(); i++) {
        if (castor::tape::SCSI::Types::tape == i->type) {
          std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(
            castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
          std::map<std::string,uint64_t> readErrorsStats =
            drive->getTapeReadErrors();
          ASSERT_EQ(2, readErrorsStats["mountTotalCorrectedReadErrors"]);
          ASSERT_EQ(2048, readErrorsStats["mountTotalReadBytesProcessed"]);
          ASSERT_EQ(1, readErrorsStats["mountTotalUncorrectedReadErrors"]);
        }
      }
    }
  } test_functor;

  castor::tape::System::mockWrapper sysWrapperSLC5;
  castor::tape::System::mockWrapper sysWrapperSLC6;
  sysWrapperSLC5.fake.setupSLC5();
  sysWrapperSLC6.fake.setupSLC6();
  sysWrapperSLC5.delegateToFake();
  sysWrapperSLC6.delegateToFake();
  test_functor(sysWrapperSLC5);
  test_functor(sysWrapperSLC6);
}

TEST(castor_tape_drive_Drive, getWriteErrors) {
  struct {
    void operator()(castor::tape::System::mockWrapper &sysWrapper) {
      EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(5));
      EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(68));
      EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(5));
      EXPECT_CALL(sysWrapper, realpath(_, _)).Times(AtLeast(6));
      EXPECT_CALL(sysWrapper, open(_, _)).Times(AtLeast(42));
      EXPECT_CALL(sysWrapper, read(_, _, _)).Times(AtLeast(76));
      EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
      EXPECT_CALL(sysWrapper, ioctl(_, _, An<mtget*>())).Times(0);
      EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t *>())).Times(4);
      EXPECT_CALL(sysWrapper, close(_)).Times(AtLeast(42));
      EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(AtLeast(6));
      EXPECT_CALL(sysWrapper, stat(_, _)).Times(AtLeast(14));

      /* Test: detect devices, then open the device files */
      castor::tape::SCSI::DeviceVector dl(sysWrapper);
      for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
           i != dl.end(); i++) {
        if (castor::tape::SCSI::Types::tape == i->type) {
          std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(
            castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
          std::map<std::string,uint64_t> writeErrorsStats =
            drive->getTapeWriteErrors();
          ASSERT_EQ(2, writeErrorsStats["mountTotalCorrectedWriteErrors"]);
          ASSERT_EQ(2048, writeErrorsStats["mountTotalWriteBytesProcessed"]);
          ASSERT_EQ(1, writeErrorsStats["mountTotalUncorrectedWriteErrors"]);
        }
      }
    }
  } test_functor;

  castor::tape::System::mockWrapper sysWrapperSLC5;
  castor::tape::System::mockWrapper sysWrapperSLC6;
  sysWrapperSLC5.fake.setupSLC5();
  sysWrapperSLC6.fake.setupSLC6();
  sysWrapperSLC5.delegateToFake();
  sysWrapperSLC6.delegateToFake();
  test_functor(sysWrapperSLC5);
  test_functor(sysWrapperSLC6);
}

TEST(castor_tape_drive_Drive, getNonMediumErrors) {
  struct {
    void operator()(castor::tape::System::mockWrapper &sysWrapper) {
      EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(5));
      EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(68));
      EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(5));
      EXPECT_CALL(sysWrapper, realpath(_, _)).Times(AtLeast(6));
      EXPECT_CALL(sysWrapper, open(_, _)).Times(AtLeast(42));
      EXPECT_CALL(sysWrapper, read(_, _, _)).Times(AtLeast(76));
      EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
      EXPECT_CALL(sysWrapper, ioctl(_, _, An<mtget*>())).Times(0);
      EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t *>())).Times(4);
      EXPECT_CALL(sysWrapper, close(_)).Times(AtLeast(42));
      EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(AtLeast(6));
      EXPECT_CALL(sysWrapper, stat(_, _)).Times(AtLeast(14));

      /* Test: detect devices, then open the device files */
      castor::tape::SCSI::DeviceVector dl(sysWrapper);
      for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
           i != dl.end(); i++) {
        if (castor::tape::SCSI::Types::tape == i->type) {
          std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(
            castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
          std::map<std::string,uint32_t> nonMediumErrorsStats =
            drive->getTapeNonMediumErrors();
          ASSERT_EQ(3, nonMediumErrorsStats["mountTotalNonMediumErrorCounts"]);
        }
      }
    }
  } test_functor;

  castor::tape::System::mockWrapper sysWrapperSLC5;
  castor::tape::System::mockWrapper sysWrapperSLC6;
  sysWrapperSLC5.fake.setupSLC5();
  sysWrapperSLC6.fake.setupSLC6();
  sysWrapperSLC5.delegateToFake();
  sysWrapperSLC6.delegateToFake();
  test_functor(sysWrapperSLC5);
  test_functor(sysWrapperSLC6);
}

TEST(castor_tape_drive_Drive, getVolumeStats) {
  struct {
    void operator()(castor::tape::System::mockWrapper &sysWrapper) {
      EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(5));
      EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(68));
      EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(5));
      EXPECT_CALL(sysWrapper, realpath(_, _)).Times(AtLeast(6));
      EXPECT_CALL(sysWrapper, open(_, _)).Times(AtLeast(42));
      EXPECT_CALL(sysWrapper, read(_, _, _)).Times(AtLeast(76));
      EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
      EXPECT_CALL(sysWrapper, ioctl(_, _, An<mtget*>())).Times(0);
      EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t *>())).Times(2);
      EXPECT_CALL(sysWrapper, close(_)).Times(AtLeast(42));
      EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(AtLeast(6));
      EXPECT_CALL(sysWrapper, stat(_, _)).Times(AtLeast(14));
  
  
      /* Test: detect devices, then open the device files */
      castor::tape::SCSI::DeviceVector dl(sysWrapper);
      for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
           i != dl.end(); i++) {
        if (castor::tape::SCSI::Types::tape == i->type) {
          std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(
            castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
          if (castor::tape::tapeserver::drive::DriveIBM3592* ibm_drive =
              dynamic_cast<castor::tape::tapeserver::drive::DriveIBM3592*>(drive.get())) {
            std::map<std::string,uint32_t> volumeStats =
              ibm_drive->getVolumeStats();
            ASSERT_EQ(true, volumeStats["validity"]);
            ASSERT_EQ(18886754, volumeStats["lifetimeVolumeMounts"]);
            ASSERT_EQ(16843223, volumeStats["lifetimeVolumeRecoveredReadErrors"]);
            ASSERT_EQ(514, volumeStats["lifetimeVolumeUnrecoveredReadErrors"]);
            ASSERT_EQ(16909057, volumeStats["lifetimeVolumeRecoveredWriteErrors"]);
            ASSERT_EQ(1, volumeStats["lifetimeVolumeUnrecoveredWriteErrors"]);
            ASSERT_EQ(20140815, volumeStats["volumeManufacturingDate"]);
            ASSERT_EQ(33752868, volumeStats["lifetimeBOTPasses"]);
            ASSERT_EQ(16908566, volumeStats["lifetimeMOTPasses"]);
          }
        }
      }
    }
  } test_functor;

  castor::tape::System::mockWrapper sysWrapperSLC5;
  castor::tape::System::mockWrapper sysWrapperSLC6;
  sysWrapperSLC5.fake.setupSLC5();
  sysWrapperSLC6.fake.setupSLC6();
  sysWrapperSLC5.delegateToFake();
  sysWrapperSLC6.delegateToFake();
  test_functor(sysWrapperSLC5);
  test_functor(sysWrapperSLC6);
}
  
TEST(castor_tape_drive_Drive, getQualityStats) {
  struct {
    void operator()(castor::tape::System::mockWrapper &sysWrapper) {
      EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(5));
      EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(68));
      EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(5));
      EXPECT_CALL(sysWrapper, realpath(_, _)).Times(AtLeast(6));
      EXPECT_CALL(sysWrapper, open(_, _)).Times(AtLeast(42));
      EXPECT_CALL(sysWrapper, read(_, _, _)).Times(AtLeast(76));
      EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
      EXPECT_CALL(sysWrapper, ioctl(_, _, An<mtget*>())).Times(0);
      EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t *>())).Times(10);
      EXPECT_CALL(sysWrapper, close(_)).Times(AtLeast(42));
      EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(AtLeast(6));
      EXPECT_CALL(sysWrapper, stat(_, _)).Times(AtLeast(14));
  
      /* Test: detect devices, then open the device files */
      castor::tape::SCSI::DeviceVector dl(sysWrapper);
      for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
           i != dl.end(); i++) {
        if (castor::tape::SCSI::Types::tape == i->type) {
          std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(
            castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
          std::map<std::string,float> qualityStats = drive->getQualityStats();
          /*
           * Assertions for common metrics are moved inside the special cases
           * due to different scales and representation of the same quality metrics.
           */
          if (dynamic_cast<castor::tape::tapeserver::drive::DriveIBM3592 *>(drive.get())) {
  
            ASSERT_EQ(68, (int)qualityStats["lifetimeDriveEfficiencyPrct"]);
            ASSERT_EQ(68, (int)qualityStats["lifetimeInterfaceEfficiency0Prct"]);
            ASSERT_EQ(68, (int)qualityStats["lifetimeInterfaceEfficiency1Prct"]);
            ASSERT_EQ(68, (int)qualityStats["lifetimeLibraryEfficiencyPrct"]);
            ASSERT_EQ(96, (int)qualityStats["lifetimeReadEfficiencyPrct"]);
            ASSERT_EQ(98, (int)qualityStats["lifetimeWriteEfficiencyPrct"]);
            ASSERT_EQ(68, (int)qualityStats["lifetimeMediumEfficiencyPrct"]);
  
            ASSERT_EQ(68, (int)qualityStats["mountDriveEfficiencyPrct"]);
            ASSERT_EQ(68, (int)qualityStats["mountInterfaceEfficiency0Prct"]);
            ASSERT_EQ(68, (int)qualityStats["mountInterfaceEfficiency1Prct"]);
            ASSERT_EQ(68, (int)qualityStats["mountLibraryEfficiencyPrct"]);
            ASSERT_EQ(96, (int)qualityStats["mountReadEfficiencyPrct"]);
            ASSERT_EQ(98, (int)qualityStats["mountWriteEfficiencyPrct"]);
            ASSERT_EQ(68, (int)qualityStats["mountMediumEfficiencyPrct"]);
          }
          else if (dynamic_cast<castor::tape::tapeserver::drive::DriveT10000 *>(drive.get())) {
  
            ASSERT_EQ(87, (int)qualityStats["mountReadEfficiencyPrct"]);
            ASSERT_EQ(100, (int)qualityStats["mountWriteEfficiencyPrct"]);
            ASSERT_EQ(99, (int)qualityStats["lifetimeMediumEfficiencyPrct"]);
            ASSERT_EQ(100, (int)qualityStats["mountReadBackCheckQualityIndexPrct"]);
          }
          else {
            // write test code in case quality statistics are populated of other vendors
          }
        }
      }
    }
  } test_functor;

  castor::tape::System::mockWrapper sysWrapperSLC5;
  castor::tape::System::mockWrapper sysWrapperSLC6;
  sysWrapperSLC5.fake.setupSLC5();
  sysWrapperSLC6.fake.setupSLC6();
  sysWrapperSLC5.delegateToFake();
  sysWrapperSLC6.delegateToFake();
  test_functor(sysWrapperSLC5);
  test_functor(sysWrapperSLC6);
}

TEST(castor_tape_drive_Drive, getDriveStats) {
  struct {
    void operator()(castor::tape::System::mockWrapper &sysWrapper) {
      EXPECT_CALL(sysWrapper, opendir(_)).Times(AtLeast(5));
      EXPECT_CALL(sysWrapper, readdir(_)).Times(AtLeast(68));
      EXPECT_CALL(sysWrapper, closedir(_)).Times(AtLeast(5));
      EXPECT_CALL(sysWrapper, realpath(_, _)).Times(AtLeast(6));
      EXPECT_CALL(sysWrapper, open(_, _)).Times(AtLeast(42));
      EXPECT_CALL(sysWrapper, read(_, _, _)).Times(AtLeast(76));
      EXPECT_CALL(sysWrapper, write(_, _, _)).Times(0);
      EXPECT_CALL(sysWrapper, ioctl(_, _, An<mtget*>())).Times(0);
      EXPECT_CALL(sysWrapper, ioctl(_, _, An<sg_io_hdr_t *>())).Times(8);
      EXPECT_CALL(sysWrapper, close(_)).Times(AtLeast(42));
      EXPECT_CALL(sysWrapper, readlink(_, _, _)).Times(AtLeast(6));
      EXPECT_CALL(sysWrapper, stat(_, _)).Times(AtLeast(14));
  
      /* Test: detect devices, then open the device files */
      castor::tape::SCSI::DeviceVector dl(sysWrapper);
      for (std::vector<castor::tape::SCSI::DeviceInfo>::iterator i = dl.begin();
           i != dl.end(); i++) {
        if (castor::tape::SCSI::Types::tape == i->type) {
          std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(
            castor::tape::tapeserver::drive::createDrive(*i, sysWrapper));
          std::map<std::string,uint32_t> driveStats = drive->getDriveStats();
  
          /*
           * Assertions for common metrics are moved inside the special cases
           * due to different scales. Moreover, ibm gathers the statistics from multiple source
           * So, this distinction aims to deduplicate as far as possible the tests of different
           * drives in order for future changes to be easier to test.
           */
  
          if (dynamic_cast<castor::tape::tapeserver::drive::DriveIBM3592 *>(drive.get())) {
            ASSERT_EQ(687, driveStats["mountServoTemps"]);
            ASSERT_EQ(771, driveStats["mountServoTransients"]);
            ASSERT_EQ(753, driveStats["mountTemps"]);
            ASSERT_EQ(497, driveStats["mountReadTransients"]);
            ASSERT_EQ(258, driveStats["mountWriteTransients"]);
            ASSERT_EQ(273, driveStats["mountTotalReadRetries"]);
            ASSERT_EQ(306, driveStats["mountTotalWriteRetries"]);
          }
          else if (dynamic_cast<castor::tape::tapeserver::drive::DriveT10000 *>(drive.get())) {
            ASSERT_EQ(531, driveStats["mountServoTemps"]);
            ASSERT_EQ(137743, driveStats["mountServoTransients"]);
            ASSERT_EQ(16777217, driveStats["mountTemps"]);
            ASSERT_EQ(65794, driveStats["mountReadTransients"]);
            ASSERT_EQ(16843266, driveStats["mountWriteTransients"]);
            ASSERT_EQ(16909060, driveStats["mountTotalReadRetries"]);
            ASSERT_EQ(65535, driveStats["mountTotalWriteRetries"]);
          }
        }
      }
    }
  } test_functor;
  castor::tape::System::mockWrapper sysWrapperSLC5;
  castor::tape::System::mockWrapper sysWrapperSLC6;
  sysWrapperSLC5.fake.setupSLC5();
  sysWrapperSLC6.fake.setupSLC6();
  sysWrapperSLC5.delegateToFake();
  sysWrapperSLC6.delegateToFake();
  test_functor(sysWrapperSLC5);
  test_functor(sysWrapperSLC6);
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
          std::vector<uint16_t> tapeAlertCodes = drive->getTapeAlertCodes();
          std::vector<std::string> alerts = drive->getTapeAlerts(tapeAlertCodes);
          ASSERT_EQ(4U, alerts.size());
          ASSERT_FALSE(alerts.end() == 
              find(alerts.begin(), alerts.end(), 
              std::string("Unexpected tapeAlert code: 0x41")));
          ASSERT_FALSE(alerts.end() == find(alerts.begin(), alerts.end(), 
              std::string("Obsolete tapeAlert code: 0x28")));
          ASSERT_FALSE(alerts.end() == find(alerts.begin(), alerts.end(), 
              std::string("Forced eject")));
          ASSERT_FALSE(alerts.end() == find(alerts.begin(), alerts.end(),
              std::string("Lost statistics")));
          ASSERT_TRUE(drive->tapeAlertsCriticalForWrite(tapeAlertCodes));

          for (std::vector<uint16_t>::const_iterator code =
            tapeAlertCodes.begin(); code!= tapeAlertCodes.end(); code++) {
              if(castor::tape::SCSI::isTapeAlertCriticalForWrite(*code)) {
                ASSERT_NE(0x32U, *code);
              } else {
                ASSERT_EQ(0x32U, *code);
              }
          }
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

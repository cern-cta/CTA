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

#include <string>

#include "castor/tape/tapeserver/daemon/CleanerSession.hpp"
#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/Exceptions.hpp"
#include "castor/tape/tapeserver/file/FileReader.hpp"
#include "castor/tape/tapeserver/file/FileReaderFactory.hpp"
#include "castor/tape/tapeserver/file/ReadSession.hpp"
#include "castor/tape/tapeserver/file/ReadSessionFactory.hpp"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "catalogue/dummy/DummyTapeCatalogue.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/log/StringLogger.hpp"
#include "common/processCap/ProcessCapDummy.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#include "scheduler/RetrieveJob.hpp"

namespace {
std::string g_device_name;
std::string g_device_path;
}

class OSMTapeCatalogue: public cta::catalogue::DummyTapeCatalogue {
public:
  using LabelFormat = cta::common::dataStructures::Label::Format;
  OSMTapeCatalogue() = default;

  LabelFormat getTapeLabelFormat(const std::string& vid) const override {
    return LabelFormat::OSM;
  }
};

class OSMCatalogue: public cta::catalogue::DummyCatalogue {
public:
  OSMCatalogue() : DummyCatalogue() {
    DummyCatalogue::m_tape = std::make_unique<OSMTapeCatalogue>();
  }
};

class BasicRetrieveJob: public cta::RetrieveJob {
public:
  BasicRetrieveJob() : cta::RetrieveJob(nullptr,
  cta::common::dataStructures::RetrieveRequest(),
  cta::common::dataStructures::ArchiveFile(), 1,
  cta::PositioningMethod::ByBlock) {}
};

class OsmReaderTest : public ::testing::Test {
protected:
  OsmReaderTest() : m_sWrapper(), m_dev(), m_catalogue(std::make_unique<OSMCatalogue>()) {
    cta::OStoreDBFactory<cta::objectstore::BackendVFS> factory;
  }

  void SetUp() override {
    m_vid = "L08033";
    m_nstDev = g_device_path;
    m_devName = g_device_name;
  }

  void TearDown() override {
    m_db.reset();
    m_catalogue.reset();
  }

  void createDrive() {
    // Create drive object and open tape device
    m_dev.product = "MHVTL";
    m_dev.nst_dev = m_nstDev;
    m_drive.reset(castor::tape::tapeserver::drive::createDrive(m_dev, m_sWrapper));

    try {
      /**
        * Gets generic device info for the drive object.
        */
      castor::tape::tapeserver::drive::deviceInfo devInfo;
      devInfo = m_drive->getDeviceInfo();
      auto removeWhiteSpaces = [](std::string *str) -> std::string {
        str->erase(std::remove_if(str->begin(), str->end(), [](uint8_t x){return std::isspace(x);}), str->end());
        return *str;
      };
      ASSERT_EQ("STK", removeWhiteSpaces(&devInfo.vendor));
      ASSERT_EQ("MHVTL", removeWhiteSpaces(&devInfo.product));
      ASSERT_EQ("0105", devInfo.productRevisionLevel);
      ASSERT_EQ(m_devName, removeWhiteSpaces(&devInfo.serialNumber));
    } catch (cta::exception::Exception & ex) {
      FAIL() << "The drive couldn't be created. " << ex.getMessageValue();
    }

    try {
      /**
        * Checks if the drive ready to use the tape installed loaded into it.
        */
      m_drive->waitUntilReady(5);
    } catch(cta::exception::Exception &ex) {
      FAIL() << "The drive is not ready to use. " << ex.getMessageValue();
    }

    m_drive->rewind();
  }

  castor::tape::System::realWrapper m_sWrapper;
  castor::tape::SCSI::DeviceInfo m_dev;
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> m_drive;
  std::unique_ptr<cta::SchedulerDatabase> m_db;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::string m_vid;
  std::string m_nstDev;
  std::string m_devName;
};

TEST_F(OsmReaderTest, ReadOsmTape) {
  createDrive();
  try {
    castor::tape::tapeserver::daemon::VolumeInfo m_volInfo;
    m_volInfo.vid = m_vid;
    m_volInfo.nbFiles = 1;
    m_volInfo.labelFormat = static_cast<OSMTapeCatalogue*>(m_catalogue->Tape().get())->getTapeLabelFormat(m_volInfo.vid);
    m_volInfo.mountType = cta::common::dataStructures::MountType::Retrieve;

    // Now read a random file
    // Create Read Session OSM
    auto readSession = castor::tape::tapeFile::ReadSessionFactory::create(*m_drive, m_volInfo, false);
    BasicRetrieveJob fileToRecall;
    fileToRecall.selectedCopyNb = 0;
    fileToRecall.archiveFile.tapeFiles.push_back(cta::common::dataStructures::TapeFile());
    fileToRecall.selectedTapeFile().blockId = 1;  // here should be the block ID of HDR1
    fileToRecall.selectedTapeFile().fSeq = 1;
    fileToRecall.retrieveRequest.archiveFileID = 1;
    fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;

    // Create Read File OSM
    auto reader = castor::tape::tapeFile::FileReaderFactory::create(readSession, fileToRecall);
    size_t bs = reader->getBlockSize();
    char *data = new char[bs+1];
    size_t j = 0;
    while (j < 100) {
        reader->readNextDataBlock(data, bs);
        j++;
    }
  } catch(cta::exception::Exception &ex) {
    FAIL() << "Problem to read the OSM Tape. " << ex.getMessageValue();
  }
}

TEST_F(OsmReaderTest, CleanDrive) {
  cta::log::DummyLogger dummylogger("dummy", "unitTest");
  cta::log::StringLogger strlogger("string", "unitTest", cta::log::DEBUG);
  cta::log::StdoutLogger stdoutlogger("stdout", "unitTest");
  cta::server::ProcessCapDummy capUtils;
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummylogger);
  cta::tape::daemon::TpconfigLine driveConfig(m_devName, "TestLogicalLibrary", m_nstDev, "dummy");

  auto scheduler = std::make_unique<cta::Scheduler>(*m_catalogue, *m_db, 5, 2 * 1000 * 1000);

  castor::tape::tapeserver::daemon::CleanerSession cleanerSession(
    capUtils,
    mc,
    strlogger,
    driveConfig,
    m_sWrapper,
    m_vid,
    false,
    0,
    "",
    *m_catalogue,
    *scheduler);

    cleanerSession.execute();

  const auto logToCheck = strlogger.getLog();
  ASSERT_NE(std::string::npos, logToCheck.find("Cleaner successfully detected tape contains data"));
  ASSERT_NE(std::string::npos, logToCheck.find("Cleaner detected volume label contains expected VSN"));
  ASSERT_NE(std::string::npos, logToCheck.find("Cleaner unloaded tape"));
  ASSERT_NE(std::string::npos, logToCheck.find("Cleaner dismounted tape"));
}

int main(int argc, char **argv) {
  g_device_name = argv[1];
  g_device_path = argv[2];
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
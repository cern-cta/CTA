/*
 * SPDX-FileCopyrightText: 2024 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/Exceptions.hpp"
#include "castor/tape/tapeserver/file/FileReader.hpp"
#include "castor/tape/tapeserver/file/FileReaderFactory.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "castor/tape/tapeserver/file/ReadSession.hpp"
#include "castor/tape/tapeserver/file/ReadSessionFactory.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "common/dataStructures/LabelFormat.hpp"
#include "scheduler/RetrieveJob.hpp"

#include <algorithm>
#include <cctype>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

namespace {
std::string g_device_name;
std::string g_device_path;
}  // namespace

class EnstoreRetrieveJob : public cta::RetrieveJob {
public:
  EnstoreRetrieveJob()
      : cta::RetrieveJob(nullptr,
                         cta::common::dataStructures::RetrieveRequest(),
                         cta::common::dataStructures::ArchiveFile(),
                         1,
                         cta::PositioningMethod::ByFSeq) {}
};

class EnstoreReaderTest : public ::testing::Test {
protected:
  EnstoreReaderTest() : m_sWrapper(), m_dev() {}

  void SetUp() override {
    m_nstDev = g_device_path;
    m_devName = g_device_name;
  }

  void createDrive() {
    m_dev.product = "MHVTL";
    m_dev.nst_dev = m_nstDev;
    m_drive.reset(castor::tape::tapeserver::drive::createDrive(m_dev, m_sWrapper));

    try {
      castor::tape::tapeserver::drive::deviceInfo devInfo;
      devInfo = m_drive->getDeviceInfo();
      auto removeWhiteSpaces = [](std::string* str) -> std::string {
        str->erase(std::remove_if(str->begin(), str->end(), [](uint8_t x) { return std::isspace(x); }), str->end());
        return *str;
      };
      ASSERT_EQ("STK", removeWhiteSpaces(&devInfo.vendor));
      ASSERT_EQ("MHVTL", removeWhiteSpaces(&devInfo.product));
      ASSERT_EQ("0107", devInfo.productRevisionLevel);
      ASSERT_EQ("S001L01D01", removeWhiteSpaces(&devInfo.serialNumber));
    } catch (cta::exception::Exception& ex) {
      FAIL() << "The drive couldn't be created. " << ex.getMessageValue();
    }

    try {
      m_drive->waitUntilReady(5);
    } catch (cta::exception::Exception& ex) {
      FAIL() << "The drive is not ready to use. " << ex.getMessageValue();
    }

    m_drive->rewind();
  }

  std::string readTapeVid(const cta::common::dataStructures::Label::Format labelFormat) {
    m_drive->rewind();
    return castor::tape::tapeFile::HeaderChecker::checkVolumeLabel(*m_drive, labelFormat);
  }

  castor::tape::System::realWrapper m_sWrapper;
  castor::tape::SCSI::DeviceInfo m_dev;
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> m_drive;
  std::string m_nstDev;
  std::string m_devName;
};

TEST_F(EnstoreReaderTest, ReadEnstoreTape) {
  createDrive();

  using LabelFormat = cta::common::dataStructures::Label::Format;
  const auto labelFormat = LabelFormat::Enstore;
  const auto vid = readTapeVid(labelFormat);

  castor::tape::tapeserver::daemon::VolumeInfo m_volInfo;
  m_volInfo.vid = vid;
  m_volInfo.nbFiles = 1;
  m_volInfo.labelFormat = labelFormat;
  m_volInfo.mountType = cta::common::dataStructures::MountType::Retrieve;

  auto readSession = castor::tape::tapeFile::ReadSessionFactory::create(*m_drive, m_volInfo, false);
  EnstoreRetrieveJob fileToRecall;
  fileToRecall.selectedCopyNb = 0;
  fileToRecall.archiveFile.tapeFiles.push_back(cta::common::dataStructures::TapeFile());
  fileToRecall.selectedTapeFile().fSeq = 1;
  fileToRecall.selectedTapeFile().blockId = 1;
  fileToRecall.retrieveRequest.archiveFileID = 1;
  fileToRecall.positioningMethod = cta::PositioningMethod::ByFSeq;

  auto reader = castor::tape::tapeFile::FileReaderFactory::create(*readSession, fileToRecall);
  const size_t blockSize = reader->getBlockSize();
  std::vector<char> data(blockSize);
  size_t blocksRead = 0;

  try {
    while (true) {
      reader->readNextDataBlock(data.data(), blockSize);
      ++blocksRead;
    }
  } catch (const castor::tape::tapeFile::EndOfFile&) {}

  ASSERT_GT(blocksRead, 0u);
}

int main(int argc, char** argv) {
  g_device_name = argv[1];
  g_device_path = argv[2];
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

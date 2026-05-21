/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "CpioFileHeaderStructure.hpp"
#include "FileReader.hpp"
#include "FileReaderFactory.hpp"
#include "ReadSession.hpp"
#include "ReadSessionFactory.hpp"
#include "Structures.hpp"
#include "common/dataStructures/LabelFormat.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "taped/drive/FakeDrive.hpp"
#include "taped/session/VolumeInfo.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <gtest/gtest.h>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace unitTests {
namespace {

constexpr size_t ENSTORE_BLOCK_SIZE = 1024 * 1024;
constexpr size_t ENSTORE_LARGE_BLOCK_SIZE = 256 * 1024;

class EnstoreTestingRetrieveJob : public cta::RetrieveJob {
public:
  EnstoreTestingRetrieveJob()
      : cta::RetrieveJob(nullptr,
                         cta::common::dataStructures::RetrieveRequest(),
                         cta::common::dataStructures::ArchiveFile(),
                         1,
                         cta::PositioningMethod::ByFSeq) {}
};

struct RawUHL2 {
  char label[4];
  char unknown[60];
  char actualFileSize[14];
  char reserved[2];
};

static_assert(sizeof(RawUHL2) == sizeof(castor::tape::tapeFile::UHL2));

template<size_t n>
void setPaddedString(char (&field)[n], const std::string& value) {
  std::memset(field, ' ', n);
  std::memcpy(field, value.data(), std::min(n, value.size()));
}

template<size_t n>
void setPaddedDecimal(char (&field)[n], const size_t value) {
  std::ostringstream valueStream;
  valueStream << std::setfill('0') << std::setw(n) << value;
  const auto valueString = valueStream.str();
  std::memcpy(field, valueString.data(), n);
}

void setVol1LabelStandard(castor::tape::tapeFile::VOL1& vol1, const char labelStandard) {
  auto* rawVol1 = reinterpret_cast<char*>(&vol1);
  rawVol1[sizeof(vol1) - 1] = labelStandard;
}

template<typename T>
void setRecordFormat(T& header, const char recordFormat) {
  auto* rawHeader = reinterpret_cast<char*>(&header);
  rawHeader[4] = recordFormat;
}

template<typename T>
void appendStruct(std::vector<char>& block, const T& value) {
  const auto* begin = reinterpret_cast<const char*>(&value);
  block.insert(block.end(), begin, begin + sizeof(T));
}

std::vector<char> makeCpioPayloadBlock(const std::string& payload, const size_t blockSize) {
  std::stringstream file;
  const std::string magic = "070707";
  const uint32_t dev = 0;
  const uint32_t ino = 1;
  const uint32_t mode = 33188;
  const uint32_t uid = 0;
  const uint32_t gid = 0;
  const uint32_t nlink = 1;
  const uint32_t rdev = 0;
  const uint64_t mtime = 1660901455;
  const std::string fid = "01234567890";
  const uint32_t nameSize = fid.size();

  file << magic << std::setfill('0') << std::oct << std::setw(6) << dev << std::setw(6) << ino << std::setw(6) << mode
       << std::setw(6) << uid << std::setw(6) << gid << std::setw(6) << nlink << std::setw(6) << rdev << std::setw(11)
       << mtime << std::setw(6) << nameSize << "H" << std::hex << std::setw(10) << payload.size() << fid;
  file << payload;
  file << magic << std::setfill('0') << std::oct << std::setw(6) << dev << std::setw(6) << ino << std::setw(6) << mode
       << std::setw(6) << uid << std::setw(6) << gid << std::setw(6) << nlink << std::setw(6) << rdev << std::setw(11)
       << mtime << std::setw(6) << 10 << "H" << std::hex << std::setw(10) << 0 << fid << "TRAILER!!" << '\0';

  const auto cpioPayload = file.str();
  if (cpioPayload.size() > blockSize) {
    throw std::runtime_error("CPIO payload does not fit in tape block");
  }

  std::vector<char> block(blockSize, '\0');
  std::copy(cpioPayload.begin(), cpioPayload.end(), block.begin());
  return block;
}

castor::tape::tapeserver::drive::deviceInfo makeDeviceInfo() {
  castor::tape::tapeserver::drive::deviceInfo deviceInfo;
  deviceInfo.vendor = "IBM";
  deviceInfo.product = "ULT3580";
  deviceInfo.serialNumber = "MHVTL_A1";
  return deviceInfo;
}

void writeVol1(castor::tape::tapeserver::drive::FakeDrive& drive, const std::string& vid, const char labelStandard) {
  castor::tape::tapeFile::VOL1 vol1;
  vol1.fill(vid, 0);
  setVol1LabelStandard(vol1, labelStandard);
  drive.writeBlock(&vol1, sizeof(vol1));
  drive.writeSyncFileMarks(1);
}

void writeEnstoreTape(castor::tape::tapeserver::drive::FakeDrive& drive,
                      const std::string& vid,
                      const std::string& payload) {
  writeVol1(drive, vid, '0');
  const auto payloadBlock = makeCpioPayloadBlock(payload, ENSTORE_BLOCK_SIZE);
  drive.writeBlock(payloadBlock.data(), payloadBlock.size());
  drive.writeSyncFileMarks(1);
  drive.rewind();
}

RawUHL2 makeUHL2(const size_t fileSize) {
  RawUHL2 uhl2;
  std::memset(&uhl2, ' ', sizeof(uhl2));
  setPaddedString(uhl2.label, "UHL2");
  setPaddedDecimal(uhl2.actualFileSize, fileSize);
  return uhl2;
}

std::vector<char> makeEnstoreLargeHeaderBlock(const std::string& vid, const size_t payloadSize) {
  const auto deviceInfo = makeDeviceInfo();
  castor::tape::tapeFile::HDR1 hdr1;
  hdr1.fill("1", vid, 1);

  castor::tape::tapeFile::HDR2 hdr2;
  hdr2.fill(ENSTORE_LARGE_BLOCK_SIZE, true);
  setRecordFormat(hdr2, 'D');

  castor::tape::tapeFile::UHL1 uhl1;
  uhl1.fill(1, ENSTORE_LARGE_BLOCK_SIZE, "CERN", "TAPEHOST", deviceInfo);

  const auto uhl2 = makeUHL2(payloadSize);

  std::vector<char> block;
  appendStruct(block, hdr1);
  appendStruct(block, hdr2);
  appendStruct(block, uhl1);
  appendStruct(block, uhl2);
  return block;
}

std::vector<char> makeEnstoreLargeTrailerBlock(const std::string& vid) {
  const auto deviceInfo = makeDeviceInfo();
  castor::tape::tapeFile::EOF1 eof1;
  eof1.fill("1", vid, 1, 1);

  castor::tape::tapeFile::EOF2 eof2;
  eof2.fill(ENSTORE_LARGE_BLOCK_SIZE, true);
  setRecordFormat(eof2, 'D');

  castor::tape::tapeFile::UTL1 utl1;
  utl1.fill(1, ENSTORE_LARGE_BLOCK_SIZE, "CERN", "TAPEHOST", deviceInfo);

  std::vector<char> block;
  appendStruct(block, eof1);
  appendStruct(block, eof2);
  appendStruct(block, utl1);
  return block;
}

void writeEnstoreLargeTape(castor::tape::tapeserver::drive::FakeDrive& drive,
                           const std::string& vid,
                           const std::string& payload) {
  writeVol1(drive, vid, '3');

  const auto headerBlock = makeEnstoreLargeHeaderBlock(vid, payload.size());
  drive.writeBlock(headerBlock.data(), headerBlock.size());
  drive.writeSyncFileMarks(1);

  std::vector<char> payloadBlock(ENSTORE_LARGE_BLOCK_SIZE, '\0');
  std::copy(payload.begin(), payload.end(), payloadBlock.begin());
  drive.writeBlock(payloadBlock.data(), payloadBlock.size());
  drive.writeSyncFileMarks(1);

  const auto trailerBlock = makeEnstoreLargeTrailerBlock(vid);
  drive.writeBlock(trailerBlock.data(), trailerBlock.size());
  drive.writeSyncFileMarks(1);
  drive.rewind();
}

void configureRetrieveJob(EnstoreTestingRetrieveJob& fileToRecall) {
  fileToRecall.selectedCopyNb = 0;
  fileToRecall.archiveFile.tapeFiles.emplace_back();
  fileToRecall.selectedTapeFile().fSeq = 1;
  fileToRecall.selectedTapeFile().blockId = 1;
  fileToRecall.retrieveRequest.archiveFileID = 1;
  fileToRecall.positioningMethod = cta::PositioningMethod::ByFSeq;
}

}  // namespace

class EnstoreTapeFileTest : public ::testing::Test {
protected:
  void readAndVerifyPayload(const cta::common::dataStructures::Label::Format labelFormat,
                            const std::string& vid,
                            const std::string& payload,
                            const size_t expectedBlockSize) {
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid = vid;
    volInfo.nbFiles = 1;
    volInfo.labelFormat = labelFormat;
    volInfo.mountType = cta::common::dataStructures::MountType::Retrieve;

    const auto readSession = castor::tape::tapeFile::ReadSessionFactory::create(m_drive, volInfo, false);
    ASSERT_NE(readSession, nullptr);
    ASSERT_EQ(readSession->m_vid, vid);

    EnstoreTestingRetrieveJob fileToRecall;
    configureRetrieveJob(fileToRecall);
    const auto reader = castor::tape::tapeFile::FileReaderFactory::create(*readSession, fileToRecall);
    const auto blockSize = reader->getBlockSize();
    ASSERT_EQ(blockSize, expectedBlockSize);

    std::vector<char> data(blockSize);
    const auto bytesRead = reader->readNextDataBlock(data.data(), blockSize);
    ASSERT_EQ(bytesRead, payload.size());
    ASSERT_EQ(std::string(data.data(), bytesRead), payload);
    ASSERT_THROW(reader->readNextDataBlock(data.data(), blockSize), castor::tape::tapeFile::EndOfFile);
  }

  castor::tape::tapeserver::drive::FakeDrive m_drive;
};

TEST_F(EnstoreTapeFileTest, canReadEnstoreTapeFromFakeDrive) {
  const std::string vid = "FL1212";
  const std::string payload = "Hello Enstore!";

  writeEnstoreTape(m_drive, vid, payload);
  readAndVerifyPayload(cta::common::dataStructures::Label::Format::Enstore, vid, payload, ENSTORE_BLOCK_SIZE);
}

TEST_F(EnstoreTapeFileTest, canReadEnstoreLargeTapeFromFakeDrive) {
  const std::string vid = "FL1587";
  const std::string payload = "Hello EnstoreLarge!";

  writeEnstoreLargeTape(m_drive, vid, payload);
  readAndVerifyPayload(cta::common::dataStructures::Label::Format::EnstoreLarge,
                       vid,
                       payload,
                       ENSTORE_LARGE_BLOCK_SIZE);
}

}  // namespace unitTests

/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "StressDrive.hpp"

#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <unistd.h>

namespace unitTests {

namespace fs = std::filesystem;
using namespace cta::tape::drive;

namespace {

void writeLibraryConf(const fs::path& baseDir,
                      const std::string& lib,
                      const std::string& vidPrefix,
                      uint64_t capacityBytes,
                      uint32_t mountDelayMs = 0,
                      uint32_t maxWriteRateHz = 0) {
  const auto confDir = baseDir / "config" / lib;
  fs::create_directories(confDir);
  std::ofstream f(confDir / "library.toml");
  f << "[library]\n"
    << "name = \"" << lib << "\"\n"
    << "changerTimeoutMs = 5000\n\n"
    << "[[library.mediaTypes]]\n"
    << "name = \"ST\"\n"
    << "vidPrefix = \"" << vidPrefix << "\"\n"
    << "capacityBytes = " << capacityBytes << "\n"
    << "mountDelayMs = " << mountDelayMs << "\n"
    << "maxWriteRateHz = " << maxWriteRateHz << "\n"
    << "count = 10\n";
}

}  // namespace

class StressDriveTest : public ::testing::Test {
protected:
  fs::path m_baseDir;
  const std::string m_lib = "test-lib";
  const std::string m_drive = "STRESS0001";
  const std::string m_vidPrefix = "ST";
  // 1 MB — large enough that ENOSPC is not a concern unless explicitly tested.
  static constexpr uint64_t k_defaultCapacity = 1024 * 1024;

  void SetUp() override {
    m_baseDir = fs::temp_directory_path() / ("stress-drive-test-" + std::to_string(::getpid()));
    fs::create_directories(m_baseDir);
    writeLibraryConf(m_baseDir, m_lib, m_vidPrefix, k_defaultCapacity);
    fs::create_directories(slotsDir());
    fs::create_directories(changerDir());
    fs::create_directories(driveDir());
  }

  void TearDown() override { fs::remove_all(m_baseDir); }

  fs::path driveDir() const {
    return m_baseDir / "contents" / m_lib / "drives" / m_drive;
  }

  fs::path slotsDir() const { return m_baseDir / "contents" / m_lib / "slots"; }

  fs::path changerDir() const {
    return m_baseDir / "contents" / m_lib / "changer";
  }

  // Place an empty (blank) tape file in the drive directory, simulating a
  // completed MediaChangerFacade mount.
  void mountBlankTape(const std::string& vid) const {
    std::ofstream(driveDir() / (vid + ".tape"), std::ios::binary);
  }
};

TEST_F(StressDriveTest, ConstructionParsesLibraryConf) {
  EXPECT_NO_THROW(StressDrive(m_drive, m_lib, m_baseDir));
}

TEST_F(StressDriveTest, ConstructionThrowsIfConfMissing) {
  const fs::path badBase = m_baseDir / "nonexistent";
  fs::create_directories(badBase / "contents" / m_lib / "drives" / m_drive);
  EXPECT_THROW(StressDrive(m_drive, m_lib, badBase), cta::exception::Exception);
}

TEST_F(StressDriveTest, NoTapeInPlaceAfterConstruction) {
  StressDrive drv(m_drive, m_lib, m_baseDir);
  EXPECT_FALSE(drv.hasTapeInPlace());
}

TEST_F(StressDriveTest, WaitUntilReady_ThrowsIfNoTapeFile) {
  StressDrive drv(m_drive, m_lib, m_baseDir);
  // drive directory is empty → no *.tape file
  EXPECT_THROW(drv.waitUntilReady(30), cta::exception::Exception);
}

TEST_F(StressDriveTest, WaitUntilReady_LoadsBlankTapeWithSyntheticVOL1) {
  StressDrive drv(m_drive, m_lib, m_baseDir);
  mountBlankTape("ST0001");

  EXPECT_NO_THROW(drv.waitUntilReady(30));
  EXPECT_TRUE(drv.hasTapeInPlace());

  // Blank tape → synthetic VOL1 label synthesised → tape is non-blank, at BOT.
  EXPECT_FALSE(drv.isTapeBlank());
  EXPECT_EQ(0u, drv.getPositionInfo().currentPosition);
}

TEST_F(StressDriveTest, WaitUntilReady_ReentryGuardIsNoop) {
  StressDrive drv(m_drive, m_lib, m_baseDir);
  mountBlankTape("ST0001");
  drv.waitUntilReady(30);

  // A second call (e.g. from TapeCleaning::~TapeCleaning) must not reload.
  EXPECT_NO_THROW(drv.waitUntilReady(30));
  EXPECT_TRUE(drv.hasTapeInPlace());
}

TEST_F(StressDriveTest, UnloadTape_SkipsSaveIfNothingMounted) {
  StressDrive drv(m_drive, m_lib, m_baseDir);
  // unloadTape() without a prior waitUntilReady() must not throw.
  EXPECT_NO_THROW(drv.unloadTape());
  EXPECT_FALSE(drv.hasTapeInPlace());
}

TEST_F(StressDriveTest, DataPersistsAcrossMountCycle) {
  const std::string vid = "ST0002";
  mountBlankTape(vid);

  // First session: skip VOL1, write a payload block, then save.
  {
    StressDrive drv(m_drive, m_lib, m_baseDir);
    drv.waitUntilReady(30);  // position = 0, tape = [VOL1(80)]

    char vol1buf[80] = {};
    drv.readBlock(vol1buf, sizeof(vol1buf));  // skip VOL1, position → 1

    const std::string payload = "persistent data block";
    drv.writeBlock(payload.data(), payload.size());  // pos 1
    drv.unloadTape();
  }

  // The tape file must be non-empty after save.
  ASSERT_GT(fs::file_size(driveDir() / (vid + ".tape")), 0u);

  // Second session: reload and verify the payload survives.
  {
    StressDrive drv2(m_drive, m_lib, m_baseDir);
    drv2.waitUntilReady(30);  // position = 0, tape = [VOL1, payload]
    EXPECT_FALSE(drv2.isTapeBlank());

    char vol1buf[80] = {};
    drv2.readBlock(vol1buf, sizeof(vol1buf));  // skip VOL1 (pos 0 → 1)

    char buf[64] = {};
    const ssize_t n = drv2.readBlock(buf, sizeof(buf));  // payload at pos 1
    EXPECT_EQ("persistent data block", std::string(buf, static_cast<size_t>(n)));
  }
}

TEST_F(StressDriveTest, MediaTypeCapacityIsApplied) {
  // Overwrite config with a tiny capacity so ENOSPC fires after VOL1.
  // VOL1 = 80 bytes; remaining = 100 - 80 = 20 bytes; writing 25 bytes → ENOSPC.
  writeLibraryConf(m_baseDir, m_lib, m_vidPrefix, 100);
  mountBlankTape("ST0003");

  StressDrive drv(m_drive, m_lib, m_baseDir);
  drv.waitUntilReady(30);

  const std::string big(25, 'X');
  try {
    drv.writeBlock(big.data(), big.size());
    FAIL() << "Expected ENOSPC";
  } catch (const cta::exception::Errnum& e) {
    EXPECT_EQ(ENOSPC, e.errorNumber());
  }
}

TEST_F(StressDriveTest, GetSerialNumber_IncludesDriveName) {
  StressDrive drv(m_drive, m_lib, m_baseDir);
  EXPECT_NE(std::string::npos, drv.getSerialNumber().find("STRESS0001"));
}

TEST_F(StressDriveTest, GetDriveStatus_ReadyAfterMount) {
  StressDrive drv(m_drive, m_lib, m_baseDir);
  mountBlankTape("ST0004");
  drv.waitUntilReady(30);

  const auto st = drv.getDriveStatus();
  EXPECT_TRUE(st.ready);
  EXPECT_TRUE(st.hasTapeInPlace);
  EXPECT_TRUE(st.bot);
}

TEST_F(StressDriveTest, GetDeviceInfo_SerialNumberMatchesDriveName) {
  StressDrive drv(m_drive, m_lib, m_baseDir);
  // getDeviceInfo() overrides serialNumber with driveName (capped at 12 chars).
  const auto info = drv.getDeviceInfo();
  EXPECT_FALSE(info.serialNumber.empty());
  EXPECT_EQ(m_drive.substr(0, 12), info.serialNumber);
}

TEST_F(StressDriveTest, MultipleBlocksAndFilemarksRoundTrip) {
  const std::string vid = "ST0005";
  mountBlankTape(vid);

  // Write [VOL1 | HDR1 | data | FM | HDR1 | data | FM] and reload.
  {
    StressDrive drv(m_drive, m_lib, m_baseDir);
    drv.waitUntilReady(30);  // position = 0, tape = [VOL1(80)]

    char vol1buf[80] = {};
    drv.readBlock(vol1buf, sizeof(vol1buf));  // skip VOL1, position → 1

    const std::string hdr = "HDR1xxxx";
    const std::string dat = "file body";
    drv.writeBlock(hdr.data(), hdr.size());
    drv.writeBlock(dat.data(), dat.size());
    drv.writeSyncFileMarks(1);
    drv.writeBlock(hdr.data(), hdr.size());
    drv.writeBlock(dat.data(), dat.size());
    drv.writeSyncFileMarks(1);
    drv.unloadTape();
  }

  {
    StressDrive drv2(m_drive, m_lib, m_baseDir);
    drv2.waitUntilReady(30);  // position = 0, tape = [VOL1 | HDR1 | data | FM | ...]

    char vol1buf[80] = {};
    drv2.readBlock(vol1buf, sizeof(vol1buf));  // skip VOL1 (pos 0 → 1)

    char buf[64] = {};
    drv2.readBlock(buf, sizeof(buf));            // HDR1
    const ssize_t n = drv2.readBlock(buf, sizeof(buf));  // data
    EXPECT_EQ("file body", std::string(buf, static_cast<size_t>(n)));
    EXPECT_NO_THROW(drv2.readFileMark("test"));  // FM
  }
}

}  // namespace unitTests

/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "VtlDrive.hpp"

#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"

#include <chrono>
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
                      uint32_t maxWriteRateHz = 0,
                      uint32_t maxReadRateHz = 0,
                      uint32_t mountDelayMaxMs = 0) {
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
    << "mountDelayMaxMs = " << mountDelayMaxMs << "\n"
    << "maxWriteRateHz = " << maxWriteRateHz << "\n"
    << "maxReadRateHz = " << maxReadRateHz << "\n"
    << "count = 10\n";
}

}  // namespace

class VtlDriveTest : public ::testing::Test {
protected:
  fs::path m_baseDir;
  const std::string m_lib = "test-lib";
  const std::string m_drive = "STRESS0001";
  const std::string m_vidPrefix = "ST";
  // 1 MB — large enough that ENOSPC is not a concern unless explicitly tested.
  static constexpr uint64_t k_defaultCapacity = 1024 * 1024;

  void SetUp() override {
    m_baseDir = fs::temp_directory_path() / ("vtl-drive-test-" + std::to_string(::getpid()));
    fs::create_directories(m_baseDir);
    writeLibraryConf(m_baseDir, m_lib, m_vidPrefix, k_defaultCapacity);
    fs::create_directories(slotsDir());
    fs::create_directories(changerDir());
    fs::create_directories(driveDir());
  }

  void TearDown() override { fs::remove_all(m_baseDir); }

  fs::path driveDir() const { return m_baseDir / "contents" / m_lib / "drives" / m_drive; }

  fs::path slotsDir() const { return m_baseDir / "contents" / m_lib / "slots"; }

  fs::path changerDir() const { return m_baseDir / "contents" / m_lib / "changer"; }

  // Place an empty (blank) tape file in the drive directory, simulating a
  // completed MediaChangerFacade mount.
  void mountBlankTape(const std::string& vid) const { std::ofstream(driveDir() / (vid + ".tape"), std::ios::binary); }
};

TEST_F(VtlDriveTest, ConstructionParsesLibraryConf) {
  EXPECT_NO_THROW(VtlDrive(m_drive, m_lib, m_baseDir));
}

TEST_F(VtlDriveTest, ConstructionThrowsIfConfMissing) {
  const fs::path badBase = m_baseDir / "nonexistent";
  fs::create_directories(badBase / "contents" / m_lib / "drives" / m_drive);
  EXPECT_THROW(VtlDrive(m_drive, m_lib, badBase), cta::exception::Exception);
}

TEST_F(VtlDriveTest, NoTapeInPlaceAfterConstruction) {
  VtlDrive drv(m_drive, m_lib, m_baseDir);
  EXPECT_FALSE(drv.hasTapeInPlace());
}

TEST_F(VtlDriveTest, WaitUntilReady_ThrowsIfNoTapeFile) {
  VtlDrive drv(m_drive, m_lib, m_baseDir);
  // drive directory is empty → no *.tape file
  EXPECT_THROW(drv.waitUntilReady(30), cta::exception::Exception);
}

TEST_F(VtlDriveTest, WaitUntilReady_LoadsBlankTapeWithSyntheticVOL1) {
  VtlDrive drv(m_drive, m_lib, m_baseDir);
  mountBlankTape("ST0001");

  EXPECT_NO_THROW(drv.waitUntilReady(30));
  EXPECT_TRUE(drv.hasTapeInPlace());

  // Blank tape → synthetic VOL1 label synthesised → tape is non-blank, at BOT.
  EXPECT_FALSE(drv.isTapeBlank());
  EXPECT_EQ(0u, drv.getPositionInfo().currentPosition);
}

TEST_F(VtlDriveTest, WaitUntilReady_ReentryGuardIsNoop) {
  VtlDrive drv(m_drive, m_lib, m_baseDir);
  mountBlankTape("ST0001");
  drv.waitUntilReady(30);

  // A second call (e.g. from TapeCleaning::~TapeCleaning) must not reload.
  EXPECT_NO_THROW(drv.waitUntilReady(30));
  EXPECT_TRUE(drv.hasTapeInPlace());
}

TEST_F(VtlDriveTest, UnloadTape_SkipsSaveIfNothingMounted) {
  VtlDrive drv(m_drive, m_lib, m_baseDir);
  // unloadTape() without a prior waitUntilReady() must not throw.
  EXPECT_NO_THROW(drv.unloadTape());
  EXPECT_FALSE(drv.hasTapeInPlace());
}

TEST_F(VtlDriveTest, DataPersistsAcrossMountCycle) {
  const std::string vid = "ST0002";
  mountBlankTape(vid);

  // First session: skip VOL1, write a payload block, then save.
  {
    VtlDrive drv(m_drive, m_lib, m_baseDir);
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
    VtlDrive drv2(m_drive, m_lib, m_baseDir);
    drv2.waitUntilReady(30);  // position = 0, tape = [VOL1, payload]
    EXPECT_FALSE(drv2.isTapeBlank());

    char vol1buf[80] = {};
    drv2.readBlock(vol1buf, sizeof(vol1buf));  // skip VOL1 (pos 0 → 1)

    char buf[64] = {};
    const ssize_t n = drv2.readBlock(buf, sizeof(buf));  // payload at pos 1
    EXPECT_EQ("persistent data block", std::string(buf, static_cast<size_t>(n)));
  }
}

TEST_F(VtlDriveTest, MediaTypeCapacityIsApplied) {
  // Overwrite config with a tiny capacity so ENOSPC fires after VOL1.
  // VOL1 = 80 bytes; remaining = 100 - 80 = 20 bytes; writing 25 bytes → ENOSPC.
  writeLibraryConf(m_baseDir, m_lib, m_vidPrefix, 100);
  mountBlankTape("ST0003");

  VtlDrive drv(m_drive, m_lib, m_baseDir);
  drv.waitUntilReady(30);  // position = 0, VOL1 at tape[0], remainingSpaceAfter = 20

  char vol1buf[80] = {};
  drv.readBlock(vol1buf, sizeof(vol1buf));  // skip VOL1, position → 1

  // getRemainingSpace(1) = tape[0].remainingSpaceAfter = 100 - 80 = 20; writing 25 → ENOSPC.
  const std::string big(25, 'X');
  try {
    drv.writeBlock(big.data(), big.size());
    FAIL() << "Expected ENOSPC";
  } catch (const cta::exception::Errnum& e) {
    EXPECT_EQ(ENOSPC, e.errorNumber());
  }
}

TEST_F(VtlDriveTest, MountDelayMaxMsWidensTheJitterSpreadWithoutRaisingTheFloor) {
  // mountDelayMs = 5, mountDelayMaxMs = 90: an 18x spread [5, 90] ms, same numbers as the
  // production [5 s, 90 s] configuration this is modelled on (just ms instead of s, to keep the
  // test fast). Unset (0), this would default to a 2x spread [5, 10] - the assertion on the
  // upper bound is what actually exercises mountDelayMaxMs rather than silently falling back to
  // that.
  constexpr uint32_t mountDelayMs = 5;
  constexpr uint32_t mountDelayMaxMs = 90;
  writeLibraryConf(m_baseDir, m_lib, m_vidPrefix, k_defaultCapacity, mountDelayMs, 0, 0, mountDelayMaxMs);
  const std::string vid = m_vidPrefix + "00001";

  // A handful of mounts is enough that, if the ceiling really is 90 ms and not the old 10 ms
  // default, at least one draw lands above 10 ms with overwhelming probability (uniform over
  // [5, 90]: per draw P(>10 ms) = 80/85, so P(none exceed in 20 draws) = (5/85)^20).
  uint64_t maxObservedMs = 0;
  for (int i = 0; i < 20; ++i) {
    mountBlankTape(vid);
    VtlDrive drv(m_drive, m_lib, m_baseDir);
    const auto start = std::chrono::steady_clock::now();
    drv.waitUntilReady(30);
    const auto elapsedMs =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    EXPECT_GE(elapsedMs, mountDelayMs) << "below the configured floor on draw " << i;
    EXPECT_LE(elapsedMs, mountDelayMaxMs) << "above the configured ceiling on draw " << i;
    maxObservedMs = std::max<uint64_t>(maxObservedMs, elapsedMs);
    drv.unloadTape();
  }
  EXPECT_GT(maxObservedMs, 2 * mountDelayMs)
    << "no draw exceeded the old 2x-mountDelayMs default; mountDelayMaxMs may not be taking effect";
}

TEST_F(VtlDriveTest, TransferRateCeilingsSleepOncePerFile) {
  // 50 files/s on both directions, i.e. 20 ms per file. The throttle sleeps before every HDR1
  // except the first one on a cartridge, so n files cost at least (n-1) intervals. Asserting only
  // the lower bound keeps this independent of how slow the machine running it is.
  constexpr uint32_t rateHz = 50;
  constexpr int files = 4;
  constexpr auto minInterval = std::chrono::milliseconds(1000 / rateHz);
  const auto expected = (files - 1) * minInterval;

  writeLibraryConf(m_baseDir, m_lib, m_vidPrefix, k_defaultCapacity, 0, rateHz, rateHz);
  const std::string vid = m_vidPrefix + "00001";
  mountBlankTape(vid);

  const std::string hdr = "HDR1xxxx";
  const std::string dat = "file body";

  {
    VtlDrive drv(m_drive, m_lib, m_baseDir);
    drv.waitUntilReady(30);
    char vol1buf[80] = {};
    drv.readBlock(vol1buf, sizeof(vol1buf));  // skip the synthetic VOL1

    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < files; ++i) {
      drv.writeBlock(hdr.data(), hdr.size());
      drv.writeBlock(dat.data(), dat.size());
      drv.writeSyncFileMarks(1);
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_GE(elapsed, expected) << "write throttle did not sleep once per file";
    drv.unloadTape();
  }

  {
    VtlDrive drv(m_drive, m_lib, m_baseDir);
    drv.waitUntilReady(30);
    char vol1buf[80] = {};
    drv.readBlock(vol1buf, sizeof(vol1buf));

    // The label blocks go through readExactBlock, which is where the read throttle lives.
    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < files; ++i) {
      char hdrbuf[8] = {};
      drv.readExactBlock(hdrbuf, sizeof(hdrbuf), "HDR1");
      char datbuf[64] = {};
      drv.readBlock(datbuf, sizeof(datbuf));
      drv.readFileMark("FM");
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_GE(elapsed, expected) << "read throttle did not sleep once per file";
  }
}

TEST_F(VtlDriveTest, PerMountByteCountersCoverTheCurrentCartridgeOnly) {
  const std::string vid = m_vidPrefix + "00001";
  mountBlankTape(vid);

  const std::string hdr = "HDR1xxxx";
  const std::string dat = "file body";
  const uint64_t writtenBytes = 2 * (hdr.size() + dat.size());

  {
    VtlDrive drv(m_drive, m_lib, m_baseDir);
    drv.waitUntilReady(30);
    EXPECT_EQ(0u, drv.getTapeWriteErrors().at("mountTotalWriteBytesProcessed"));

    char vol1buf[80] = {};
    drv.readBlock(vol1buf, sizeof(vol1buf));
    for (int i = 0; i < 2; ++i) {
      drv.writeBlock(hdr.data(), hdr.size());
      drv.writeBlock(dat.data(), dat.size());
      drv.writeSyncFileMarks(1);
    }
    // File marks take no space on the tape and are not payload, so they are not counted either.
    EXPECT_EQ(writtenBytes, drv.getTapeWriteErrors().at("mountTotalWriteBytesProcessed"));
    EXPECT_EQ(sizeof(vol1buf), drv.getTapeReadErrors().at("mountTotalReadBytesProcessed"));
    drv.unloadTape();
  }

  {
    // A fresh mount starts from zero, as a real drive's log pages do.
    VtlDrive drv(m_drive, m_lib, m_baseDir);
    drv.waitUntilReady(30);
    EXPECT_EQ(0u, drv.getTapeWriteErrors().at("mountTotalWriteBytesProcessed"));
    EXPECT_EQ(0u, drv.getTapeReadErrors().at("mountTotalReadBytesProcessed"));

    char vol1buf[80] = {};
    drv.readBlock(vol1buf, sizeof(vol1buf));
    char hdrbuf[8] = {};
    drv.readExactBlock(hdrbuf, sizeof(hdrbuf), "HDR1");
    EXPECT_EQ(sizeof(vol1buf) + sizeof(hdrbuf), drv.getTapeReadErrors().at("mountTotalReadBytesProcessed"));
    EXPECT_EQ(0u, drv.getTapeWriteErrors().at("mountTotalWriteBytesProcessed"));
  }
}

TEST_F(VtlDriveTest, GetSerialNumber_IncludesDriveName) {
  VtlDrive drv(m_drive, m_lib, m_baseDir);
  EXPECT_NE(std::string::npos, drv.getSerialNumber().find("STRESS0001"));
}

TEST_F(VtlDriveTest, GetDriveStatus_ReadyAfterMount) {
  VtlDrive drv(m_drive, m_lib, m_baseDir);
  mountBlankTape("ST0004");
  drv.waitUntilReady(30);

  const auto st = drv.getDriveStatus();
  EXPECT_TRUE(st.ready);
  EXPECT_TRUE(st.hasTapeInPlace);
  EXPECT_TRUE(st.bot);
}

TEST_F(VtlDriveTest, GetDeviceInfo_SerialNumberMatchesDriveName) {
  VtlDrive drv(m_drive, m_lib, m_baseDir);
  // getDeviceInfo() overrides serialNumber with driveName (capped at 12 chars).
  const auto info = drv.getDeviceInfo();
  EXPECT_FALSE(info.serialNumber.empty());
  EXPECT_EQ(m_drive.substr(0, 12), info.serialNumber);
}

TEST_F(VtlDriveTest, MultipleBlocksAndFilemarksRoundTrip) {
  const std::string vid = "ST0005";
  mountBlankTape(vid);

  // Write [VOL1 | HDR1 | data | FM | HDR1 | data | FM] and reload.
  {
    VtlDrive drv(m_drive, m_lib, m_baseDir);
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
    VtlDrive drv2(m_drive, m_lib, m_baseDir);
    drv2.waitUntilReady(30);  // position = 0, tape = [VOL1 | HDR1 | data | FM | ...]

    char vol1buf[80] = {};
    drv2.readBlock(vol1buf, sizeof(vol1buf));  // skip VOL1 (pos 0 → 1)

    char buf[64] = {};
    drv2.readBlock(buf, sizeof(buf));                    // HDR1
    const ssize_t n = drv2.readBlock(buf, sizeof(buf));  // data
    EXPECT_EQ("file body", std::string(buf, static_cast<size_t>(n)));
    EXPECT_NO_THROW(drv2.readFileMark("test"));  // FM
  }
}

}  // namespace unitTests

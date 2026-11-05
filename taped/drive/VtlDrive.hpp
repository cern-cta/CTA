/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "DriveInterface.hpp"
#include "InMemoryTape.hpp"

#include <chrono>
#include <filesystem>
#include <limits>
#include <map>
#include <random>
#include <string>

namespace cta::tape::drive {

/**
 * Virtual Tape Library drive: implements DriveInterface and holds an
 * InMemoryTape whose content is persisted to a file on a shared volume
 * between mount/dismount cycles.  This allows multiple drives — in the same
 * taped process or in different taped processes on the same host — to share
 * virtual tapes by VID, exactly as a physical tape library makes one tape
 * accessible from any drive slot.
 *
 * Directory layout (all under baseDir, e.g. /cta-stress-lib):
 *
 *   config/<lib>/library.toml          — per-library configuration
 *   contents/<lib>/slots/VID.tape      — tape at rest
 *   contents/<lib>/changer/VID.tape    — tape in transit (robot holds it)
 *   contents/<lib>/changer/.lock       — robot-busy flag (O_CREAT|O_EXCL)
 *   contents/<lib>/drives/<drv>/       — empty = no tape loaded
 *   contents/<lib>/drives/<drv>/VID.tape — tape mounted in this drive
 *
 * Lifecycle:
 *   1. MediaChangerFacade::mountTapeReadWrite(vid) acquires the changer lock
 *      and renames VID.tape: slots/ → changer/ → drives/<driveName>/.
 *   2. DataTransferSession calls waitUntilReady(): VtlDrive scans
 *      drives/<driveName>/ for a *.tape file, derives the VID from its stem,
 *      reads the tape capacity from library.toml, then loads the binary
 *      block stream from the file into m_tape.  An empty file means a blank tape.
 *   3. The full transfer runs via m_tape in-memory block I/O.
 *   4. DataTransferSession calls unloadTape(): VtlDrive serialises m_tape
 *      back to the same file, then marks the tape as absent.
 *   5. MediaChangerFacade::dismountTape(vid) acquires the changer lock and
 *      renames VID.tape: drives/<driveName>/ → changer/ → slots/.
 *
 * The drive is registered in the CTA catalogue with DriveDevice = "virtual://<base_dir>",
 * e.g. "virtual:///cta-stress-lib".  drive::createDrive() detects the "virtual://"
 * prefix and creates a VtlDrive; the path component is the library root.
 */
class VtlDrive : public DriveInterface {
public:
  /**
   * @param driveName          Drive name as registered in the CTA catalogue
   *                           (e.g. "STRESS0003").
   * @param logicalLibraryName Logical library this drive belongs to; used to
   *                           construct all paths under baseDir.
   * @param baseDir            Root of the virtual-tape-library shared volume
   *                           (e.g. /dev/shm/cta-stress).
   *
   * The constructor reads baseDir/config/<logicalLibraryName>/library.toml to
   * obtain the mount-delay for this drive and the VID-prefix → capacity map
   * for all media types in the library.
   */
  VtlDrive(std::string driveName, std::string logicalLibraryName, std::filesystem::path baseDir);
  ~VtlDrive() override = default;

  /**
   * Simulates tape loading:
   *   1. Sleeps a uniformly random duration in [mountDelayMs, 2×mountDelayMs]
   *      so every drive has a guaranteed minimum delay while concurrent mounts
   *      are spread across a window, preventing synchronised PVC I/O storms.
   *   2. Scans contents/<lib>/drives/<driveName>/ for a *.tape file.
   *   3. Derives VID from filename stem; looks up tape capacity from library.toml.
   *   4. Loads the binary block stream from the file into m_tape.  An empty
   *      file (new blank tape) synthesises a VOL1 label in m_tape.
   *   5. Resets position to BOT.
   */
  void waitUntilReady(uint32_t timeoutSecond) override;

  /**
   * Serialises the current m_tape state back to the *.tape file via an atomic
   * tmp-rename, then marks the tape as absent.  Saving is skipped if no tape
   * was ever loaded in this session.
   */
  void unloadTape() override;

  // Drive-status and hardware-info overrides.
  driveStatus getDriveStatus() override;
  deviceInfo getDeviceInfo() override;
  std::string getSerialNumber() override;

  // These SCSI concepts have no meaning for a virtual tape; silently ignored.
  void setDensityAndCompression(bool = true, unsigned char = 0) override {}

  void setSTBufferWrite(bool) override {}

  drive::LBPInfo getLBPInfo() override;

  void setLogicalBlockProtection(unsigned char, unsigned char, bool, bool) override {}

  void setEncryptionKey(const std::string&) override {}

  bool clearEncryptionKey() override { return false; }

  bool isEncryptionCapEnabled() override { return false; }

  // Virtual drive has no SCSI device; the empty-drive probe is a no-op.
  bool requiresEmptyDriveProbe() const override { return false; }

  /**
   * Adaptive-mirror throttle: sleeps once per file at HDR1 for
   * max(minInterval, elapsed), where minInterval = 1s/maxWriteRateHz.
   * No sleep when maxWriteRateHz == 0.
   */
  void writeBlock(const void* data, size_t count) override;

  // --- Delegation to InMemoryTape (tape-block operations) ---
  compressionStats getCompression() override { return m_tape.getCompression(); }

  void clearCompressionStats() override { m_tape.clearCompressionStats(); }

  void positionToLogicalObject(uint32_t b) override { m_tape.positionToLogicalObject(b); }

  positionInfo getPositionInfo() override { return m_tape.getPositionInfo(); }

  physicalPositionInfo getPhysicalPositionInfo() override { return m_tape.getPhysicalPositionInfo(); }

  std::vector<endOfWrapPosition> getEndOfWrapPositions() override;
  void enableCRC32CLogicalBlockProtectionReadOnly() override;
  void enableCRC32CLogicalBlockProtectionReadWrite() override;
  void disableLogicalBlockProtection() override;

  void fastSpaceToEOM(void) override { m_tape.fastSpaceToEOM(); }

  void rewind(void) override { m_tape.rewind(); }

  void spaceToEOM(void) override { m_tape.spaceToEOM(); }

  void spaceFileMarksBackwards(size_t count) override { m_tape.spaceFileMarksBackwards(count); }

  void spaceFileMarksForward(size_t count) override { m_tape.spaceFileMarksForward(count); }

  void flush(void) override { m_tape.flush(); }

  void writeSyncFileMarks(size_t count) override { m_tape.writeSyncFileMarks(count); }

  void writeImmediateFileMarks(size_t count) override { m_tape.writeImmediateFileMarks(count); }

  ssize_t readBlock(void* data, size_t count) override { return m_tape.readBlock(data, count); }

  void readExactBlock(void* data, size_t count, const std::string& ctx) override {
    m_tape.readExactBlock(data, count, ctx);
  }

  void readFileMark(const std::string& ctx) override { m_tape.readFileMark(ctx); }

  bool isWriteProtected() override;

  bool isAtBOT() override { return m_tape.isAtBOT(); }

  bool isAtEOD() override { return m_tape.isAtEOD(); }

  bool isTapeBlank() override { return m_tape.isTapeBlank(); }

  lbpToUse getLbpToUse() override;

  bool hasTapeInPlace() override { return m_tape.isLoaded(); }

  // --- Drive-metadata and SCSI stubs ---
  std::string getGenericSCSIPath() override { return ""; }

  std::string getDriveFirmwareVersion() override { return "0.0"; }

  std::map<std::string, uint64_t> getTapeWriteErrors() override {
    return {
      {"mountTotalCorrectedWriteErrors",   0},
      {"mountTotalWriteBytesProcessed",    0},
      {"mountTotalUncorrectedWriteErrors", 0}
    };
  }

  std::map<std::string, uint64_t> getTapeReadErrors() override {
    return {
      {"mountTotalCorrectedReadErrors",   0},
      {"mountTotalReadBytesProcessed",    0},
      {"mountTotalUncorrectedReadErrors", 0}
    };
  }

  std::map<std::string, uint32_t> getTapeNonMediumErrors() override {
    return {
      {"mountTotalNonMediumErrorCounts", 0}
    };
  }

  std::map<std::string, float> getQualityStats() override {
    return {
      {"lifetimeMediumEfficiencyPrct", 100.0f},
      {"mountReadEfficiencyPrct",      100.0f},
      {"mountWriteEfficiencyPrct",     100.0f}
    };
  }

  std::map<std::string, uint32_t> getDriveStats() override {
    return {
      {"mountTemps",             0},
      {"mountReadTransients",    0},
      {"mountWriteTransients",   0},
      {"mountTotalReadRetries",  0},
      {"mountTotalWriteRetries", 0},
      {"mountServoTemps",        0},
      {"mountServoTransients",   0}
    };
  }

  std::map<std::string, uint32_t> getVolumeStats() override { return {}; }

  std::vector<uint16_t> getTapeAlertCodes() override { return {}; }

  std::vector<std::string> getTapeAlerts(const std::vector<uint16_t>&) override { return {}; }

  std::vector<std::string> getTapeAlertsCompact(const std::vector<uint16_t>&) override { return {}; }

  bool tapeAlertsCriticalForWrite(const std::vector<uint16_t>&) override { return false; }

  cta::tape::SCSI::Structures::RAO::udsLimits getLimitUDS() override;

  void queryRAO(std::list<SCSI::Structures::RAO::blockLims>& files, int) override { files.reverse(); }

  // Per-media-type properties derived from library.toml at construction time.
  struct MediaTypeInfo {
    uint64_t capacityBytes = std::numeric_limits<uint64_t>::max();
    uint32_t mountDelayMs = 0;
    // Adaptive-throttle ceiling in files/sec (HDR1 blocks per second).
    // 0 = disabled (write at RAM speed).
    uint32_t maxWriteRateHz = 0;
  };

private:
  const std::string m_driveName;
  const std::string m_logicalLibrary;
  const std::filesystem::path m_baseDir;

  InMemoryTape m_tape;
  lbpToUse m_lbpToUse = lbpToUse::disabled;

  // Maps VID prefix (e.g. "ST") → MediaTypeInfo for that media type.
  // Populated by parseLibraryConf() at construction.
  std::map<std::string, MediaTypeInfo> m_mediaTypes;

  // Full path to the *.tape file in drives/<driveName>/ after waitUntilReady().
  // Empty when no tape is loaded in the current session.
  std::filesystem::path m_currentTapePath;

  // Adaptive rate state (set from MediaTypeInfo at each waitUntilReady call).
  uint32_t m_maxWriteRateHz = 0;
  // Default-constructed time_point (epoch) signals "no write yet this session".
  std::chrono::steady_clock::time_point m_lastWriteBlockTime;

  // Per-drive RNG for jittering mount delay; seeded from drive name XOR current time.
  std::mt19937 m_rng;

  // Magic word and format version written at the beginning of each .tape file.
  static constexpr uint32_t k_magic = 0x43544150;  // 'CTAP'
  static constexpr uint32_t k_version = 1;

  // Serialise m_tape to m_currentTapePath via an atomic tmp+rename.
  void saveTape() const;

  // Deserialise from tapePath into m_tape.  Clears m_tape first.
  // An empty file leaves m_tape empty (blank tape).
  void loadTape(const std::filesystem::path& tapePath);

  // Parse baseDir/config/<lib>/library.toml.  Populates m_mediaTypes.
  // Throws if the file is missing or malformed.
  void parseLibraryConf();

  // Look up MediaTypeInfo for a given VID by longest-prefix match.
  // Returns a default-constructed MediaTypeInfo (unlimited capacity, no delay)
  // if no prefix matches.
  MediaTypeInfo infoForVid(const std::string& vid) const;
};

}  // namespace cta::tape::drive

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
 *   3. The full transfer runs via m_tape in-memory block I/O. flush() is an in-memory no-op by
 *      default — the file on disk is stale until step 4 — unless the media type's
 *      persistOnFlush is set, in which case flush() also serialises m_tape to disk
 *      immediately (see MediaTypeInfo::persistOnFlush). If the media type's
 *      transferFailureRate/retrieveFailureRate is set, each file's first HDR1 block may throw a
 *      synthetic I/O error instead of transferring (see MediaTypeInfo::transferFailureRate and
 *      maybeInjectFault()), for failure-scenario testing.
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
   *   1. Sleeps a uniformly random duration in [mountDelayMs, mountDelayMaxMs] (default
   *      [mountDelayMs, 2×mountDelayMs] when mountDelayMaxMs is unset) so every drive has a
   *      guaranteed minimum delay while concurrent mounts are spread across a window,
   *      preventing synchronised PVC I/O storms.
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

  ssize_t readBlock(void* data, size_t count) override;

  /**
   * Counts the bytes read and applies the read-side adaptive-mirror throttle, which sleeps once
   * per file for max(minInterval, elapsed) with minInterval = 1s/maxReadRateHz. The label blocks
   * are read through here, so the file's HDR1 is the per-file marker, as it is for writes; the
   * sleep lands just after it rather than just before. No sleep when maxReadRateHz == 0.
   */
  void readExactBlock(void* data, size_t count, const std::string& ctx) override;

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

  /**
   * In-memory flush, plus an eager saveTape() when the current cartridge's media type has
   * persistOnFlush enabled (default off — see MediaTypeInfo::persistOnFlush).
   */
  void flush(void) override;

  void writeSyncFileMarks(size_t count) override { m_tape.writeSyncFileMarks(count); }

  void writeImmediateFileMarks(size_t count) override { m_tape.writeImmediateFileMarks(count); }

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

  // The statistics below are the same maps a real drive builds from its SCSI log pages, so that
  // the session layer's logSCSIMetrics() reports the same keys for a virtual drive. The byte
  // counters are measured, counted per mount exactly like the drive's own log pages: they are
  // reset by waitUntilReady() when a cartridge is loaded. The error, retry and temperature
  // counters are genuinely zero: an in-memory tape has no medium to fail, and reporting anything
  // else would invent failures the test never had.
  std::map<std::string, uint64_t> getTapeWriteErrors() override {
    return {
      {"mountTotalCorrectedWriteErrors",   0                  },
      {"mountTotalWriteBytesProcessed",    m_mountBytesWritten},
      {"mountTotalUncorrectedWriteErrors", 0                  }
    };
  }

  std::map<std::string, uint64_t> getTapeReadErrors() override {
    return {
      {"mountTotalCorrectedReadErrors",   0               },
      {"mountTotalReadBytesProcessed",    m_mountBytesRead},
      {"mountTotalUncorrectedReadErrors", 0               }
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
    // Upper bound of the mount-delay jitter window; 0 means "unset", which keeps the original
    // behaviour of defaulting to 2 x mountDelayMs. Set explicitly to widen the spread without
    // also raising the floor (see waitUntilReady()).
    uint32_t mountDelayMaxMs = 0;
    // Adaptive-throttle ceilings in files/sec (one HDR1 block per file).
    // 0 = disabled (transfer at RAM speed).
    uint32_t maxWriteRateHz = 0;
    uint32_t maxReadRateHz = 0;
    // Off by default: the tape is deliberately in-memory-only until unloadTape() calls
    // saveTape(), so a session never pays disk I/O mid-transfer. Set true to make flush()
    // durable immediately (see flush() below) — e.g. for a run where losing in-flight data to
    // a pod restart or an unload-time saveTape() failure is unacceptable even at the cost of
    // the extra PVC I/O.
    bool persistOnFlush = false;
    // Fault injection for failure-scenario testing, off by default (0.0). On the first HDR1
    // block of a write/read, with probability transferFailureRate/retrieveFailureRate (a stable
    // function of the file's archive-file-ID, not re-rolled per attempt), VtlDrive throws a
    // synthetic I/O error instead of transferring the file — see tryClaimFaultOnce() for why this
    // is guaranteed to affect a given file at most once across its lifetime, however many drives
    // or retries it passes through. 0.0 (unset) disables injection entirely; the normal case for
    // every deployment and test except the dedicated failure-scenario preset.
    double transferFailureRate = 0.0;
    double retrieveFailureRate = 0.0;
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

  // Bytes written to and read from the cartridge since it was loaded, reported through
  // getTapeWriteErrors()/getTapeReadErrors() as a real drive reports its SCSI log-page counters.
  // Reset by waitUntilReady() for each newly loaded cartridge.
  uint64_t m_mountBytesWritten = 0;
  uint64_t m_mountBytesRead = 0;

  // Adaptive rate state (set from MediaTypeInfo at each waitUntilReady call).
  uint32_t m_maxWriteRateHz = 0;
  uint32_t m_maxReadRateHz = 0;
  // Set from MediaTypeInfo::persistOnFlush at each waitUntilReady call; see flush().
  bool m_persistOnFlush = false;
  // Set from MediaTypeInfo::transferFailureRate/retrieveFailureRate at each waitUntilReady call;
  // see maybeInjectFault().
  double m_transferFailureRate = 0.0;
  double m_retrieveFailureRate = 0.0;
  // Default-constructed time_point (epoch) signals "no file transferred yet on this cartridge".
  // Both are reset when a cartridge is loaded, so the tape change is not mistaken for queue wait.
  std::chrono::steady_clock::time_point m_lastWriteBlockTime;
  std::chrono::steady_clock::time_point m_lastReadBlockTime;

  /// True when the block is the HDR1 label, i.e. the first block of a file: the per-file marker
  /// both throttles key off.
  static bool isFileHeader(const void* data, size_t count);

  /// Adaptive-mirror throttle shared by the read and write paths. The per-file interval is
  /// 1/rateHz seconds; the throttle sleeps whatever is left of that interval since the previous
  /// file and then sets last to the time the sleep ended, so that elapsed measures only the wait
  /// between transfers and never the sleep itself. Per-file spacing is therefore
  /// max(arrival gap, interval): drive-limited (elapsed < interval) caps the rate at rateHz,
  /// queue-limited (elapsed >= interval) passes work through at the rate it arrives, and a stall
  /// costs exactly its own duration. No interval debt is carried, so recovery never produces a
  /// catch-up burst. No-op when rateHz is 0 or no file has been transferred yet on this cartridge.
  static void throttle(uint32_t rateHz, std::chrono::steady_clock::time_point& last);

  /// If data/count is an HDR1 block and rate > 0, decide whether this file is fault-eligible (a
  /// stable function of the fileId hex-encoded in the block, see FileWriter.cpp/Structures.hpp —
  /// NOT re-rolled per attempt) and, if so, atomically claim its one-time fault via
  /// tryClaimFaultOnce(). Throws a synthetic cta::exception::Exception if the claim succeeds (this
  /// is the first attempt, on any drive, to reach this file); does nothing otherwise — including
  /// when the file is eligible but another attempt already claimed its fault, which is what
  /// bounds the injected failures to AT MOST ONE per file for its entire lifetime regardless of
  /// how many times it is retried or which drive handles the retry.
  void maybeInjectFault(const void* data, size_t count, double rate, const char* kind) const;

  /// Parse the fileId field (hex-encoded cta::ArchiveFile::archiveFileID, see
  /// taped/file/FileWriter.cpp) out of an HDR1 block. Returns empty if data/count is not a valid
  /// HDR1 block or the field cannot be parsed.
  static std::string extractFileIdFromHDR1(const void* data, size_t count);

  /// Atomically claim fileIdHex's one-time fault of the given kind ("write" or "read") on the
  /// shared library volume, using the same O_CREAT|O_EXCL idiom as ChangerGuard's .lock file.
  /// Returns true if this call created the marker (first claim — inject the fault), false if it
  /// already existed (already claimed by an earlier attempt — do not inject again). The marker is
  /// permanent for the life of the PVC/test run: deliberately never cleaned up, since its entire
  /// purpose is "has this file ever had its one fault consumed," which must survive across mounts,
  /// drives and taped pod restarts.
  bool tryClaimFaultOnce(const char* kind, const std::string& fileIdHex) const;

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

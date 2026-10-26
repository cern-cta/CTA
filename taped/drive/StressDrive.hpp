/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "VirtualDrive.hpp"

#include <chrono>
#include <filesystem>
#include <limits>
#include <map>
#include <random>
#include <string>

namespace cta::tape::drive {

/**
 * Stress-test tape drive: a VirtualDrive whose tape content is persisted to a
 * file on a shared PVC between mount/dismount cycles.  This allows multiple
 * drives — in the same taped process or in different taped processes on the
 * same node — to share virtual tapes by VID, exactly as a physical tape library
 * makes one tape accessible from any drive slot.
 *
 * Unlike mhVTL (which requires a kernel module and SCSI target infrastructure),
 * StressDrive is entirely in-process: no kernel modules, no SCSI I/O, no node
 * configuration beyond mounting the shared PVC.  Any number of drives can run
 * on any Kubernetes node.
 *
 * Directory layout (all under baseDir, e.g. /cta-stress-lib-0):
 *
 *   config/<lib>/library.toml             — library name, changer timeout,
 *                                           and per-media-type properties
 *                                           (vidPrefix, capacityBytes,
 *                                            mountDelayMs, maxWriteRateHz)
 *   contents/<lib>/slots/VID.tape         — tape at rest (empty file = blank)
 *   contents/<lib>/changer/VID.tape       — tape in transit (robot holds it)
 *   contents/<lib>/changer/.lock          — robot-busy flag (O_CREAT|O_EXCL)
 *   contents/<lib>/drives/<drv>/          — empty directory = no tape loaded
 *   contents/<lib>/drives/<drv>/VID.tape  — tape mounted in this drive
 *
 * Mount/dismount lifecycle (one tape at a time enforced by .lock):
 *   1. MediaChangerFacade::mountTapeReadWrite(vid) acquires the changer lock
 *      and renames VID.tape: slots/ → changer/ → drives/<driveName>/.
 *   2. DataTransferSession calls waitUntilReady(): StressDrive scans
 *      drives/<driveName>/ for a *.tape file, derives the VID from its stem,
 *      looks up capacityBytes and mountDelayMs from library.toml, sleeps a
 *      jittered delay, then deserialises the binary block stream from the file
 *      into m_tape (VirtualDrive's in-memory block vector).
 *      An empty file (blank tape) triggers synthesis of a VOL1 label.
 *   3. The full transfer runs in-memory via VirtualDrive's block I/O.
 *   4. DataTransferSession calls unloadTape(): StressDrive serialises m_tape
 *      back to the *.tape file (atomic tmp+rename), then marks tape absent.
 *   5. MediaChangerFacade::dismountTape(vid) acquires the changer lock and
 *      renames VID.tape: drives/<driveName>/ → changer/ → slots/.
 *
 * The drive is registered in the CTA catalogue with
 * DriveDevice = "virtual://<base_dir>" (e.g. "virtual:///cta-stress-lib-0").
 * DataTransferSession::findDrive() detects the "virtual://" prefix and
 * constructs a StressDrive; the path component is the library root.
 */
class StressDrive : public VirtualDrive {
public:
  /**
   * @param driveName          Drive name as registered in the CTA catalogue
   *                           (e.g. "STRESS0003").
   * @param logicalLibraryName Logical library this drive belongs to; used to
   *                           construct all paths under baseDir.
   * @param baseDir            Root of the stress-test shared volume
   *                           (e.g. /dev/shm/cta-stress).
   *
   * The constructor reads baseDir/config/<logicalLibraryName>/library.toml to
   * obtain the mount-delay for this drive and the VID-prefix → capacity map
   * for all media types in the library.
   */
  StressDrive(std::string driveName, std::string logicalLibraryName, std::filesystem::path baseDir);
  ~StressDrive() override = default;

  /**
   * Simulates tape loading:
   *   1. Sleeps a uniformly random duration in [mountDelayMs, 2×mountDelayMs]
   *      so every drive has a guaranteed minimum delay while concurrent mounts
   *      are spread across a window, preventing synchronised PVC I/O storms.
   *   2. Scans contents/<lib>/drives/<driveName>/ for a *.tape file.
   *   3. Derives VID from filename stem; looks up tape capacity from library.toml.
   *   4. Loads the binary block stream from the file into m_tape.  An empty
   *      file (new blank tape) synthesises a VOL1 label in m_tape.
   *   5. Resets m_currentPosition to 0 (BOT).
   */
  void waitUntilReady(uint32_t timeoutSecond) override;

  /**
   * Serialises the current m_tape state back to the *.tape file in the drive
   * directory, then delegates to FakeDrive::unloadTape() to mark the tape as
   * absent.  Saving is skipped if no tape was ever loaded in this session.
   */
  void unloadTape() override;

  // The following five methods throw NotImplementedException in VirtualDrive but
  // are called by taped during normal transfer sessions.  Stress overrides
  // return sensible no-op values so the rest of the session logic proceeds.
  driveStatus getDriveStatus() override;
  void setDensityAndCompression(bool compression = true, unsigned char densityCode = 0) override;
  void setSTBufferWrite(bool bufWrite) override;
  drive::LBPInfo getLBPInfo() override;
  std::string getSerialNumber() override;
  deviceInfo getDeviceInfo() override;

  /**
   * Adaptive-mirror throttle: sleeps once per file at HDR1 for
   * max(minInterval, elapsed), where minInterval = 1s/maxWriteRateHz.
   * No sleep when maxWriteRateHz == 0.
   */
  void writeBlock(const void* data, size_t count) override;

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

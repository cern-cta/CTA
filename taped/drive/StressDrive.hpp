/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "FakeDrive.hpp"

#include <filesystem>
#include <string>

namespace cta::tape::drive {

/**
 * Stress-test tape drive: a FakeDrive whose tape content is persisted to a
 * file on a shared tmpfs between mount/dismount cycles.  This allows multiple
 * drives — in the same taped process or in different taped processes on the
 * same host — to share virtual tapes by VID, exactly as a physical tape library
 * makes one tape accessible from any drive slot.
 *
 * Directory layout (all under stressBaseDir, e.g. /dev/shm/cta-stress):
 *
 *   tapes/{VID}/tape.bin       -- serialised tape block stream (written on
 *                                 dismount, read on mount)
 *   drives/{driveName}         -- symlink → ../../tapes/{VID}/
 *                                 created by NullMediaChangerFacade::mountTape*
 *                                 removed by NullMediaChangerFacade::dismountTape
 *
 * Lifecycle:
 *   1. NullMediaChangerFacade::mountTapeReadWrite(vid) creates the symlink.
 *   2. DataTransferSession calls waitUntilReady(): StressDrive resolves the
 *      symlink to find the VID directory, loads tape.bin into m_tape (the
 *      inherited FakeDrive in-memory block vector).  A missing tape.bin means
 *      a blank tape (first archive on this VID).
 *   3. The full transfer runs via FakeDrive's in-memory block I/O — no
 *      filesystem access during data transfer.
 *   4. DataTransferSession calls unloadTape(): StressDrive serialises m_tape
 *      back to tape.bin, then delegates to FakeDrive::unloadTape().
 *   5. NullMediaChangerFacade::dismountTape(vid) removes the symlink.
 *
 * The drive is registered in the CTA catalogue with devFilename = "stress://"
 * which DataTransferSession::findDrive() recognises as the trigger to create
 * a StressDrive instead of opening a real SCSI device.
 */
class StressDrive : public FakeDrive {
public:
  /**
   * @param driveName     Drive name as registered in the CTA catalogue
   *                      (e.g. "STRESS0003").  Used to resolve the
   *                      drives/{driveName} symlink at mount time.
   * @param baseDir       Root of the stress-test tmpfs tree
   *                      (e.g. /dev/shm/cta-stress).
   * @param mountDelayMs  Milliseconds to sleep in waitUntilReady() to
   *                      simulate realistic tape-load latency.  0 = no delay.
   */
  StressDrive(std::string driveName, std::filesystem::path baseDir, uint32_t mountDelayMs);
  ~StressDrive() override = default;

  /**
   * Simulates tape loading:
   *   1. Sleeps mountDelayMs.
   *   2. Resolves drives/{driveName} symlink to find the tape directory.
   *   3. If tape.bin exists, deserialises it into m_tape (the FakeDrive block
   *      vector) so the tape content is available for the transfer.  If
   *      tape.bin is absent a synthetic VOL1 label is written to m_tape so
   *      that WriteSession does not reject it as a blank tape.
   *   4. Resets m_currentPosition to 0 (BOT).
   */
  void waitUntilReady(uint32_t timeoutSecond) override;

  /**
   * Serialises the current m_tape state to tape.bin in the active tape
   * directory, then delegates to FakeDrive::unloadTape() to mark the tape as
   * absent.  Saving is skipped if no tape was ever loaded (e.g. the session
   * aborted before waitUntilReady was called).
   */
  void unloadTape() override;

  // The following five methods throw NotImplementedException in FakeDrive but
  // are called by taped during normal transfer sessions.  Stress overrides
  // return sensible no-op values so the rest of the session logic proceeds.
  driveStatus getDriveStatus() override;
  void setDensityAndCompression(bool compression = true, unsigned char densityCode = 0) override;
  void setSTBufferWrite(bool bufWrite) override;
  drive::LBPInfo getLBPInfo() override;
  std::string getSerialNumber() override;

private:
  const std::string m_driveName;
  const std::filesystem::path m_baseDir;
  const uint32_t m_mountDelayMs;

  // Tape directory resolved from drives/{driveName} during waitUntilReady().
  // Empty string when no tape has been loaded in this session.
  std::filesystem::path m_currentTapeDir;

  // Magic word and format version written at the beginning of tape.bin to
  // detect truncated or mismatched files.
  static constexpr uint32_t k_magic = 0x43544150;   // 'CTAP'
  static constexpr uint32_t k_version = 1;

  // Serialise m_tape to m_currentTapeDir/tape.bin.
  void saveTape() const;

  // Deserialise tape.bin from tapeDir into m_tape.  Clears m_tape first;
  // if tape.bin is absent, m_tape is left empty (blank tape).
  void loadTape(const std::filesystem::path& tapeDir);
};

}  // namespace cta::tape::drive

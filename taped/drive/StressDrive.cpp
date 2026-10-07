/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "StressDrive.hpp"

#include "common/exception/Exception.hpp"

#include <cstring>
#include <fstream>
#include <thread>

namespace cta::tape::drive {

// Binary format for tape.bin:
//
//   [uint32_t magic   ]  -- k_magic, sanity check
//   [uint32_t version ]  -- k_version
//   for each block:
//     [uint64_t dataSize           ]  -- 0 for a file mark, >0 for a data block
//     [char     data[dataSize]     ]  -- absent for file marks
//     [uint64_t remainingSpaceAfter]
//
// The format is little-endian on all platforms where CTA runs (x86-64).

StressDrive::StressDrive(std::string driveName, std::filesystem::path baseDir, uint32_t mountDelayMs)
    : FakeDrive(std::numeric_limits<uint64_t>::max()),
      m_driveName(std::move(driveName)),
      m_baseDir(std::move(baseDir)),
      m_mountDelayMs(mountDelayMs) {
  // FakeDrive defaults m_tapeInPlace to true (for unit-test convenience), but
  // a StressDrive starts with an empty slot: no tape is loaded until the media
  // changer mounts one and waitUntilReady() is called.
  setTapeInPlace(false);
}

void StressDrive::waitUntilReady(uint32_t /*timeoutSecond*/) {
  if (m_mountDelayMs > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(m_mountDelayMs));
  }

  const auto symlinkPath = m_baseDir / "drives" / m_driveName;
  if (!std::filesystem::is_symlink(symlinkPath)) {
    throw cta::exception::Exception(
      "StressDrive::waitUntilReady: no tape mounted on drive \"" + m_driveName +
      "\" — symlink " + symlinkPath.string() + " not found; "
      "NullMediaChangerFacade::mountTape* must be called before waitUntilReady");
  }

  // read_symlink returns the raw target stored in the symlink, which may be a
  // relative path.  canonical() resolves it against the symlink's parent dir.
  const auto tapeDir =
    std::filesystem::canonical(m_baseDir / "drives" / m_driveName);
  loadTape(tapeDir);
  m_currentTapeDir = tapeDir;
  m_currentPosition = 0;

  // Synthesize a VOL1 label on first use of a blank tape so that
  // WriteSession does not reject it as blank.  The VID is derived from
  // the tape directory name (e.g. /dev/shm/cta-stress/tapes/STRS0001 →
  // "STRS0001").
  //
  // We reproduce the 80-byte ANSI label inline rather than including
  // taped/file/Structures.hpp, which would add a ctatapedfile link
  // dependency to ctatapeddrive for this stress-only code path.
  //
  // VOL1 layout (80 bytes, space-initialised):
  //   [0..3]   label id    "VOL1"
  //   [4..9]   VSN         VID left-justified, space-padded to 6 chars
  //   [10]     accessibility  space
  //   [11..23] reserved1      spaces (13 bytes)
  //   [24..36] implID         spaces (13 bytes)
  //   [37..50] ownerID        "CTA" + spaces (14 bytes)
  //   [51..76] reserved2      spaces (26 bytes)
  //   [77..78] LBPMethod      "00"  (no LBP)
  //   [79]     lblStandard    "3"
  if (m_tape.empty()) {
    struct RawVOL1 {
      char label[4];
      char vsn[6];
      char accessibility[1];
      char reserved1[13];
      char implID[13];
      char ownerID[14];
      char reserved2[26];
      char lbpMethod[2];
      char lblStandard[1];
    };
    static_assert(sizeof(RawVOL1) == 80, "VOL1 must be exactly 80 bytes");

    RawVOL1 vol1;
    std::memset(&vol1, ' ', sizeof(vol1));
    std::memcpy(vol1.label, "VOL1", 4);
    std::memcpy(vol1.ownerID, "CTA", 3);
    std::memcpy(vol1.lbpMethod, "00", 2);
    vol1.lblStandard[0] = '3';

    const std::string vid = m_currentTapeDir.filename().string();
    const size_t vidLen = std::min(vid.size(), sizeof(vol1.vsn));
    std::memcpy(vol1.vsn, vid.data(), vidLen);

    tapeBlock block;
    block.data.assign(reinterpret_cast<const char*>(&vol1), sizeof(vol1));
    block.remainingSpaceAfter = m_tapeCapacity - sizeof(vol1);
    m_tape.push_back(std::move(block));
  }

  // Tape is now in the drive; reflect this so hasTapeInPlace() returns true
  // and getDriveStatus() reports the correct state to taped.
  setTapeInPlace(true);
}

void StressDrive::unloadTape() {
  if (!m_currentTapeDir.empty()) {
    saveTape();
    m_currentTapeDir.clear();
  }
  FakeDrive::unloadTape();
}

void StressDrive::saveTape() const {
  const auto tapePath = m_currentTapeDir / "tape.bin";
  std::ofstream f(tapePath, std::ios::binary | std::ios::trunc);
  if (!f) {
    throw cta::exception::Exception(
      "StressDrive::saveTape: cannot open " + tapePath.string() + " for writing");
  }

  const auto write32 = [&](uint32_t v) {
    f.write(reinterpret_cast<const char*>(&v), sizeof(v));
  };
  const auto write64 = [&](uint64_t v) {
    f.write(reinterpret_cast<const char*>(&v), sizeof(v));
  };

  write32(k_magic);
  write32(k_version);

  for (const auto& block : m_tape) {
    const uint64_t sz = block.data.size();
    write64(sz);
    if (sz > 0) {
      f.write(block.data.data(), static_cast<std::streamsize>(sz));
    }
    write64(block.remainingSpaceAfter);
  }

  if (!f) {
    throw cta::exception::Exception(
      "StressDrive::saveTape: I/O error writing " + tapePath.string());
  }
}

void StressDrive::loadTape(const std::filesystem::path& tapeDir) {
  m_tape.clear();

  const auto tapePath = tapeDir / "tape.bin";
  if (!std::filesystem::exists(tapePath)) {
    // Blank tape: no prior archive sessions on this VID.
    return;
  }

  std::ifstream f(tapePath, std::ios::binary);
  if (!f) {
    throw cta::exception::Exception(
      "StressDrive::loadTape: cannot open " + tapePath.string() + " for reading");
  }

  const auto read32 = [&](uint32_t& v) {
    f.read(reinterpret_cast<char*>(&v), sizeof(v));
  };
  const auto read64 = [&](uint64_t& v) {
    f.read(reinterpret_cast<char*>(&v), sizeof(v));
  };

  uint32_t magic = 0, version = 0;
  read32(magic);
  read32(version);
  if (!f || magic != k_magic) {
    throw cta::exception::Exception(
      "StressDrive::loadTape: " + tapePath.string() +
      " has wrong magic (got " + std::to_string(magic) +
      ", expected " + std::to_string(k_magic) + ")");
  }
  if (version != k_version) {
    throw cta::exception::Exception(
      "StressDrive::loadTape: " + tapePath.string() +
      " has unsupported version " + std::to_string(version));
  }

  while (f.peek() != std::char_traits<char>::eof()) {
    tapeBlock block;
    uint64_t sz = 0;
    read64(sz);
    if (!f) {
      break;
    }
    if (sz > 0) {
      block.data.resize(sz);
      f.read(block.data.data(), static_cast<std::streamsize>(sz));
    }
    // sz == 0: file mark (empty data string), no data bytes to read
    read64(block.remainingSpaceAfter);
    if (!f) {
      throw cta::exception::Exception(
        "StressDrive::loadTape: truncated block in " + tapePath.string());
    }
    m_tape.push_back(std::move(block));
  }
}

cta::tape::drive::driveStatus StressDrive::getDriveStatus() {
  driveStatus st;
  st.ready = true;
  st.writeProtection = false;
  st.eod = false;
  st.bot = (m_currentPosition == 0);
  st.hasTapeInPlace = hasTapeInPlace();
  return st;
}

void StressDrive::setDensityAndCompression(bool /*compression*/, unsigned char /*densityCode*/) {
  // No physical density setting on a virtual tape; silently ignored.
}

void StressDrive::setSTBufferWrite(bool /*bufWrite*/) {
  // ST buffer control is a SCSI detail irrelevant to a virtual tape; ignored.
}

cta::tape::drive::LBPInfo StressDrive::getLBPInfo() {
  LBPInfo info;
  info.method = 0;
  info.methodLength = 0;
  info.enableLBPforRead = false;
  info.enableLBPforWrite = false;
  return info;
}

std::string StressDrive::getSerialNumber() {
  // Return a deterministic serial number derived from the drive name so that
  // each stress drive appears unique in logs and catalogue entries.
  return "STRESS-" + m_driveName;
}

}  // namespace cta::tape::drive

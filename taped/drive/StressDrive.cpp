/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "StressDrive.hpp"

#include "common/exception/Exception.hpp"

#include <cstring>
#include <fstream>
#include <map>
#include <thread>
#include <toml++/toml.hpp>

namespace cta::tape::drive {

// Binary format for .tape files:
//
//   [uint32_t magic   ]  -- k_magic, sanity check
//   [uint32_t version ]  -- k_version
//   for each block:
//     [uint64_t dataSize           ]  -- 0 for a file mark, >0 for a data block
//     [char     data[dataSize]     ]  -- absent for file marks
//     [uint64_t remainingSpaceAfter]
//
// The format is little-endian on all platforms where CTA runs (x86-64).
// An empty file (size 0) means a blank (never-archived) tape.

StressDrive::StressDrive(std::string driveName, std::string logicalLibraryName, std::filesystem::path baseDir)
    : VirtualDrive(std::numeric_limits<uint64_t>::max()),
      m_driveName(std::move(driveName)),
      m_logicalLibrary(std::move(logicalLibraryName)),
      m_baseDir(std::move(baseDir)) {
  // VirtualDrive defaults m_tapeInPlace to true (for convenience), but a
  // StressDrive starts with an empty slot: no tape is loaded until the media
  // changer mounts one and waitUntilReady() is called.
  m_tapeInPlace = false;
  // Pre-allocate for large stress-test tapes (~320 k files at 470 bytes each).
  m_tape.reserve(512'000);
  parseLibraryConf();
}

void StressDrive::waitUntilReady(uint32_t /*timeoutSecond*/) {
  // TapeCleaning::~TapeCleaning() calls waitUntilReady() after the write
  // session to prepare for dismount.  Guard against that re-entry: if a tape
  // is already loaded (m_currentTapePath is set), the drive is already ready
  // and reloading from disk would discard in-memory data not yet saved.
  if (!m_currentTapePath.empty()) {
    return;
  }

  const auto driveDir = m_baseDir / "contents" / m_logicalLibrary / "drives" / m_driveName;

  // Scan the drive directory for exactly one *.tape file.
  std::filesystem::path tapeFile;
  for (const auto& entry : std::filesystem::directory_iterator(driveDir)) {
    if (entry.path().extension() == ".tape") {
      if (!tapeFile.empty()) {
        throw cta::exception::Exception(
          "StressDrive::waitUntilReady: multiple .tape files in " + driveDir.string() +
          " — only one tape can be mounted at a time");
      }
      tapeFile = entry.path();
    }
  }
  if (tapeFile.empty()) {
    throw cta::exception::Exception(
      "StressDrive::waitUntilReady: no tape mounted on drive \"" + m_driveName +
      "\" — no *.tape file found in " + driveDir.string() +
      "; MediaChangerFacade::mountTape* must be called before waitUntilReady");
  }

  const std::string vid = tapeFile.stem().string();

  // Look up per-media-type properties from the VID prefix.
  const MediaTypeInfo info = infoForVid(vid);

  // Simulate mechanical tape-load latency (configurable per media type).
  if (info.mountDelayMs > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(info.mountDelayMs));
  }

  m_tapeCapacity       = info.capacityBytes;
  m_maxWriteRateHz     = info.maxWriteRateHz;
  m_lastWriteBlockTime = {};  // reset adaptive-rate state for this tape session

  loadTape(tapeFile);
  m_currentTapePath = tapeFile;
  m_currentPosition = 0;

  // Synthesize a VOL1 label on first use of a blank tape so that
  // WriteSession does not reject it as blank.  The VID is derived from the
  // .tape filename stem (e.g. /…/drives/STRESS0000/ST0001.tape → "ST0001").
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

    const size_t vidLen = std::min(vid.size(), sizeof(vol1.vsn));
    std::memcpy(vol1.vsn, vid.data(), vidLen);

    tapeBlock block;
    block.data.assign(reinterpret_cast<const char*>(&vol1), sizeof(vol1));
    block.remainingSpaceAfter = m_tapeCapacity - sizeof(vol1);
    m_tape.push_back(std::move(block));
  }

  setTapeInPlace(true);
}

void StressDrive::unloadTape() {
  if (!m_currentTapePath.empty()) {
    saveTape();
    m_currentTapePath.clear();
  }
  VirtualDrive::unloadTape();
}

void StressDrive::writeBlock(const void* data, size_t count) {
  // Sleep once per file, not once per block.  HDR1 is the first block of every
  // ANSI tape file, so detecting it here gives one sleep at the inter-file
  // boundary and none between the blocks that belong to the same file.
  if (m_maxWriteRateHz > 0 && count >= 4 && std::memcmp(data, "HDR1", 4) == 0) {
    const auto now = std::chrono::steady_clock::now();
    if (m_lastWriteBlockTime != std::chrono::steady_clock::time_point{}) {
      const auto elapsed = now - m_lastWriteBlockTime;
      const std::chrono::nanoseconds minInterval{1'000'000'000LL / m_maxWriteRateHz};
      // Queue-limited: elapsed > minInterval — sleep mirrors inter-file arrival gap
      // so the drive never appears idle on monitoring.
      // Drive-limited: elapsed < minInterval — sleep equals minInterval, capping
      // throughput at maxWriteRateHz files/sec.
      std::this_thread::sleep_for(std::max(minInterval, elapsed));
    }
    m_lastWriteBlockTime = now;
  }
  VirtualDrive::writeBlock(data, count);
}

void StressDrive::saveTape() const {
  const auto tmpPath = std::filesystem::path(m_currentTapePath.string() + ".tmp");

  {
    std::ofstream f(tmpPath, std::ios::binary | std::ios::trunc);
    if (!f) {
      throw cta::exception::Exception(
        "StressDrive::saveTape: cannot open " + tmpPath.string() + " for writing");
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
      std::filesystem::remove(tmpPath);
      throw cta::exception::Exception(
        "StressDrive::saveTape: I/O error writing " + tmpPath.string() +
        " (disk full? check shared volume space)");
    }
  }

  std::filesystem::rename(tmpPath, m_currentTapePath);
}

void StressDrive::loadTape(const std::filesystem::path& tapePath) {
  m_tape.clear();

  if (std::filesystem::file_size(tapePath) == 0) {
    // Empty file = blank tape (never archived).  Leave m_tape empty so that
    // waitUntilReady() synthesises the VOL1 label.
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
    read64(block.remainingSpaceAfter);
    if (!f) {
      throw cta::exception::Exception(
        "StressDrive::loadTape: truncated block in " + tapePath.string());
    }
    m_tape.push_back(std::move(block));
  }
}

void StressDrive::parseLibraryConf() {
  const auto confPath = m_baseDir / "config" / m_logicalLibrary / "library.toml";

  toml::table tbl;
  try {
    tbl = toml::parse_file(confPath.string());
  } catch (const toml::parse_error& e) {
    throw cta::exception::Exception(
      "StressDrive: failed to parse " + confPath.string() + ": " +
      std::string(e.description()));
  }

  const auto* mediaTypes = tbl["library"]["mediaTypes"].as_array();
  if (!mediaTypes) return;

  for (const auto& entry : *mediaTypes) {
    const auto* mt = entry.as_table();
    if (!mt) continue;
    const auto prefix = (*mt)["vidPrefix"].value<std::string>();
    if (!prefix) continue;
    MediaTypeInfo info;
    if (auto cap   = (*mt)["capacityBytes"].value<uint64_t>())  info.capacityBytes  = *cap;
    if (auto delay = (*mt)["mountDelayMs"].value<int64_t>())    info.mountDelayMs   = static_cast<uint32_t>(*delay);
    if (auto rate  = (*mt)["maxWriteRateHz"].value<int64_t>())  info.maxWriteRateHz = static_cast<uint32_t>(*rate);
    m_mediaTypes[*prefix] = info;
  }
}

StressDrive::MediaTypeInfo StressDrive::infoForVid(const std::string& vid) const {
  // Longest-prefix match so overlapping prefixes (e.g. "S" and "ST") resolve correctly.
  std::string bestPrefix;
  MediaTypeInfo best;
  for (const auto& [prefix, info] : m_mediaTypes) {
    if (vid.rfind(prefix, 0) == 0 && prefix.size() > bestPrefix.size()) {
      bestPrefix = prefix;
      best       = info;
    }
  }
  return best;
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

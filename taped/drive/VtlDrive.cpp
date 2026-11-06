/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "VtlDrive.hpp"

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

VtlDrive::VtlDrive(std::string driveName, std::string logicalLibraryName, std::filesystem::path baseDir)
    : m_driveName(std::move(driveName)),
      m_logicalLibrary(std::move(logicalLibraryName)),
      m_baseDir(std::move(baseDir)),
      m_tape(std::numeric_limits<uint64_t>::max()),
      m_rng(std::hash<std::string> {}(m_driveName)
            ^ static_cast<size_t>(std::chrono::steady_clock::now().time_since_epoch().count())) {
  // m_tape defaults to loaded=true (convenient for most drives), but a VtlDrive
  // starts with an empty slot: no tape is present until the media changer mounts
  // one and waitUntilReady() is called.
  m_tape.setLoaded(false);
  // Pre-allocate for large stress-test tapes (~320 k files at 470 bytes each).
  m_tape.reserve(512'000);
  parseLibraryConf();
}

void VtlDrive::waitUntilReady(uint32_t /*timeoutSecond*/) {
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
        throw cta::exception::Exception("VtlDrive::waitUntilReady: multiple .tape files in " + driveDir.string()
                                        + " — only one tape can be mounted at a time");
      }
      tapeFile = entry.path();
    }
  }
  if (tapeFile.empty()) {
    throw cta::exception::Exception("VtlDrive::waitUntilReady: no tape mounted on drive \"" + m_driveName
                                    + "\" — no *.tape file found in " + driveDir.string()
                                    + "; MediaChangerFacade::mountTape* must be called before waitUntilReady");
  }

  const std::string vid = tapeFile.stem().string();

  // Look up per-media-type properties from the VID prefix.
  const MediaTypeInfo info = infoForVid(vid);

  // Jitter sleep in [mountDelayMs, mountDelayMaxMs] spreads concurrent remounts across drives,
  // preventing synchronised PVC I/O bursts. mountDelayMaxMs defaults to 2x mountDelayMs when
  // unset (0), preserving the original spread for anyone not setting it explicitly. Widening the
  // window (without also raising the floor) only helps the no-progress detector: it is the
  // SIMULTANEOUS dead zones across many drives that risk tripping it, not one drive's own delay,
  // and a wider spread makes that overlap less likely, not more.
  if (info.mountDelayMs > 0) {
    const uint64_t maxDelay = info.mountDelayMaxMs > 0 ? info.mountDelayMaxMs : 2u * info.mountDelayMs;
    std::uniform_int_distribution<uint64_t> dist(info.mountDelayMs, maxDelay);
    std::this_thread::sleep_for(std::chrono::milliseconds(dist(m_rng)));
  }

  m_tape.setCapacity(info.capacityBytes);
  m_maxWriteRateHz = info.maxWriteRateHz;
  m_maxReadRateHz = info.maxReadRateHz;
  m_lastWriteBlockTime = {};  // reset adaptive-rate state for this tape session

  loadTape(tapeFile);
  m_currentTapePath = tapeFile;
  m_tape.resetPosition();

  // Synthesize a VOL1 label on a blank tape so WriteSession does not reject it.
  // Inlined to avoid a ctatapedfile link dependency on ctatapeddrive.
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
  if (m_tape.isTapeBlank()) {
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

    // writeBlock advances position; reset back to BOT so the caller reads
    // from position 0 as expected.
    m_tape.writeBlock(reinterpret_cast<const char*>(&vol1), sizeof(vol1));
    m_tape.resetPosition();
  }

  m_tape.setLoaded(true);
  // A real drive's log-page counters cover the current mount only; reset them with the cartridge.
  m_mountBytesWritten = 0;
  m_mountBytesRead = 0;
  // Reset the throttle clocks too: the gap the tape change leaves is not queue wait, and mirroring
  // it would make the first file of every cartridge pay the mount delay a second time.
  m_lastWriteBlockTime = {};
  m_lastReadBlockTime = {};
}

void VtlDrive::unloadTape() {
  if (!m_currentTapePath.empty()) {
    saveTape();
    m_currentTapePath.clear();
  }
  m_tape.unloadTape();
}

bool VtlDrive::isFileHeader(const void* data, size_t count) {
  return count >= 4 && std::memcmp(data, "HDR1", 4) == 0;
}

void VtlDrive::throttle(uint32_t rateHz, std::chrono::steady_clock::time_point& last) {
  if (rateHz == 0) {
    return;
  }
  if (last != std::chrono::steady_clock::time_point {}) {
    const auto elapsed = std::chrono::steady_clock::now() - last;
    const std::chrono::nanoseconds minInterval {1'000'000'000LL / rateHz};
    std::this_thread::sleep_for(std::max(minInterval, elapsed));
  }
  last = std::chrono::steady_clock::now();
}

void VtlDrive::writeBlock(const void* data, size_t count) {
  // One throttle per file: HDR1 is the first block the writer emits for a file.
  if (isFileHeader(data, count)) {
    throttle(m_maxWriteRateHz, m_lastWriteBlockTime);
  }
  m_tape.writeBlock(data, count);
  m_mountBytesWritten += count;
}

ssize_t VtlDrive::readBlock(void* data, size_t count) {
  const ssize_t read = m_tape.readBlock(data, count);
  if (read > 0) {
    m_mountBytesRead += static_cast<uint64_t>(read);
  }
  return read;
}

void VtlDrive::readExactBlock(void* data, size_t count, const std::string& ctx) {
  m_tape.readExactBlock(data, count, ctx);
  m_mountBytesRead += count;
  // One throttle per file, as on the write side. The content is only known once read, so the
  // sleep follows the header instead of preceding it, which makes no difference to the rate.
  if (isFileHeader(data, count)) {
    throttle(m_maxReadRateHz, m_lastReadBlockTime);
  }
}

void VtlDrive::saveTape() const {
  const auto tmpPath = std::filesystem::path(m_currentTapePath.string() + ".tmp");

  {
    std::ofstream f(tmpPath, std::ios::binary | std::ios::trunc);
    if (!f) {
      throw cta::exception::Exception("VtlDrive::saveTape: cannot open " + tmpPath.string() + " for writing");
    }

    const auto write32 = [&](uint32_t v) { f.write(reinterpret_cast<const char*>(&v), sizeof(v)); };
    const auto write64 = [&](uint64_t v) { f.write(reinterpret_cast<const char*>(&v), sizeof(v)); };

    write32(k_magic);
    write32(k_version);

    for (const auto& block : m_tape.blocks()) {
      const uint64_t sz = block.data.size();
      write64(sz);
      if (sz > 0) {
        f.write(block.data.data(), static_cast<std::streamsize>(sz));
      }
      write64(block.remainingSpaceAfter);
    }

    f.flush();
    if (!f) {
      std::filesystem::remove(tmpPath);
      throw cta::exception::Exception("VtlDrive::saveTape: I/O error writing " + tmpPath.string()
                                      + " (disk full? check shared volume space)");
    }
  }

  std::filesystem::rename(tmpPath, m_currentTapePath);
}

void VtlDrive::loadTape(const std::filesystem::path& tapePath) {
  if (std::filesystem::file_size(tapePath) == 0) {
    // Empty file = blank tape (never archived).  loadBlocks({}) leaves m_tape
    // blank so waitUntilReady() synthesises the VOL1 label.
    m_tape.loadBlocks({});
    return;
  }

  std::ifstream f(tapePath, std::ios::binary);
  if (!f) {
    throw cta::exception::Exception("VtlDrive::loadTape: cannot open " + tapePath.string() + " for reading");
  }

  uint32_t magic = 0, version = 0;
  f.read(reinterpret_cast<char*>(&magic), sizeof(magic));
  f.read(reinterpret_cast<char*>(&version), sizeof(version));
  if (!f || magic != k_magic) {
    throw cta::exception::Exception("VtlDrive::loadTape: " + tapePath.string() + " has wrong magic (got "
                                    + std::to_string(magic) + ", expected " + std::to_string(k_magic) + ")");
  }
  if (version != k_version) {
    throw cta::exception::Exception("VtlDrive::loadTape: " + tapePath.string() + " has unsupported version "
                                    + std::to_string(version));
  }

  std::vector<InMemoryTape::Block> blocks;
  while (f.peek() != std::char_traits<char>::eof()) {
    InMemoryTape::Block block;
    uint64_t sz = 0;
    f.read(reinterpret_cast<char*>(&sz), sizeof(sz));
    if (!f) {
      break;
    }
    if (sz > 0) {
      block.data.resize(sz);
      f.read(block.data.data(), static_cast<std::streamsize>(sz));
      if (!f) {
        throw cta::exception::Exception("VtlDrive::loadTape: truncated block in " + tapePath.string());
      }
    }
    f.read(reinterpret_cast<char*>(&block.remainingSpaceAfter), sizeof(block.remainingSpaceAfter));
    if (!f) {
      throw cta::exception::Exception("VtlDrive::loadTape: truncated block in " + tapePath.string());
    }
    blocks.push_back(std::move(block));
  }
  m_tape.loadBlocks(std::move(blocks));
}

void VtlDrive::parseLibraryConf() {
  const auto confPath = m_baseDir / "config" / m_logicalLibrary / "library.toml";

  toml::table tbl;
  try {
    tbl = toml::parse_file(confPath.string());
  } catch (const toml::parse_error& e) {
    throw cta::exception::Exception("VtlDrive: failed to parse " + confPath.string() + ": "
                                    + std::string(e.description()));
  }

  const auto* mediaTypes = tbl["library"]["mediaTypes"].as_array();
  if (!mediaTypes) {
    return;
  }

  for (const auto& entry : *mediaTypes) {
    const auto* mt = entry.as_table();
    if (!mt) {
      continue;
    }
    const auto prefix = (*mt)["vidPrefix"].value<std::string>();
    if (!prefix) {
      continue;
    }
    MediaTypeInfo info;
    if (auto cap = (*mt)["capacityBytes"].value<uint64_t>()) {
      info.capacityBytes = *cap;
    }
    if (auto delay = (*mt)["mountDelayMs"].value<int64_t>()) {
      info.mountDelayMs = static_cast<uint32_t>(*delay);
    }
    if (auto delayMax = (*mt)["mountDelayMaxMs"].value<int64_t>()) {
      info.mountDelayMaxMs = static_cast<uint32_t>(*delayMax);
    }
    if (auto rate = (*mt)["maxWriteRateHz"].value<int64_t>()) {
      info.maxWriteRateHz = static_cast<uint32_t>(*rate);
    }
    if (auto rate = (*mt)["maxReadRateHz"].value<int64_t>()) {
      info.maxReadRateHz = static_cast<uint32_t>(*rate);
    }
    m_mediaTypes[*prefix] = info;
  }
}

VtlDrive::MediaTypeInfo VtlDrive::infoForVid(const std::string& vid) const {
  // Longest-prefix match so overlapping prefixes (e.g. "S" and "ST") resolve correctly.
  std::string bestPrefix;
  MediaTypeInfo best;
  for (const auto& [prefix, info] : m_mediaTypes) {
    if (vid.rfind(prefix, 0) == 0 && prefix.size() > bestPrefix.size()) {
      bestPrefix = prefix;
      best = info;
    }
  }
  return best;
}

cta::tape::drive::driveStatus VtlDrive::getDriveStatus() {
  driveStatus st;
  st.ready = true;
  st.writeProtection = false;
  st.eod = false;
  st.bot = m_tape.isAtBOT();
  st.hasTapeInPlace = m_tape.isLoaded();
  return st;
}

std::string VtlDrive::getSerialNumber() {
  return "VTL-" + m_driveName;
}

cta::tape::drive::LBPInfo VtlDrive::getLBPInfo() {
  LBPInfo info;
  info.method = 0;
  info.methodLength = 0;
  info.enableLBPforRead = false;
  info.enableLBPforWrite = false;
  return info;
}

cta::tape::SCSI::Structures::RAO::udsLimits VtlDrive::getLimitUDS() {
  cta::tape::SCSI::Structures::RAO::udsLimits lims;
  lims.maxSize = 30000;
  lims.maxSupported = 30;
  return lims;
}

cta::tape::drive::deviceInfo VtlDrive::getDeviceInfo() {
  // UTL1::verifyCommon() checks serialNumber against the value written at archive
  // time, so it must be non-empty.  Capped at 12 chars to fit UHL1/UTL1 field
  // width without silent truncation.
  deviceInfo devInfo;
  devInfo.product = "VirtualDrive";
  devInfo.productRevisionLevel = "0.0";
  devInfo.vendor = "CTA";
  devInfo.isPIsupported = false;
  devInfo.serialNumber = m_driveName.substr(0, 12);
  return devInfo;
}

void VtlDrive::enableCRC32CLogicalBlockProtectionReadOnly() {
  m_lbpToUse = lbpToUse::crc32cReadOnly;
}

void VtlDrive::enableCRC32CLogicalBlockProtectionReadWrite() {
  m_lbpToUse = lbpToUse::crc32cReadWrite;
}

void VtlDrive::disableLogicalBlockProtection() {
  m_lbpToUse = lbpToUse::disabled;
}

lbpToUse VtlDrive::getLbpToUse() {
  return m_lbpToUse;
}

bool VtlDrive::isWriteProtected() {
  return false;
}

std::vector<endOfWrapPosition> VtlDrive::getEndOfWrapPositions() {
  return {
    {0, 208310, 0},
    {1, 416271, 0},
    {2, 624562, 0}
  };
}

}  // namespace cta::tape::drive

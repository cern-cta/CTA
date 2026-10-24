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
      m_baseDir(std::move(baseDir)),
      m_rng(std::hash<std::string> {}(m_driveName)
            ^ static_cast<size_t>(std::chrono::steady_clock::now().time_since_epoch().count())) {
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
        throw cta::exception::Exception("StressDrive::waitUntilReady: multiple .tape files in " + driveDir.string()
                                        + " — only one tape can be mounted at a time");
      }
      tapeFile = entry.path();
    }
  }
  if (tapeFile.empty()) {
    throw cta::exception::Exception("StressDrive::waitUntilReady: no tape mounted on drive \"" + m_driveName
                                    + "\" — no *.tape file found in " + driveDir.string()
                                    + "; MediaChangerFacade::mountTape* must be called before waitUntilReady");
  }

  const std::string vid = tapeFile.stem().string();

  // Look up per-media-type properties from the VID prefix.
  const MediaTypeInfo info = infoForVid(vid);

  // Simulate mechanical tape-load latency with per-drive jitter.
  // Per-drive unavailability: saveTape() [burst] → dismount/mount renames →
  //   waitUntilReady: sleep [mountDelayMs, 2×mountDelayMs] → loadTape() [spread].
  // Saves are a concurrent burst (N_drives × tape_on_disk bytes) before any jitter.
  // The jitter window spreads the subsequent loads across drives.
  // Total downtime per drive = save_time + jitter + load_time must stay below the
  // no-progress detector (6 × 10 s = 60 s).
  // Current (30 drives, ~39 MB on-disk, fast-ssd): ~2 s save + 15–30 s jitter
  //   + ~0.5 s load ≈ 17–33 s — well within the 60 s limit.
  // Multiple PVCs (one per tape pool) become necessary when save and load times
  // grow too large, e.g. ~66 MB tapes (k=5, 100 k files) on a network-SSD at
  // 200 MB/s: ~10 s save + 15–30 s jitter + ~10 s load ≈ 35–50 s — close to
  // the limit.  P PVCs reduce save+load times by factor P.
  if (info.mountDelayMs > 0) {
    std::uniform_int_distribution<uint64_t> dist(info.mountDelayMs, 2u * info.mountDelayMs);
    std::this_thread::sleep_for(std::chrono::milliseconds(dist(m_rng)));
  }

  m_tapeCapacity = info.capacityBytes;
  m_maxWriteRateHz = info.maxWriteRateHz;
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
  // Adaptive-mirror throttle — why it exists and how it works.
  //
  // WITHOUT throttling, StressDrive writes to its in-memory tape at RAM speed
  // (~GB/s).  At that rate one drive drains a 100 k-file scheduler batch in
  // milliseconds and immediately idles, waiting for the next pgsched fetch.
  // All 30 drives thundering-herd on the scheduler simultaneously, then all
  // going idle simultaneously, then repeating — this produces bursty metrics
  // with no useful signal on queue depth, drive utilisation or diskReport lag.
  // The scheduler is hammered with large fetch requests spaced by long idle
  // gaps instead of a steady trickle, which is also unrepresentative of real
  // tape throughput.
  //
  // WITH throttling (maxWriteRateHz > 0), each drive sleeps for
  //   max(minInterval, elapsed)
  // once at the HDR1 block of each file (the first block of every ANSI tape
  // file, so exactly one sleep per file, none between blocks of the same file).
  //
  //   minInterval = 1 s / maxWriteRateHz   (natural drive speed ceiling)
  //   elapsed     = wall time since the previous file's HDR1
  //
  // Queue-limited regime (elapsed > minInterval):
  //   The disk-read pipeline was slower than the drive — the tape thread spent
  //   most of elapsed waiting for the next EOS read to complete.  Sleep equals
  //   elapsed, so the drive's inter-file gap exactly mirrors the queue's
  //   inter-file arrival gap.  From monitoring the drive appears to be writing
  //   continuously at the queue drain rate — never idle, never ahead of supply.
  //   This is the normal operating regime during the 10 M-file stress run.
  //
  // Drive-limited regime (elapsed < minInterval):
  //   The pipeline delivered the file faster than the drive's natural speed.
  //   Sleep equals minInterval, capping throughput at maxWriteRateHz files/sec.
  //   This models the hard mechanical ceiling of a real tape drive.
  //
  // Natural throughput unit — files/sec:
  //   The stress test uses tiny fixed-size files (16 bytes payload).  The
  //   meaningful throughput unit here is files/sec, not bytes/sec — at 16 bytes
  //   per file a bytes/sec ceiling of 300 MB/s would give a per-file sleep of
  //   ~53 ns, which is below the OS timer granularity (~50–100 µs on Linux) and
  //   produces completely unpredictable rates.  Files/sec sidesteps this: a
  //   target of 100 files/s/drive gives a 10 ms minInterval, well within OS
  //   scheduling accuracy.
  //
  // Alternative: two-parameter throughput model (future extension):
  //   To simulate a realistic hardware ceiling in bytes/sec while keeping tiny
  //   payloads, two config fields could replace maxWriteRateHz:
  //
  //     maxWriteRateBytesPerSec  — drive ceiling (e.g. 314572800 = 300 MB/s)
  //     simulatedBlockSizeBytes  — effective tape block per file (e.g. 524288 = 512 KB)
  //
  //   The derived file rate is:
  //     maxWriteRateHz = maxWriteRateBytesPerSec / simulatedBlockSizeBytes
  //     → 300 MB/s / 512 KB ≈ 572 files/s, minInterval ≈ 1.75 ms — OS-accurate
  //
  //   simulatedBlockSizeBytes models the fact that on a real tape drive each
  //   file occupies at least one full block regardless of payload size.  The
  //   minimum block size for OS-accurate sleep is roughly:
  //     simulatedBlockSizeBytes_min = maxWriteRateBytesPerSec × 1 ms
  //     → 300 MB/s × 1 ms = 300 KB;  1 GB/s × 1 ms = 1 MB
  //   Below that floor the timer error dominates.  Until a use-case with
  //   meaningful bytes/sec semantics arises, files/sec is sufficient.
  //
  // Recovery when the queue transitions from slow to fast:
  //   In the queue-limited regime elapsed can grow large (e.g. 500 ms) if the
  //   disk-read pipeline stalls.  When the queue fills up and files start
  //   arriving fast again, recovery is immediate: elapsed on the very next file
  //   is tiny (pipeline delivered quickly) so sleep snaps back to minInterval.
  //   This works because the timestamp is captured AFTER the sleep — elapsed
  //   measures only the idle wait for the next file, not the sleep itself.
  //   If the timestamp were captured before the sleep, elapsed on the next
  //   iteration would include the previous sleep, causing the sleep to grow
  //   monotonically even after the queue recovers — the drive would never
  //   catch up.  Timestamp-after-sleep is what makes recovery self-correcting.
  if (m_maxWriteRateHz > 0 && count >= 4 && std::memcmp(data, "HDR1", 4) == 0) {
    if (m_lastWriteBlockTime != std::chrono::steady_clock::time_point {}) {
      const auto elapsed = std::chrono::steady_clock::now() - m_lastWriteBlockTime;
      const std::chrono::nanoseconds minInterval {1'000'000'000LL / m_maxWriteRateHz};
      std::this_thread::sleep_for(std::max(minInterval, elapsed));
    }
    m_lastWriteBlockTime = std::chrono::steady_clock::now();
  }
  VirtualDrive::writeBlock(data, count);
}

void StressDrive::saveTape() const {
  const auto tmpPath = std::filesystem::path(m_currentTapePath.string() + ".tmp");

  {
    std::ofstream f(tmpPath, std::ios::binary | std::ios::trunc);
    if (!f) {
      throw cta::exception::Exception("StressDrive::saveTape: cannot open " + tmpPath.string() + " for writing");
    }

    const auto write32 = [&](uint32_t v) { f.write(reinterpret_cast<const char*>(&v), sizeof(v)); };
    const auto write64 = [&](uint64_t v) { f.write(reinterpret_cast<const char*>(&v), sizeof(v)); };

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

    f.flush();
    if (!f) {
      std::filesystem::remove(tmpPath);
      throw cta::exception::Exception("StressDrive::saveTape: I/O error writing " + tmpPath.string()
                                      + " (disk full? check shared volume space)");
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
    throw cta::exception::Exception("StressDrive::loadTape: cannot open " + tapePath.string() + " for reading");
  }

  uint32_t magic = 0, version = 0;
  f.read(reinterpret_cast<char*>(&magic), sizeof(magic));
  f.read(reinterpret_cast<char*>(&version), sizeof(version));
  if (!f || magic != k_magic) {
    throw cta::exception::Exception("StressDrive::loadTape: " + tapePath.string() + " has wrong magic (got "
                                    + std::to_string(magic) + ", expected " + std::to_string(k_magic) + ")");
  }
  if (version != k_version) {
    throw cta::exception::Exception("StressDrive::loadTape: " + tapePath.string() + " has unsupported version "
                                    + std::to_string(version));
  }

  while (f.peek() != std::char_traits<char>::eof()) {
    tapeBlock block;
    uint64_t sz = 0;
    f.read(reinterpret_cast<char*>(&sz), sizeof(sz));
    if (!f) {
      break;
    }
    if (sz > 0) {
      block.data.resize(sz);
      f.read(block.data.data(), static_cast<std::streamsize>(sz));
      if (!f) {
        throw cta::exception::Exception("StressDrive::loadTape: truncated block in " + tapePath.string());
      }
    }
    f.read(reinterpret_cast<char*>(&block.remainingSpaceAfter), sizeof(block.remainingSpaceAfter));
    if (!f) {
      throw cta::exception::Exception("StressDrive::loadTape: truncated block in " + tapePath.string());
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
    throw cta::exception::Exception("StressDrive: failed to parse " + confPath.string() + ": "
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
    if (auto rate = (*mt)["maxWriteRateHz"].value<int64_t>()) {
      info.maxWriteRateHz = static_cast<uint32_t>(*rate);
    }
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
      best = info;
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

cta::tape::drive::deviceInfo StressDrive::getDeviceInfo() {
  // VirtualDrive::getDeviceInfo() leaves serialNumber empty, which causes
  // UTL1::verifyCommon() to throw "Failed verify for serialNumber" during
  // retrieve (the UTL1 trailer is written with the empty value at archive
  // time and then read back and verified at retrieve time).
  // m_serialNumber in UHL1UTL1 is 12 chars; use the drive name directly
  // (up to 12 chars) rather than the "STRESS-" + name form to stay within
  // the field width without silent truncation.
  deviceInfo devInfo = VirtualDrive::getDeviceInfo();
  devInfo.serialNumber = m_driveName.substr(0, 12);
  return devInfo;
}

}  // namespace cta::tape::drive

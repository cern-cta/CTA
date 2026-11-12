/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "VtlDrive.hpp"

#include "common/exception/Exception.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <syncstream>
#include <system_error>
#include <thread>
#include <toml++/toml.hpp>
#include <unistd.h>

namespace cta::tape::drive {

namespace {

// RAII guard serialising VtlDrive::saveTape() writes across every drive sharing one library's
// PVC (see CTA-1775): a lock file under contents/<lib>/changer/, same O_CREAT|O_EXCL idiom as
// mediachanger's ChangerGuard. Deliberately a SEPARATE lock file from that one's changer/.lock,
// not a reuse of it: ChangerGuard's timeout (changerTimeoutMs, default 30 s) is sized for an
// near-instant rename, not a save that can legitimately take minutes under concurrent PVC load —
// reusing it here would make healthy mounts/dismounts on OTHER drives throw spurious "changer
// lock timeout" errors while one drive is still saving.
struct WriteSerializationGuard {
  const std::filesystem::path lockPath;

  explicit WriteSerializationGuard(const std::filesystem::path& libraryContentsDir)
      : lockPath(libraryContentsDir / "changer" / ".write.lock") {
    constexpr auto kRetry = std::chrono::milliseconds(50);
    // A lock file older than this was almost certainly left by a holder that crashed or was
    // OOM-killed mid-save, not one still legitimately writing (saves have been observed taking
    // minutes under PVC contention, but nowhere near this long — and after this lock removes
    // that contention, an individual save's duration is just its own size/throughput, nothing
    // else). Stealing it is safe even if this heuristic is occasionally wrong: every drive
    // writes to its OWN uniquely-named tmp file (never a shared one), so a mistaken steal from a
    // merely-slow holder just re-creates one window of ordinary contention, not corruption.
    constexpr auto kStaleAfter = std::chrono::minutes(10);
    // Absolute ceiling across every steal attempt, in case something keeps re-creating and
    // abandoning the lock faster than this loop can recover (a real bug, not a one-off crash) —
    // give up for real rather than spin forever.
    constexpr auto kGiveUpAfter = std::chrono::minutes(30);

    const auto waitStart = std::chrono::steady_clock::now();
    for (;;) {
      int fd = ::open(lockPath.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644);
      if (fd >= 0) {
        ::close(fd);
        return;
      }
      if (errno != EEXIST) {
        throw cta::exception::Exception("VtlDrive: cannot acquire write-serialization lock at " + lockPath.string()
                                        + ": " + std::strerror(errno));
      }
      if (std::chrono::steady_clock::now() - waitStart >= kGiveUpAfter) {
        throw cta::exception::Exception(
          "VtlDrive: giving up after " + std::to_string(kGiveUpAfter.count())
          + " min waiting for write-serialization lock at " + lockPath.string()
          + " — repeated steal attempts did not clear it, something beyond a crashed holder is wrong");
      }
      std::error_code ec;
      const auto mtime = std::filesystem::last_write_time(lockPath, ec);
      if (!ec && std::filesystem::file_time_type::clock::now() - mtime >= kStaleAfter) {
        std::osyncstream(std::cerr) << "WARNING cta-taped VtlDrive stealing stale write-serialization lock at "
                                    << lockPath.string() << " (untouched for >= " << kStaleAfter.count()
                                    << " min — holder likely crashed or was OOM-killed)\n";
        std::filesystem::remove(lockPath, ec);  // ignore failure: another waiter may already have removed it
      }
      std::this_thread::sleep_for(kRetry);
    }
  }

  ~WriteSerializationGuard() noexcept {
    try {
      std::filesystem::remove(lockPath);
    } catch (...) {}
  }

  WriteSerializationGuard(const WriteSerializationGuard&) = delete;
  WriteSerializationGuard& operator=(const WriteSerializationGuard&) = delete;
};

// Serialises [magic+version, if includeHeader] + blocks[firstBlock..] into ONE contiguous
// buffer, so saveTape() can hand it to the OS as a single write() instead of several per block
// (size prefix, data, trailer) — on network-backed PVC storage classes, where every write()
// carries its own request/round-trip overhead regardless of size, that is thousands of tiny
// requests competing for the volume's IOPS ceiling instead of one big sequential one.
std::string serializeTapeChunk(bool includeHeader,
                               uint32_t magic,
                               uint32_t version,
                               const std::vector<cta::tape::drive::InMemoryTape::Block>& blocks,
                               size_t firstBlock) {
  size_t total = includeHeader ? sizeof(magic) + sizeof(version) : 0;
  for (size_t i = firstBlock; i < blocks.size(); ++i) {
    total += 2 * sizeof(uint64_t) + blocks[i].data.size();
  }
  std::string buf;
  buf.reserve(total);
  const auto append32 = [&](uint32_t v) { buf.append(reinterpret_cast<const char*>(&v), sizeof(v)); };
  const auto append64 = [&](uint64_t v) { buf.append(reinterpret_cast<const char*>(&v), sizeof(v)); };
  if (includeHeader) {
    append32(magic);
    append32(version);
  }
  for (size_t i = firstBlock; i < blocks.size(); ++i) {
    const auto& block = blocks[i];
    append64(block.data.size());
    if (!block.data.empty()) {
      buf.append(block.data.data(), block.data.size());
    }
    append64(block.remainingSpaceAfter);
  }
  return buf;
}

}  // namespace

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
  logStep("init: drive created, logicalLibrary=" + m_logicalLibrary + " baseDir=" + m_baseDir.string());
}

void VtlDrive::logStep(const std::string& msg) const {
  std::osyncstream(std::cerr) << "WARNING cta-taped VtlDrive drive=" << m_driveName << " lib=" << m_logicalLibrary
                              << " " << msg << '\n';
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
  logStep("mount: found tape vid=" + vid + " in " + driveDir.string());

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
    const uint64_t delayMs = dist(m_rng);
    logStep("mount: vid=" + vid + " sleeping " + std::to_string(delayMs) + " ms mount-delay jitter");
    std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
  }

  logStep("mount: vid=" + vid + " jitter done, reading tape from disk");
  m_tape.setCapacity(info.capacityBytes);
  m_maxWriteRateHz = info.maxWriteRateHz;
  m_maxReadRateHz = info.maxReadRateHz;
  m_persistOnFlush = info.persistOnFlush;
  m_transferFailureRate = info.transferFailureRate;
  m_retrieveFailureRate = info.retrieveFailureRate;
  m_lastWriteBlockTime = {};  // reset adaptive-rate state for this tape session

  loadTape(tapeFile);
  m_currentTapePath = tapeFile;
  m_tape.resetPosition();
  // Incremental-save bookkeeping (see saveTape()): whatever loadTape() just populated m_tape
  // with IS what is already durable on disk for this cartridge, header included unless the file
  // was blank. Must be set here, before the VOL1 synthesis below adds an in-memory-only block
  // that is not yet on disk.
  m_persistedBlockCount = m_tape.blocks().size();
  m_headerPersisted = !m_tape.isTapeBlank();

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
  logStep("mount: vid=" + vid + " ready, tape loaded");
}

void VtlDrive::unloadTape() {
  if (!m_currentTapePath.empty()) {
    logStep("unload: dismount requested for vid=" + m_currentTapePath.stem().string());
    saveTape();
    m_currentTapePath.clear();
  }
  m_tape.unloadTape();
}

void VtlDrive::flush(void) {
  m_tape.flush();
  // Skip entirely, lock included, ONLY when the block count is EXACTLY unchanged since the last
  // persist (e.g. the end-of-data flush right after a threshold flush already caught
  // everything) — saveTape() would otherwise still acquire the per-library lock for a
  // zero-byte append, needlessly contending with other drives for nothing. Deliberately not
  // "<=": a block count that went DOWN (the repositioning edge case saveTape() itself guards
  // against) must still reach saveTape() so its own full-rewrite fallback can fix the on-disk
  // file — skipping here on "<=" would hide that case from ever being corrected until
  // unloadTape() finally runs.
  if (m_persistOnFlush && !m_currentTapePath.empty() && m_tape.blocks().size() != m_persistedBlockCount) {
    logStep("flush: persistOnFlush set, eager-saving vid=" + m_currentTapePath.stem().string() + " mid-session");
    saveTape();
  }
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
    // Sleep only what is left of the interval. Sleeping max(minInterval, elapsed) instead made the
    // drive pay every supply stall twice: after an idle gap, elapsed is the whole gap, so a 90 s
    // wait for work cost another 90 s of sleep before the next file was written. Its average rate
    // was still correct - the overshoot on a late file ate into the next file's wait, a feedback
    // loop that holds only while work keeps arriving - but a stall is precisely where it does not.
    // Spacing files at max(arrival gap, minInterval) needs no such feedback.
    if (elapsed < minInterval) {
      std::this_thread::sleep_for(minInterval - elapsed);
    }
  }
  last = std::chrono::steady_clock::now();
}

std::string VtlDrive::extractFileIdFromHDR1(const void* data, size_t count) {
  // HDR1 layout (ECMA/ANSI label, see taped/file/Structures.hpp's HDR1EOF1 and
  // FileWriter.cpp's hdr1.fill(fileId, ...)): m_label[4] = "HDR1", then m_fileId[17] - the
  // ArchiveFile's archiveFileID, hex-encoded upper-case, right-padded with spaces. Parsed here by
  // fixed byte offset rather than including Structures.hpp, matching the existing precedent in
  // waitUntilReady() of inlining the VOL1 layout to avoid a ctatapedfile link dependency on
  // ctatapeddrive.
  constexpr size_t kFileIdOffset = 4;
  constexpr size_t kFileIdLen = 17;
  if (count < kFileIdOffset + kFileIdLen) {
    return {};
  }
  const char* fileIdField = static_cast<const char*>(data) + kFileIdOffset;
  std::string fileId(fileIdField, kFileIdLen);
  const auto lastNonSpace = fileId.find_last_not_of(' ');
  if (lastNonSpace == std::string::npos) {
    return {};
  }
  fileId.erase(lastNonSpace + 1);
  return fileId;
}

bool VtlDrive::tryClaimFaultOnce(const char* kind, const std::string& fileIdHex) const {
  // All library PVCs are mounted into every taped pod at deterministic paths generated by
  // create_instance.sh's extraVolumes loop (/cta-stress-lib-0 .. /cta-stress-lib-{N-1} - see the
  // comment there: "All library PVCs are mounted into every taped pod"). A single tape pool
  // shared across multiple libraries (the test's default setup) means a requeued job's retry can
  // be mounted on a different library's drive than its first attempt, so this marker must be
  // visible to every library's VtlDrive, not just this one's — using m_baseDir directly would
  // only bound the guarantee within one library. Library 0 always exists and is always mounted
  // in every pod, so its PVC serves as the shared location for every library's markers. (With a
  // future "one pool per library" setup, cross-library retries cannot happen at all and this
  // would be unnecessary, but remains correct — just a shared location that happens not to be
  // needed.)
  const std::string myDirName = m_baseDir.filename().string();
  const auto lastDash = myDirName.rfind('-');
  // Falls back to this drive's own baseDir if the "...-N" naming convention does not match,
  // rather than throwing: the guarantee then degrades to per-library instead of global, but
  // fault injection must never itself crash a session.
  const auto sharedRoot =
    lastDash == std::string::npos ? m_baseDir : m_baseDir.parent_path() / (myDirName.substr(0, lastDash + 1) + "0");

  const auto faultsDir = sharedRoot / "faults" / kind;
  std::error_code ec;
  std::filesystem::create_directories(faultsDir, ec);  // no-op if it already exists; races are harmless
  const auto markerPath = faultsDir / fileIdHex;
  // O_CREAT|O_EXCL is atomic on POSIX, same idiom as ChangerGuard's .lock file: exactly one
  // concurrent caller across every drive/library sees success. The marker is deliberately never
  // removed — its only purpose is "has this file's one fault already been spent," which must
  // hold for the file's entire lifetime, across every mount, drive and taped pod restart.
  const int fd = ::open(markerPath.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644);
  if (fd >= 0) {
    ::close(fd);
    return true;
  }
  return false;
}

void VtlDrive::maybeInjectFault(const void* data, size_t count, double rate, const char* kind) const {
  if (rate <= 0.0) {
    return;
  }
  const std::string fileIdHex = extractFileIdFromHDR1(data, count);
  if (fileIdHex.empty()) {
    return;
  }
  // Stable, deterministic eligibility: hashing the fileId (not re-rolled per attempt or drive)
  // fixes "is this file one of the unlucky ones" for the file's whole lifetime. Only WHETHER the
  // fault has already been consumed (tryClaimFaultOnce) can vary across attempts - never whether
  // the file is a candidate at all. This is what bounds injected failures to at most one per
  // file, however many times it is retried: the retry-neutral paths (ENOSPC, dead-mount recovery)
  // never touch the retry budget, so a file's 3 total attempts are never reduced by anything
  // except this one, single, already-consumed fault.
  const std::hash<std::string> hasher;
  const uint64_t h = hasher(fileIdHex);
  constexpr uint64_t kHashSpan = 1'000'000;
  const double bucket = static_cast<double>(h % kHashSpan) / static_cast<double>(kHashSpan);
  if (bucket >= rate) {
    return;
  }
  if (!tryClaimFaultOnce(kind, fileIdHex)) {
    return;
  }
  // RecoverableTransferFault (not plain cta::exception::Exception) on purpose: TapeWriteTask.cpp's
  // catch (const cta::exception::Exception&) sets m_errorFlag and rethrows specifically to end the
  // whole write session, which is correct for a genuine, session-wide I/O error but far too broad
  // a blast radius for failure-scenario testing of a single file. RecoverableTransferFault is
  // caught earlier, alongside RecoverableMigrationErrorException, and handled per-file: report
  // this one job failed and continue the mount for every other file. The read-side equivalent
  // needs no such distinction - TapeReadTask.hpp's existing cta::exception::Exception handler is
  // already per-file - so this type is thrown identically on both paths for symmetry, even though
  // only the write side's catch clause currently depends on which type it is.
  throw cta::tape::drive::RecoverableTransferFault(std::string("VtlDrive: injected ") + kind
                                                   + " fault for fileId=" + fileIdHex);
}

void VtlDrive::writeBlock(const void* data, size_t count) {
  // One throttle per file: HDR1 is the first block the writer emits for a file.
  if (isFileHeader(data, count)) {
    throttle(m_maxWriteRateHz, m_lastWriteBlockTime);
    // Checked before committing anything to m_tape, so an injected fault leaves no partial
    // trace of this attempt on the tape — the write simply never happened, same as a real
    // I/O error at the start of a file.
    maybeInjectFault(data, count, m_transferFailureRate, "write");
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
  // Fault injection follows the same constraint, for the same reason: unlike the write side,
  // there is no way to check the HDR1 content before the bytes are actually read off m_tape. This
  // is safe for retrieve: m_tape here is read-only ephemeral state scoped to one session, so the
  // position advance from this read vanishes with the session regardless of whether the caller
  // sees the data or an injected exception instead.
  if (isFileHeader(data, count)) {
    maybeInjectFault(data, count, m_retrieveFailureRate, "read");
    throttle(m_maxReadRateHz, m_lastReadBlockTime);
  }
}

void VtlDrive::saveTape() {
  const std::string vid = m_currentTapePath.stem().string();
  const auto& blocks = m_tape.blocks();

  logStep("save: vid=" + vid + " waiting for per-library write-serialization lock");
  const auto lockWaitStart = std::chrono::steady_clock::now();
  const WriteSerializationGuard writeGuard(m_baseDir / "contents" / m_logicalLibrary);
  const auto lockWaitMs =
    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - lockWaitStart).count();
  const auto saveStart = std::chrono::steady_clock::now();

  // blocks.size() can in principle shrink below m_persistedBlockCount: InMemoryTape::writeBlock()
  // does m_blocks.resize(m_currentPosition + 1), which DISCARDS everything past the current
  // position if the drive ever repositions backward and writes (real tape semantics). Archive
  // sessions write at EOD in practice, so this should not happen, but if it ever did, the blocks
  // already appended to disk from an earlier flush would be stale and incremental appending
  // would corrupt the file — fall back to a full rewrite in that case instead, which is always
  // correct regardless of how m_tape got to its current state.
  if (m_persistOnFlush && blocks.size() >= m_persistedBlockCount) {
    // Incremental: append only the blocks written since the last persist (m_persistedBlockCount
    // onward), directly onto m_currentTapePath — no tmp+rename. Safe to resume from: every
    // earlier append already left the file ending exactly on a block boundary, so the only risk
    // window is THIS call's own write, and a crash during it leaves at worst an incomplete
    // trailing block, never corrupts a previously-flushed one (see loadTape()).
    const uint64_t newBlocks = blocks.size() - m_persistedBlockCount;
    logStep("save: vid=" + vid + " lock acquired after " + std::to_string(lockWaitMs) + " ms, appending "
            + std::to_string(newBlocks) + " new block(s) (persistOnFlush)");

    // One buffer, one write() — see serializeTapeChunk()'s comment for why this beats one
    // write() call per block.
    const std::string chunk = serializeTapeChunk(!m_headerPersisted, k_magic, k_version, blocks, m_persistedBlockCount);

    // Recorded BEFORE writing so a failed/partial append (e.g. ENOSPC partway through a large
    // chunk — ofstream::write() gives no way to know how many bytes of a failed write actually
    // landed) can be rolled back to exactly this size. Without this, a partial append that still
    // reports failure would leave garbage bytes on disk that m_persistedBlockCount does not know
    // about; a RETRY would then append the same blocks again, AFTER that garbage, corrupting the
    // file in a way loadTape()'s truncated-tail tolerance cannot recover from (the garbage is not
    // at the very end, it is sandwiched between valid data and the retried blocks).
    const uint64_t preWriteSize = std::filesystem::file_size(m_currentTapePath);

    std::ofstream f(m_currentTapePath, std::ios::binary | std::ios::app);
    if (!f) {
      throw cta::exception::Exception("VtlDrive::saveTape: cannot open " + m_currentTapePath.string()
                                      + " for appending");
    }
    f.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
    f.flush();
    if (!f) {
      f.close();
      std::error_code ec;
      std::filesystem::resize_file(m_currentTapePath, preWriteSize, ec);  // best effort; see comment above
      throw cta::exception::Exception("VtlDrive::saveTape: I/O error appending to " + m_currentTapePath.string()
                                      + " (disk full? check shared volume space)");
    }
    m_headerPersisted = true;
    m_persistedBlockCount = blocks.size();
  } else {
    // Full rewrite via an atomic tmp+rename: unloadTape() is this cartridge's only save, so
    // there is nothing from an earlier call in this mount to build on incrementally.
    logStep("save: vid=" + vid + " lock acquired after " + std::to_string(lockWaitMs)
            + " ms, writing full tape to disk");
    const auto tmpPath = std::filesystem::path(m_currentTapePath.string() + ".tmp");

    // One buffer, one write() — see serializeTapeChunk()'s comment for why this beats one
    // write() call per block. This does mean holding a second, full copy of the tape's
    // serialized bytes in memory for the duration of the write (on top of m_tape itself) —
    // acceptable at this test's tape sizes; worth revisiting if tapes get much larger.
    const std::string chunk = serializeTapeChunk(/*includeHeader=*/true, k_magic, k_version, blocks, 0);

    {
      std::ofstream f(tmpPath, std::ios::binary | std::ios::trunc);
      if (!f) {
        throw cta::exception::Exception("VtlDrive::saveTape: cannot open " + tmpPath.string() + " for writing");
      }

      f.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));

      f.flush();
      if (!f) {
        std::filesystem::remove(tmpPath);
        throw cta::exception::Exception("VtlDrive::saveTape: I/O error writing " + tmpPath.string()
                                        + " (disk full? check shared volume space)");
      }
    }

    std::filesystem::rename(tmpPath, m_currentTapePath);
    m_headerPersisted = true;
    m_persistedBlockCount = blocks.size();
  }

  const auto saveMs =
    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - saveStart).count();
  logStep("save: vid=" + vid + " written to disk in " + std::to_string(saveMs) + " ms, releasing lock");
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
        // Cut short mid-block: only possible if an earlier persistOnFlush save (see saveTape())
        // was interrupted by a crash/OOM-kill while appending. Every block before this one was
        // already fully written and flushed by an earlier, COMPLETED save call, so it is safe —
        // and correct — to drop just this incomplete tail rather than failing the whole tape.
        logStep("load: vid=" + tapePath.stem().string() + " dropping incomplete trailing block in "
                + tapePath.string() + " (" + std::to_string(blocks.size())
                + " complete block(s) kept) — an earlier save was likely interrupted mid-append");
        break;
      }
    }
    f.read(reinterpret_cast<char*>(&block.remainingSpaceAfter), sizeof(block.remainingSpaceAfter));
    if (!f) {
      // Same reasoning as above: the block's data was complete but its trailer was cut short.
      logStep("load: vid=" + tapePath.stem().string() + " dropping incomplete trailing block in "
              + tapePath.string() + " (" + std::to_string(blocks.size())
              + " complete block(s) kept) — an earlier save was likely interrupted mid-append");
      break;
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
    if (auto persist = (*mt)["persistOnFlush"].value<bool>()) {
      info.persistOnFlush = *persist;
    }
    if (auto rate = (*mt)["transferFailureRate"].value<double>()) {
      info.transferFailureRate = *rate;
    }
    if (auto rate = (*mt)["retrieveFailureRate"].value<double>()) {
      info.retrieveFailureRate = *rate;
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

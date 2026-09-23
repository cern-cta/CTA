/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "Payload.hpp"
#include "RecordedFailure.hpp"
#include "common/exception/Exception.hpp"

#include <memory>
#include <optional>

namespace cta::tape::daemon {

/**
 * Individual memory block with metadata
 */
class MemBlock {
  enum class State { Normal, Failed, Cancelled, VerifyOnly };

  // Keep the state outside optional storage so reset blocks always have an initialized discriminator.
  State m_state = State::Normal;
  std::optional<std::string> m_errorMsg;
  std::optional<RecordedFailure> m_recordedFailure;

public:
  /**
   * Constructor
   * @param id the block ID for its whole life
   * @param capacity the capacity (in byte) of the embed payload
   */
  MemBlock(const uint32_t id, const uint32_t capacity) : m_memoryBlockId(id), m_payload(capacity) { reset(); }

  /**
   * Get the error message from the context,
   * Throw an exception if there is no context
   * @return
   */
  std::string errorMsg() const {
    if (m_errorMsg) {
      return *m_errorMsg;
    }

    throw cta::exception::Exception("Error Context is not set ="
                                    " no error message to give");
  }

  /**
   * Return true if the block has been marked as failed
   * @return
   */
  bool isFailed() const { return m_state == State::Failed; }

  /**
   * Return true if the block has been marked as canceled
   * @return
   */
  bool isCanceled() const { return m_state == State::Cancelled; }

  /**
   * Return true if the block has been marked as verify only
   * @return
   */
  bool isVerifyOnly() const { return m_state == State::VerifyOnly; }

  // A failed block carries the source diagnostic through the reader/writer handoff.
  std::optional<RecordedFailure> recordedFailure() const { return m_recordedFailure; }

  /**
   * Mark this block as failed and clear the file and tape block indices.
   */
  void markAsFailed(const std::string& msg, RecordedFailure failure) {
    m_recordedFailure = failure;
    m_errorMsg = msg;
    m_state = State::Failed;
    m_fileBlock.reset();
    m_tapeFileBlock.reset();
  }

  /**
   * Mark the block as canceled: this indicates the writer thread that
   * the read was skipped due to previous, unrelated errors, and that this
   * file will not be processed at all (and hence should not be reported about).
   * This is mainly used for the tape read case, when positioning is confused
   * (when positioning by fSeq, there's nothing we can do).
   */
  void markAsCancelled() {
    m_recordedFailure.reset();
    m_errorMsg = "";
    m_state = State::Cancelled;
    m_fileBlock.reset();
    m_tapeFileBlock.reset();
  }

  /**
   * Mark the block as verify only: no disk file will be written but the
   * file should otherwise be processed normally
   */
  void markAsVerifyOnly() {
    m_recordedFailure.reset();
    m_errorMsg = "";
    m_state = State::VerifyOnly;
  }

  /**
   * Clear transfer metadata, payload, alteration state and recorded failure.
   */
  void reset() noexcept {
    m_fileid.reset();
    m_fileBlock.reset();
    m_fSeq.reset();
    m_tapeFileBlock.reset();
    m_payload.reset();

    m_state = State::Normal;
    m_errorMsg.reset();
    m_recordedFailure.reset();
  }

  /** Unique memory block id */
  const uint32_t m_memoryBlockId;

  /** handle to the raw data to be migrated/recalled */
  Payload m_payload;

  /** NsFileId file concerned */
  std::optional<uint64_t> m_fileid;

  /** number of the memory-chunk of the current file we are manipulating */
  std::optional<uint64_t> m_fileBlock;

  /** order of file on the tape */
  std::optional<uint64_t> m_fSeq;

  /** Sequence number of the first tape block file in this memory block */
  std::optional<size_t> m_tapeFileBlock;

  /** Size of the tape blocks, allowing sanity checks on the disk write side in recalls */
  std::optional<size_t> m_tapeBlockSize;
};

}  // namespace cta::tape::daemon

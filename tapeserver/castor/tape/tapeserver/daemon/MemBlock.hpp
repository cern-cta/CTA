/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

#include "common/exception/Exception.hpp"
#include "castor/tape/tapeserver/daemon/Payload.hpp"

namespace castor::tape::tapeserver::daemon {

/**
 * Individual memory block with metadata
 */
class MemBlock {

  struct AlterationContext {

    /** Flag indicating to the receiver that the file read failed */
    bool m_failed;

    /** Flag indicating that the transfer was cancelled, usually due to a
     previous failure. */
    bool m_cancelled;

    /** Flag indicating that the transfer is verify only, no disk file
     should be written. */
    bool m_verifyOnly;

    /**
     * in case of error, the error message
     */
    std::string m_errorMsg;

    static AlterationContext Failed(const std::string& msg) {
      return {true, false, false, msg};
    }

    static AlterationContext Cancelled() {
      return {false, true, false, ""};
    }

    static AlterationContext VerifyOnly() {
      return {false, false, true, ""};
    }

  private:
    AlterationContext(const bool failed, const bool cancelled, const bool verifyOnly, const std::string &msg):
                      m_failed(failed), m_cancelled(cancelled), m_verifyOnly(verifyOnly), m_errorMsg(msg) {};
  };

  std::optional<AlterationContext> m_context;
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
    if (m_context) {
      return m_context->m_errorMsg;
    }

    throw cta::exception::Exception("Error Context is not set ="
            " no error message to give");
  }

  /**
   * Return true if the block has been marked as failed
   * @return
   */
  bool isFailed() const {
    return m_context.has_value() && m_context->m_failed;
  }

  /**
   * Return true if the block has been marked as canceled
   * @return
   */
  bool isCanceled() const {
    return m_context.has_value() && m_context->m_cancelled;
  }

  /**
   * Return true if the block has been marked as verify only
   * @return
   */
  bool isVerifyOnly() const {
    return m_context.has_value() && m_context->m_verifyOnly;
  }

  /**
   * Mark this block as failed ie
   * m_failed is true, m_fileBlock and m_tapeFileBlock are set at -1
   * Other members do not change
   */
  void markAsFailed(const std::string& msg){
    m_context = AlterationContext::Failed(msg);
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
  void markAsCancelled(){
    m_context = AlterationContext::Cancelled();
    m_fileBlock.reset();
    m_tapeFileBlock.reset();
  }
  /**
   * Mark the block as verify only: no disk file will be written but the
   * file should otherwise be processed normally
   */
  void markAsVerifyOnly(){
    m_context = AlterationContext::VerifyOnly();
  }
  /**
   * Reset all the members.
   * Numerical ones are set at -1.and m_failed to false.
   */
  void reset() noexcept {
    m_fileid.reset();
    m_fileBlock.reset();
    m_fSeq.reset();
    m_tapeFileBlock.reset();
    m_payload.reset();

    //delete the previous m_context (if allocated)
    //and set the new one to nullptr
    m_context.reset();
  }
  /** Unique memory block id */
  const uint32_t m_memoryBlockId;

  /** handle to the raw data to be migrated/recalled */
  Payload m_payload;

  /** CASTOR NsFileId file concerned */
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

} // namespace castor::tape::tapeserver::daemon

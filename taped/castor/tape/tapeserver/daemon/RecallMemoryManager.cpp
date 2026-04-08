/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "castor/tape/tapeserver/daemon/RecallMemoryManager.hpp"

#include "castor/tape/tapeserver/daemon/MemBlock.hpp"
#include "common/telemetry/metrics/instruments/TapedInstruments.hpp"

namespace castor::tape::tapeserver::daemon {

//------------------------------------------------------------------------------
// Callbacks for observing metrics
//------------------------------------------------------------------------------

static void ObserveRecallMemoryUsage(opentelemetry::metrics::ObserverResult observer_result, void* state) noexcept {
  // Recover the object pointer
  const auto memoryManager = static_cast<RecallMemoryManager*>(state);
  if (!memoryManager) {
    return;
  }

  if (std::holds_alternative<std::shared_ptr<opentelemetry::metrics::ObserverResultT<int64_t>>>(observer_result)) {
    auto typed_observer = std::get<std::shared_ptr<opentelemetry::metrics::ObserverResultT<int64_t>>>(observer_result);
    typed_observer->Observe(memoryManager->getTotalMemoryUsed());
  }
}

static void ObserveRecallMemoryLimit(opentelemetry::metrics::ObserverResult observer_result, void* state) noexcept {
  // Recover the object pointer
  const auto memoryManager = static_cast<RecallMemoryManager*>(state);
  if (!memoryManager) {
    return;
  }

  if (std::holds_alternative<std::shared_ptr<opentelemetry::metrics::ObserverResultT<int64_t>>>(observer_result)) {
    auto typed_observer = std::get<std::shared_ptr<opentelemetry::metrics::ObserverResultT<int64_t>>>(observer_result);
    typed_observer->Observe(memoryManager->getTotalMemoryAllocated());
  }
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RecallMemoryManager::RecallMemoryManager(size_t numberOfBlocks, size_t blockSize, cta::log::LogContext& lc)
    : m_blockCapacity(blockSize),
      m_lc(lc) {
  for (size_t i = 0; i < numberOfBlocks; i++) {
    m_freeBlocks.push(new MemBlock(i, m_blockCapacity));
    m_totalNumberOfBlocks++;
    m_totalMemoryAllocated += m_blockCapacity;
  }
  cta::log::ScopedParamContainer params(m_lc);
  params.add("blockCount", m_totalNumberOfBlocks)
    .add("blockSize", m_blockCapacity)
    .add("totalSize", m_totalMemoryAllocated);
  m_lc.log(cta::log::INFO, "RecallMemoryManager: all blocks have been created");
  cta::telemetry::metrics::ctaTapedBufferUsage->AddCallback(ObserveRecallMemoryUsage, this);
  cta::telemetry::metrics::ctaTapedBufferLimit->AddCallback(ObserveRecallMemoryLimit, this);
}

//------------------------------------------------------------------------------
// RecallMemoryManager::~RecallMemoryManager
//------------------------------------------------------------------------------
RecallMemoryManager::~RecallMemoryManager() {
  // Make sure the thread is finished: this should be done by the caller,
  // who should have called waitThreads.
  // castor::server::Thread::wait();
  // we expect to be called after all users are finished. Just "free"
  // the memory blocks we still have.

  cta::threading::BlockingQueue<MemBlock*>::valueRemainingPair ret;
  do {
    ret = m_freeBlocks.popGetSize();
    delete ret.value;
  } while (ret.remaining > 0);

  m_lc.log(cta::log::INFO, "RecallMemoryManager destruction : all memory blocks have been deleted");
  cta::telemetry::metrics::ctaTapedBufferUsage->RemoveCallback(ObserveRecallMemoryUsage, this);
  cta::telemetry::metrics::ctaTapedBufferLimit->RemoveCallback(ObserveRecallMemoryLimit, this);
}

//------------------------------------------------------------------------------
// RecallMemoryManager::areBlocksAllBack
//------------------------------------------------------------------------------
bool RecallMemoryManager::areBlocksAllBack() const noexcept {
  return m_totalNumberOfBlocks == m_freeBlocks.size();
}

//------------------------------------------------------------------------------
// RecallMemoryManager::getFreeBlock
//------------------------------------------------------------------------------
MemBlock* RecallMemoryManager::getFreeBlock() {
  MemBlock* ret = m_freeBlocks.pop();
  // When delivering a fresh block to the user, it should be empty.
  if (ret->m_payload.size()) {
    m_freeBlocks.push(ret);
    throw cta::exception::Exception("Internal error: RecallMemoryManager::getFreeBlock "
                                    "popped a non-empty memory block");
  }
  return ret;
}

//------------------------------------------------------------------------------
// RecallMemoryManager::releaseBlock
//------------------------------------------------------------------------------
void RecallMemoryManager::releaseBlock(MemBlock* mb) {
  mb->reset();
  m_freeBlocks.push(mb);
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::getTotalMemoryAllocated
//------------------------------------------------------------------------------
size_t RecallMemoryManager::getTotalMemoryAllocated() const {
  return m_totalMemoryAllocated;
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::getTotalMemoryUsed
//------------------------------------------------------------------------------
size_t RecallMemoryManager::getTotalMemoryUsed() const {
  return m_totalMemoryAllocated - (m_freeBlocks.size() * m_blockCapacity);
}

}  // namespace castor::tape::tapeserver::daemon

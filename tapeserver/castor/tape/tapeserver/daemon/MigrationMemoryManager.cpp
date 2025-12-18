/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "castor/tape/tapeserver/daemon/MigrationMemoryManager.hpp"

#include "castor/tape/tapeserver/daemon/DataPipeline.hpp"
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"
#include "common/telemetry/metrics/instruments/TapedInstruments.hpp"

namespace castor::tape::tapeserver::daemon {

//------------------------------------------------------------------------------
// Callbacks for observing metrics
//------------------------------------------------------------------------------

static void ObserveMigrationMemoryUsage(opentelemetry::metrics::ObserverResult observer_result, void* state) noexcept {
  // Recover the object pointer
  const auto memoryManager = static_cast<MigrationMemoryManager*>(state);
  if (!memoryManager) {
    return;
  }

  if (std::holds_alternative<std::shared_ptr<opentelemetry::metrics::ObserverResultT<int64_t>>>(observer_result)) {
    auto typed_observer = std::get<std::shared_ptr<opentelemetry::metrics::ObserverResultT<int64_t>>>(observer_result);
    typed_observer->Observe(memoryManager->getTotalMemoryUsed());
  }
}

static void ObserveMigrationMemoryLimit(opentelemetry::metrics::ObserverResult observer_result, void* state) noexcept {
  // Recover the object pointer
  const auto memoryManager = static_cast<MigrationMemoryManager*>(state);
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
MigrationMemoryManager::MigrationMemoryManager(const uint32_t numberOfBlocks,
                                               const uint32_t blockSize,
                                               const cta::log::LogContext& lc)
    : m_blockCapacity(blockSize),
      m_lc(lc) {
  for (uint32_t i = 0; i < numberOfBlocks; i++) {
    m_freeBlocks.push(new MemBlock(i, m_blockCapacity));
    m_totalNumberOfBlocks++;
    m_totalMemoryAllocated += m_blockCapacity;
  }
  m_lc.log(cta::log::INFO, "MigrationMemoryManager: all blocks have been created");
  cta::telemetry::metrics::ctaTapedBufferUsage->AddCallback(ObserveMigrationMemoryUsage, this);
  cta::telemetry::metrics::ctaTapedBufferLimit->AddCallback(ObserveMigrationMemoryLimit, this);
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::~MigrationMemoryManager
//------------------------------------------------------------------------------
MigrationMemoryManager::~MigrationMemoryManager() noexcept {
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

  m_lc.log(cta::log::INFO, "MigrationMemoryManager destruction : all memory blocks have been deleted");
  cta::telemetry::metrics::ctaTapedBufferUsage->RemoveCallback(ObserveMigrationMemoryUsage, this);
  cta::telemetry::metrics::ctaTapedBufferLimit->RemoveCallback(ObserveMigrationMemoryLimit, this);
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::startThreads
//------------------------------------------------------------------------------
void MigrationMemoryManager::startThreads() {
  cta::threading::Thread::start();
  m_lc.log(cta::log::INFO, "MigrationMemoryManager starting thread");
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::waitThreads
//------------------------------------------------------------------------------
void MigrationMemoryManager::waitThreads() {
  cta::threading::Thread::wait();
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::addClient
//------------------------------------------------------------------------------
void MigrationMemoryManager::addClient(DataPipeline* c) {
  m_clientQueue.push(c);
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::areBlocksAllBack
//------------------------------------------------------------------------------
bool MigrationMemoryManager::areBlocksAllBack() const noexcept {
  return m_totalNumberOfBlocks == m_freeBlocks.size();
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::blockCapacity
//------------------------------------------------------------------------------
size_t MigrationMemoryManager::blockCapacity() const {
  return m_blockCapacity;
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::finish
//------------------------------------------------------------------------------
void MigrationMemoryManager::finish() {
  addClient(nullptr);
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::releaseBlock
//------------------------------------------------------------------------------
void MigrationMemoryManager::releaseBlock(MemBlock* mb) {
  mb->reset();
  m_freeBlocks.push(mb);
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::getTotalMemoryAllocated
//------------------------------------------------------------------------------
size_t MigrationMemoryManager::getTotalMemoryAllocated() const {
  return m_totalMemoryAllocated;
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::getTotalMemoryUsed
//------------------------------------------------------------------------------
size_t MigrationMemoryManager::getTotalMemoryUsed() const {
  return m_totalMemoryAllocated - (m_freeBlocks.size() * m_blockCapacity);
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::run
//------------------------------------------------------------------------------
void MigrationMemoryManager::run() {
  while (true) {
    DataPipeline* c = m_clientQueue.pop();
    // If the c is a nullptr pointer, that means end of clients
    if (!c) {
      return;
    }
    // Spin on the the client. We rely on the fact that he will want
    // at least one block (which is the case currently)
    while (c->provideBlock(m_freeBlocks.pop())) {
      // Nothing to do; just wait to provide the next block
    }
  }
}

}  // namespace castor::tape::tapeserver::daemon

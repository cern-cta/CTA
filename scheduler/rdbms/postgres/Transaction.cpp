/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/postgres/Transaction.hpp"

#include <random>

namespace cta::schedulerdb {

Transaction::Transaction(cta::rdbms::ConnPool& connPool, log::LogContext& logContext)
    : m_begin(false),
      m_conn(std::make_unique<cta::rdbms::Conn>(connPool.getConn())),
      m_lc(logContext) {
  startWithRetry(connPool);
}

Transaction::Transaction(Transaction&& other) noexcept
    : m_begin(other.m_begin),
      m_conn(std::move(other.m_conn)),
      m_lc(other.m_lc) {
  other.m_begin = false;
}

Transaction::~Transaction() {
  if (m_begin && m_conn) {
    try {
      m_conn->rollback();
      m_conn->reset();
    } catch (const cta::exception::Exception& e) {
      log::ScopedParamContainer(m_lc).add("exceptionMessage", e.getMessageValue());
      m_lc.log(cta::log::ERR, "In Transaction::~Transaction(): Failed to rollback.");
    }
  }
}

void Transaction::lockGlobal() {
  // a global access lock to the whole database
  // for anyone trying to acquire the same lock
  try {
    std::string sql = "SELECT PG_ADVISORY_XACT_LOCK(:LOCK_ID::bigint)";
    auto stmt = m_conn->createStmt(sql);
    stmt.bindUint64(":LOCK_ID", 0);
    m_conn->setDbQuerySummary("global DB lock");
    stmt.executeQuery();
  } catch (const cta::exception::Exception& e) {
    log::ScopedParamContainer errorParams(m_lc);
    errorParams.add("exceptionMessage", e.getMessageValue());
    m_lc.log(cta::log::ERR, "In Transaction::lockGlobal(): Failed to take a global advisory lock.");
  }
}

void Transaction::takeNamedLock(std::string_view tapePoolString) {
  try {
    std::hash<std::string_view> lock_id_hasher;
    std::size_t lock_id = lock_id_hasher(tapePoolString);
    // Convert to 64-bit integer
    auto hash64 = static_cast<uint64_t>(lock_id);
    auto hash32 = static_cast<uint32_t>(hash64 ^ (hash64 >> 32));

    std::string sql = "SELECT PG_ADVISORY_XACT_LOCK(:HASH32::bigint)";
    auto stmt = m_conn->createStmt(sql);
    m_conn->setDbQuerySummary("advisory DB lock");
    stmt.bindUint64(":HASH32", hash32);
    stmt.executeQuery();
  } catch (const cta::exception::Exception& e) {
    log::ScopedParamContainer errorParams(m_lc);
    errorParams.add("exceptionMessage", e.getMessageValue());
    errorParams.add("pgLockString", tapePoolString);
    m_lc.log(cta::log::ERR, "In Transaction::takeNamedLock(): Failed to take an advisory lock.");
  }
}

cta::rdbms::Conn& Transaction::getConn() const {
  return *m_conn;
}

void Transaction::startWithRetry(cta::rdbms::ConnPool& connPool) {
  for (int attempt = 1; attempt <= MAX_TXN_START_RETRIES; ++attempt) {
    try {
      if (nullptr == m_conn) {
        m_conn = std::make_unique<cta::rdbms::Conn>(connPool.getConn());
      }
      m_conn->executeNonQuery("BEGIN");
      m_begin = true;
      return;
    } catch (const cta::exception::Exception& e) {
      log::ScopedParamContainer params(m_lc);
      params.add("attempt", attempt);
      params.add("maxAttempts", MAX_TXN_START_RETRIES);
      params.add("exceptionMessage", e.getMessageValue());

      m_lc.log(cta::log::ERR, "Transaction::startWithRetry(): Failed to start DB transaction, retrying.");

      // Cleanup before retry
      m_conn.reset();
      m_begin = false;

      if (attempt == MAX_TXN_START_RETRIES) {
        throw;
      }

      auto backoff = BASE_BACKOFF * std::pow(2, attempt - 1);
      backoff += std::chrono::milliseconds(rand() % 50);
      std::this_thread::sleep_for(backoff);
    }
  }
}

void Transaction::setRowCountForTelemetry(uint64_t row_count) {
  m_conn->setRowCountForTelemetry(row_count);
}

void Transaction::commit() {
  m_conn->commit();
  m_begin = false;
}

void Transaction::abort() {
  try {
    m_conn->rollback();
    m_begin = false;
  } catch (const cta::exception::Exception& e) {
    log::ScopedParamContainer errorParams(m_lc);
    errorParams.add("exceptionMessage", e.getMessageValue());
    m_lc.log(cta::log::ERR, "Transaction::abort(): Failed to abort rollback.");
  }
}

}  // namespace cta::schedulerdb

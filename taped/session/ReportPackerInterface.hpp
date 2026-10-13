/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "taped/session/TapeSessionTracker.hpp"

#include <memory>

namespace cta::tape::daemon {

/// Shared logging and batching support for archive and retrieval report packers.
class ReportPackerInterface {
protected:
  /// Destroy the shared report-packer support.
  virtual ~ReportPackerInterface() = default;

  /// Copy the logging context and borrow a tracker that outlives the report worker.
  ReportPackerInterface(const cta::log::LogContext& lc, TapeSessionTracker& tracker)
      : m_lc(lc),
        m_tapeSessionTracker(tracker) {}

  /// Log identifiers for each file-report pointer in c, using msg as the log message.
  template<class C>
  void logReport(const C& c, const std::string& msg) {
    using cta::log::LogContext;
    using cta::log::Param;
    for (typename C::const_iterator it = c.begin(); it != c.end(); ++it) {
      cta::log::ScopedParamContainer sp(m_lc);
      sp.add("fileId", (*it)->fileid())
        .add("NSFSEQ", (*it)->fseq())
        .add("NSHOST", (*it)->nshost())
        .add("NSFILETRANSACTIONID", (*it)->fileTransactionId());
      m_lc.log(cta::log::INFO, msg);
    }
  }

  /// Log identifiers and error text for each file-report pointer in c, using msg as the log message.
  template<class C>
  void logReportWithError(const C& c, const std::string& msg) {
    using cta::log::LogContext;
    using cta::log::Param;
    for (typename C::const_iterator it = c.begin(); it != c.end(); ++it) {
      cta::log::ScopedParamContainer sp(m_lc);
      sp.add("fileId", (*it)->fileid())
        .add("NSFSEQ", (*it)->fseq())
        .add("NSHOST", (*it)->nshost())
        .add("NSFILETRANSACTIONID", (*it)->fileTransactionId())
        .add(cta::semconv::log::errorMessage, (*it)->errorMessage());
      m_lc.log(cta::log::INFO, msg);
    }
  }

  /// Private copy keeps worker log parameters separate from the caller's context.
  cta::log::LogContext m_lc;

  /// Select batched or per-file client reporting.
  enum class ReportBatching { ReportInBulk, ReportByFile };
  ReportBatching m_reportBatching = ReportBatching::ReportInBulk;

public:
  /// @brief Select per-file reporting, as required by cta-readtp.
  ///
  /// Call before starting the report worker.
  virtual void disableBulk() { m_reportBatching = ReportBatching::ReportByFile; }

protected:
  /// Borrowed; the session owner keeps the tracker alive until report workers have joined.
  TapeSessionTracker& m_tapeSessionTracker;
};

}  // namespace cta::tape::daemon

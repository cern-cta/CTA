/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "taped/session/TapeSessionTracker.hpp"

#include <memory>

namespace cta::tape::daemon {

namespace detail {
/// Retrieval report-packer tag.
struct Recall {};

/// Archive report-packer tag.
struct Migration {};

/// Select batched or per-file client reporting.
enum ReportBatching { ReportInBulk, ReportByFile };
}  // namespace detail

/// @brief Shared logging and batching support for archive and retrieval report packers.
/// @tparam PlaceHolder Client tag: detail::Recall or detail::Migration.
template<class PlaceHolder>
class ReportPackerInterface {
protected:
  /// Destroy the shared report-packer support.
  virtual ~ReportPackerInterface() = default;

  /// Copy the logging context and borrow a tracker that outlives the report worker.
  ReportPackerInterface(const cta::log::LogContext& lc, TapeSessionTracker& tracker)
      : m_lc(lc),
        m_tapeSessionTracker(tracker) {}

  /// @brief Log identifiers for each file in a report batch.
  /// @param c Container of file-report pointers.
  /// @param msg Message accompanying each file.
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

  /// @brief Log identifiers and error text for each file in a report batch.
  /// @param c Container of file-report pointers.
  /// @param msg Message accompanying each file.
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

  enum detail::ReportBatching m_reportBatching = detail::ReportInBulk;

public:
  /// @brief Select per-file reporting, as required by cta-readtp.
  ///
  /// Call before starting the report worker.
  virtual void disableBulk() { m_reportBatching = detail::ReportByFile; }

protected:
  /// Borrowed; the session owner keeps the tracker alive until report workers have joined.
  TapeSessionTracker& m_tapeSessionTracker;
};

}  // namespace cta::tape::daemon

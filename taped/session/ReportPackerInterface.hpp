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
//nameholder
struct Recall {};

struct Migration {};

// Enum describing the type of client. Some clients need batched reports,
// some prefer reports file by file
enum ReportBatching { ReportInBulk, ReportByFile };
}  // namespace detail

/**
 * Utility class that should be inherited privately/protectedly
 * the type PlaceHolder is either detail::Recall or detail::Migration
 */
template<class PlaceHolder>
class ReportPackerInterface {
protected:
  virtual ~ReportPackerInterface() = default;

  ReportPackerInterface(const cta::log::LogContext& lc, TapeSessionTracker& tracker)
      : m_lc(lc),
        m_tapeSessionTracker(tracker) {}

  /**
   * Log a set of files independently of the success/failure
   * @param c The set of files to log
   * @param msg The message to be append at the end.
   */
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

  /**
   * Log a set of files independently of the success/failure
   * @param c The set of files to log
   * @param msg The message to be append at the end.
   */
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

  /**
   * The  log context, copied due to threads
   */
  cta::log::LogContext m_lc;

  /**
   * Define how we should report to the client (by file/in bulk).
   */
  enum detail::ReportBatching m_reportBatching = detail::ReportInBulk;

public:
  /**
   * Turn off the packing of the reports by the report packer.
   * This is used for recalls driven by read_tp.
   */
  virtual void disableBulk() { m_reportBatching = detail::ReportByFile; }

protected:
  // The session owner keeps the tracker alive until report workers have joined.
  TapeSessionTracker& m_tapeSessionTracker;
};

}  // namespace cta::tape::daemon
